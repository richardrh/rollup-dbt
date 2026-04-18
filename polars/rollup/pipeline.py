"""Orchestrator. Build a single cached `all_factors` LazyFrame, then fan out
into per-(vendor, forecast_date, flavor) Hisco parquet files in one optimized pass.
"""

from __future__ import annotations

import sys
from collections.abc import Sequence
from datetime import date
from pathlib import Path
from typing import NamedTuple

import polars as pl

from rollup import config
from rollup.config import Flavor, Vendor
from rollup.schemas import frames as F
from rollup.schemas.columns import AllFactorsCol as AF
from rollup.schemas.columns import HiscoFanoutCol as H
from rollup.schemas.columns import MetricCol as M
from rollup.schemas.columns import RefForecastFactorsCol as FF
from rollup.seeds import Seeds
from rollup.validate import validate_schema


# --------------------------------------------------------------------------- #
# Variant spec                                                                #
# --------------------------------------------------------------------------- #

class VariantSpec(NamedTuple):
    """One Hisco fan-out output, as a typed triple.

    The product (vendor × forecast_date × flavor) replaces january's 21
    hand-listed `(RMS/AIR, yyyyMM, flavor-with-fix-suffix)` rows. Forecast
    dates come from the `forecast_factors` seed at pipeline start — they're
    data, not code.
    """
    vendor:        Vendor
    forecast_date: date
    flavor:        Flavor

    @property
    def forecast_tag(self) -> str:
        """`date(2026, 1, 1)` → `"202601"`. Used in the output filename and
        in the `loss_uplifted_..._{year}` metric column names."""
        return self.forecast_date.strftime("%Y%m")

    @property
    def name(self) -> str:
        """Output filename (no extension), e.g. `HiscoAIR_202601_fagross`."""
        return f"Hisco{self.vendor.hisco_label}_{self.forecast_tag}_{self.flavor.value}"

    @property
    def loss_metric(self) -> str:
        """The MetricCol member that feeds `Hisco.ModelGrossLoss`.

        STANDARD  → loss_uplifted_capped_localccy_{year}_euws
        FAGROSS   → loss_uplifted_capped_localccy_{year}_euws_fagross
        DIALSUP   → dialsup_{year}  (computed in stages.dialsup — not a MetricCol)
        """
        y = self.forecast_tag
        match self.flavor:
            case Flavor.STANDARD:
                return f"loss_uplifted_capped_localccy_{y}_euws"
            case Flavor.FAGROSS:
                return f"loss_uplifted_capped_localccy_{y}_euws_fagross"
            case Flavor.DIALSUP:
                return f"dialsup_{y}"


_METRIC_VALUES: frozenset[str] = frozenset(m.value for m in M)


def build_variants(
    forecast_dates: Sequence[date],
    vendors: Sequence[Vendor],
) -> list[VariantSpec]:
    """Cross-product of (vendor × forecast_date × flavor), sorted.

    `forecast_dates` come from the `forecast_factors` seed at runtime;
    `vendors` from `config.resolve().vendors`. Each vendor's own
    `flavors` tuple controls which Hisco outputs it emits.
    """
    return [
        VariantSpec(vendor=v, forecast_date=d, flavor=f)
        for v in vendors
        for d in sorted(forecast_dates)
        for f in v.flavors
    ]


def forecast_dates_from_seed(seeds: Seeds) -> list[date]:
    """Distinct forecast dates carried by the forecast_factors seed."""
    return (
        seeds.forecast_factors
        .select(pl.col(FF.FORECAST_DATE))
        .unique()
        .sort(FF.FORECAST_DATE)
        .collect()
        .to_series()
        .to_list()
    )


# --------------------------------------------------------------------------- #
# Stage placeholders — implement in rollup/stages/*.py                        #
# --------------------------------------------------------------------------- #

def build_all_factors(*, raw_dir: Path, seeds_dir: Path) -> pl.LazyFrame:  # noqa: ARG001
    """Compose staging → blending → forecast → euws → fa_gross."""
    raise NotImplementedError(
        "build_all_factors: wire rollup.stages.{staging,blending,forecast,"
        "euws,fa_gross} once those modules exist."
    )


def fanout_hisco(all_factors: pl.LazyFrame, variant: VariantSpec) -> pl.LazyFrame:
    """Project all_factors → one Hisco variant.

    Filters to this variant's vendor and picks the loss-metric column
    implied by the flavor. The dialsup flavor pulls from a computed
    column (`dialsup_{year}`) that isn't a MetricCol member; all others
    pull from MetricCol.
    """
    if variant.flavor is not Flavor.DIALSUP:
        assert variant.loss_metric in _METRIC_VALUES, (
            f"variant {variant.name}: {variant.loss_metric!r} is not a MetricCol member"
        )

    out = (
        all_factors
        .filter(pl.col(AF.BASE_MODEL) == variant.vendor.name)
        .select(
            pl.col(AF.MODEL_EVENT_ID).alias(H.MODEL_EVENT_ID),
            pl.col(AF.YEAR_ID).alias(H.MODEL_YEAR),
            pl.col(AF.REQUIRED_CURRENCY).alias(H.CURRENCY_CODE),
            pl.lit(0, dtype=pl.Int32).alias(H.MODEL_YOA),
            pl.col(variant.loss_metric).alias(H.MODEL_GROSS_LOSS),
            pl.lit(0, dtype=pl.Int32).alias(H.MODEL_INWARDS_REINSTATEMENT),
            pl.lit(0, dtype=pl.Int64).alias(H.MODEL_EVENT_DAY),
            pl.col(AF.CDS_CAT_CLASS_NAME).alias(H.LOSS_CLASS_NAME),
        )
    )
    validate_schema(out, F.HISCO_FANOUT, name=f"fanout.{variant.name}")
    return out


# --------------------------------------------------------------------------- #
# Entry point                                                                 #
# --------------------------------------------------------------------------- #

def run(cfg: config.Config) -> None:
    """Run the pipeline end-to-end. One parquet per fan-out variant."""
    from rollup.seeds import load_all

    cfg.output_dir.mkdir(parents=True, exist_ok=True)

    seeds     = load_all(cfg.seeds_dir)
    variants  = build_variants(forecast_dates_from_seed(seeds), cfg.vendors)

    # TODO: thread vendor-by-vendor (staging produces both, concat → all_factors)
    all_factors = build_all_factors(
        raw_dir=cfg.vendor("verisk").ylt_dir,
        seeds_dir=cfg.seeds_dir,
    ).cache()

    outputs = [fanout_hisco(all_factors, v) for v in variants]
    collected = pl.collect_all(outputs)
    for df, variant in zip(collected, variants, strict=True):
        df.write_parquet(cfg.output_dir / f"{variant.name}.parquet")


def main(argv: list[str] | None = None) -> int:
    """CLI entry: resolve config, print the plan, prompt, then run."""
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-y", "--yes", action="store_true",
                        help="skip the interactive y/N confirmation")
    parser.add_argument("--dry-run", action="store_true",
                        help="print the plan and exit without running")
    args = parser.parse_args(argv)

    cfg  = config.resolve()
    plan = config.build_plan(cfg)

    if args.dry_run:
        print(config.format_plan(plan))
        return 0

    if not plan.all_seeds_ok:
        print(config.format_plan(plan), file=sys.stderr)
        print("aborting: one or more seeds failed schema validation", file=sys.stderr)
        return 2

    if not config.confirm(plan, assume_yes=args.yes):
        print("aborted by user")
        return 1

    run(cfg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
