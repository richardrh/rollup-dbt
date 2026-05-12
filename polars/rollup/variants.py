"""Hisco fanout variant definitions."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from typing import NamedTuple

import polars as pl

from rollup.chain import DIALSUP_COL, main_loss_col
from rollup.config import Flavor, Vendor
from rollup.schemas.columns import RefForecastFactorsCol as FF
from rollup.seeds import Seeds


def forecast_tags(forecast_dates: Sequence[date]) -> list[str]:
    """`[date(2026,1,1), date(2026,7,1)] → ['202601', '202607']`."""
    return sorted(set(d.strftime("%Y%m") for d in forecast_dates))


class VariantSpec(NamedTuple):
    """One Hisco fan-out output, as a typed triple."""
    vendor: Vendor
    forecast_date: date
    flavor: Flavor

    @property
    def forecast_tag(self) -> str:
        return self.forecast_date.strftime("%Y%m")

    @property
    def name(self) -> str:
        match self.flavor:
            case Flavor.MAIN:
                return f"Hisco{self.vendor.hisco_label}_{self.forecast_tag}_{self.flavor.value}"
            case Flavor.DIALSUP:
                return f"Hisco{self.vendor.hisco_label}_{self.flavor.value}"

    @property
    def loss_metric(self) -> str:
        match self.flavor:
            case Flavor.MAIN:
                return main_loss_col(self.forecast_tag)
            case Flavor.DIALSUP:
                return DIALSUP_COL


def build_variants(
    forecast_dates: Sequence[date],
    vendors: Sequence[Vendor],
) -> list[VariantSpec]:
    """Build the set of Hisco fan-out outputs."""
    unique_tags: list[str] = []
    seen_tags: set[str] = set()
    for d in forecast_dates:
        tag = d.strftime("%Y%m")
        if tag not in seen_tags:
            seen_tags.add(tag)
            unique_tags.append(tag)

    variants: list[VariantSpec] = []
    for vendor in vendors:
        for flavor in vendor.flavors:
            if flavor == Flavor.DIALSUP:
                if unique_tags:
                    variants.append(VariantSpec(
                        vendor=vendor,
                        forecast_date=forecast_dates[0],
                        flavor=flavor,
                    ))
                continue
            for tag in unique_tags:
                for d in forecast_dates:
                    if d.strftime("%Y%m") == tag:
                        variants.append(VariantSpec(vendor=vendor, forecast_date=d, flavor=flavor))
                        break
    return variants


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
