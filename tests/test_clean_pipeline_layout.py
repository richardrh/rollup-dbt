from __future__ import annotations

import ast
from pathlib import Path
import tomllib


REPO_ROOT = Path(__file__).resolve().parents[1]
ROLLUP_ROOT = REPO_ROOT / "src" / "rollup"
MODEL_ROOTS = (
    ROLLUP_ROOT / "staging",
    ROLLUP_ROOT / "intermediate",
    ROLLUP_ROOT / "marts",
)
APPROVED_PRIVATE_MODULES = {"rollup.marts._fanout_helpers", "rollup.writers._sql"}


def _tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"))


def _public_functions(path: Path) -> list[ast.FunctionDef]:
    return [
        node
        for node in _tree(path).body
        if isinstance(node, ast.FunctionDef) and not node.name.startswith("_")
    ]


def _public_model_paths() -> list[Path]:
    return [
        path
        for root in MODEL_ROOTS
        for path in root.glob("*.py")
        if path.name != "__init__.py" and not path.name.startswith("_")
    ]


def test_wheel_package_discovery_includes_rollup_subpackages() -> None:
    pyproject = tomllib.loads(
        (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    )

    assert pyproject["tool"]["setuptools"]["packages"]["find"] == {
        "where": ["src"],
        "include": ["rollup", "rollup.*"],
    }


def test_source_modules_expose_only_load_as_public_operation() -> None:
    offenders: dict[str, list[str]] = {}
    for path in (ROLLUP_ROOT / "sources").glob("*.py"):
        if path.name == "__init__.py":
            continue
        public_defs = [node.name for node in _public_functions(path)]
        if public_defs != ["load"]:
            offenders[path.relative_to(REPO_ROOT).as_posix()] = public_defs

    assert offenders == {}


def test_public_models_validate_transform_and_transform_validates_contracts() -> None:
    offenders: dict[str, list[str]] = {}
    for path in _public_model_paths():
        public_defs = _public_functions(path)
        public_names = sorted(node.name for node in public_defs)
        if public_names != ["transform", "validate"]:
            offenders[path.relative_to(REPO_ROOT).as_posix()] = public_names
            continue
        transform = next(node for node in public_defs if node.name == "transform")
        calls = {
            node.func.id
            for node in ast.walk(transform)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        }
        if not {"validate", "validate_output"}.issubset(calls):
            offenders[path.relative_to(REPO_ROOT).as_posix()] = sorted(calls)

    assert offenders == {}


def test_product_writers_validate_write_and_write_validates() -> None:
    writer_paths = [
        ROLLUP_ROOT / "writers" / name
        for name in (
            "parquet.py",
            "debug.py",
            "fanout_partitions.py",
            "wide_output.py",
            "duckdb_export.py",
        )
    ]
    offenders: dict[str, list[str]] = {}
    for path in writer_paths:
        public_defs = _public_functions(path)
        public_names = sorted(node.name for node in public_defs)
        if public_names != ["validate", "write"]:
            offenders[path.relative_to(REPO_ROOT).as_posix()] = public_names
            continue
        write = next(node for node in public_defs if node.name == "write")
        if not any(
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "validate"
            for node in ast.walk(write)
        ):
            offenders[path.relative_to(REPO_ROOT).as_posix()] = public_names

    assert offenders == {}


def test_marts_are_pure_transformations_without_filesystem_subprocess_or_duckdb_io() -> (
    None
):
    forbidden_names = {"open", "TemporaryDirectory"}
    forbidden_attrs = {"mkdir", "unlink", "write_parquet", "sink_parquet"}
    offenders: list[str] = []
    for path in (ROLLUP_ROOT / "marts").glob("*.py"):
        tree = _tree(path)
        imports_duckdb_or_subprocess = any(
            isinstance(node, ast.Import)
            and any(alias.name in {"duckdb", "subprocess"} for alias in node.names)
            or isinstance(node, ast.ImportFrom)
            and node.module in {"duckdb", "subprocess"}
            for node in ast.walk(tree)
        )
        calls_io = any(
            isinstance(node, ast.Call)
            and (
                isinstance(node.func, ast.Name)
                and node.func.id in forbidden_names
                or isinstance(node.func, ast.Attribute)
                and node.func.attr in forbidden_attrs
            )
            for node in ast.walk(tree)
        )
        if imports_duckdb_or_subprocess or calls_io:
            offenders.append(path.relative_to(REPO_ROOT).as_posix())

    assert offenders == []


def test_model_modules_do_not_import_pipeline_and_pipeline_imports_no_private_model_names() -> (
    None
):
    offenders: list[str] = []
    for path in [*MODEL_ROOTS, ROLLUP_ROOT / "writers"]:
        for module_path in path.glob("*.py"):
            for node in ast.walk(_tree(module_path)):
                if isinstance(node, ast.Import) and any(
                    alias.name == "rollup.pipeline" for alias in node.names
                ):
                    offenders.append(module_path.relative_to(REPO_ROOT).as_posix())
                if (
                    isinstance(node, ast.ImportFrom)
                    and node.module == "rollup.pipeline"
                ):
                    offenders.append(module_path.relative_to(REPO_ROOT).as_posix())

    pipeline = _tree(ROLLUP_ROOT / "pipeline.py")
    model_prefixes = (
        "rollup.sources",
        "rollup.staging",
        "rollup.intermediate",
        "rollup.marts",
    )
    for node in ast.walk(pipeline):
        if (
            isinstance(node, ast.ImportFrom)
            and node.module
            and node.module.startswith(model_prefixes)
        ):
            offenders.extend(
                f"pipeline imports {node.module}.{alias.name}"
                for alias in node.names
                if alias.name.startswith("_")
            )

    assert offenders == []


def test_pipeline_calls_imported_model_transform_methods_without_inline_business_transforms() -> (
    None
):
    tree = _tree(ROLLUP_ROOT / "pipeline.py")
    imported_model_modules = {
        alias.asname or alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        and node.module
        in {"rollup.sources", "rollup.staging", "rollup.intermediate", "rollup.marts"}
        for alias in node.names
    }
    transform_modules = {
        node.func.value.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "transform"
        and isinstance(node.func.value, ast.Name)
    }
    inline_polars_ops = {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in {"filter", "with_columns", "join", "select", "group_by"}
    }

    assert imported_model_modules <= transform_modules
    assert inline_polars_ops == set()


def test_writers_do_not_import_marts() -> None:
    offenders = [
        path.relative_to(REPO_ROOT).as_posix()
        for path in (ROLLUP_ROOT / "writers").glob("*.py")
        for node in ast.walk(_tree(path))
        if isinstance(node, ast.ImportFrom)
        and node.module
        and node.module.startswith("rollup.marts")
    ]

    assert offenders == []


def test_public_modules_do_not_import_private_names_except_approved_shared_modules() -> (
    None
):
    offenders: list[str] = []
    for path in ROLLUP_ROOT.rglob("*.py"):
        if path.name == "__init__.py":
            continue
        for node in ast.walk(_tree(path)):
            if not isinstance(node, ast.ImportFrom) or not node.module:
                continue
            if (
                not node.module.startswith("rollup.")
                or node.module in APPROVED_PRIVATE_MODULES
            ):
                continue
            private_names = [
                alias.name for alias in node.names if alias.name.startswith("_")
            ]
            if private_names:
                offenders.append(
                    f"{path.relative_to(REPO_ROOT).as_posix()} imports {private_names} from {node.module}"
                )

    assert offenders == []


def test_no_private_intermediate_helper_modules() -> None:
    helper_paths = [
        path.relative_to(REPO_ROOT).as_posix()
        for path in (ROLLUP_ROOT / "intermediate").glob("_*.py")
        if path.name != "__init__.py"
    ]

    assert helper_paths == []


def test_model_modules_are_not_import_only_and_public_functions_are_not_forwarders() -> (
    None
):
    offenders: list[str] = []
    for path in _public_model_paths():
        tree = _tree(path)
        meaningful = [
            node
            for node in tree.body
            if not (
                isinstance(node, ast.Expr)
                and isinstance(node.value, ast.Constant)
                and isinstance(node.value.value, str)
            )
        ]
        if meaningful and all(
            isinstance(node, ast.Import | ast.ImportFrom | ast.Assign | ast.AnnAssign)
            for node in meaningful
        ):
            offenders.append(f"{path.relative_to(REPO_ROOT).as_posix()} is import-only")

    for path in ROLLUP_ROOT.rglob("*.py"):
        if path.name == "__init__.py":
            continue
        for function in _public_functions(path):
            body = [
                node
                for node in function.body
                if not (
                    isinstance(node, ast.Expr)
                    and isinstance(node.value, ast.Constant)
                    and isinstance(node.value.value, str)
                )
            ]
            if len(body) != 1 or not isinstance(body[0], ast.Return):
                continue
            returned = body[0].value
            if not isinstance(returned, ast.Call):
                continue
            direct_delegate = isinstance(returned.func, ast.Name | ast.Attribute)
            validates_or_raises = any(
                isinstance(node, ast.Raise)
                or isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id in {"validate", "validate_output"}
                for node in ast.walk(function)
            )
            if direct_delegate and not validates_or_raises:
                offenders.append(
                    f"{path.relative_to(REPO_ROOT).as_posix()}::{function.name}"
                )

    assert offenders == []


def test_production_has_no_dynamic_dispatch_or_file_wide_mypy_ignores() -> None:
    forbidden_calls = {"getattr", "hasattr", "set_defaults"}
    offenders: list[str] = []
    for path in ROLLUP_ROOT.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        tree = _tree(path)
        if "# mypy: ignore-errors" in text:
            offenders.append(
                f"{path.relative_to(REPO_ROOT).as_posix()} file-wide mypy ignore"
            )
        if "args.func" in text:
            offenders.append(f"{path.relative_to(REPO_ROOT).as_posix()} args.func")
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id in forbidden_calls
            ):
                offenders.append(
                    f"{path.relative_to(REPO_ROOT).as_posix()} calls {node.func.id}"
                )

    assert offenders == []


def test_duckdb_public_validate_has_logic_and_write_calls_it_once() -> None:
    functions = {
        node.name: node
        for node in _tree(ROLLUP_ROOT / "writers" / "duckdb_export.py").body
        if isinstance(node, ast.FunctionDef)
    }
    validate = functions["validate"]
    write = functions["write"]

    assert any(isinstance(node, ast.Raise) for node in ast.walk(validate))
    assert (
        sum(
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "validate"
            for node in ast.walk(write)
        )
        == 1
    )
