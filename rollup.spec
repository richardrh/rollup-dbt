# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path

from PyInstaller.utils.hooks import collect_data_files, collect_submodules


ROOT = Path(SPECPATH)

zensical_datas = collect_data_files("zensical", include_py_files=False)
docs_hiddenimports = [
    *collect_submodules("zensical"),
    *collect_submodules("markdown"),
    *collect_submodules("pymdownx"),
    *collect_submodules("pygments.lexers"),
]
runtime_hiddenimports = [
    "duckdb",
    "pandera",
    "pandera.errors",
    "pandera.polars",
    "polars",
    "pyarrow",
    "pyarrow.parquet",
]

a = Analysis(
    [str(ROOT / "src" / "rollup" / "__main__.py")],
    pathex=[str(ROOT / "src")],
    binaries=[],
    datas=[
        (str(ROOT / "docs"), "docs"),
        (str(ROOT / "zensical.toml"), "."),
        *zensical_datas,
    ],
    hiddenimports=[*docs_hiddenimports, *runtime_hiddenimports],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="rollup",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="rollup",
)
