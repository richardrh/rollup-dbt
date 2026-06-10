#!/usr/bin/env python3
"""
Build script.

Usage:
  python scripts/build.py                         # Build everything (wheel + binary)
  python scripts/build.py package --no-version-prompt  # Build Python package (wheel only)
  python scripts/build.py binary                  # Build PyInstaller binary
  python scripts/build.py clean                   # Clean build artifacts

Built artifacts are placed in ./dist/
"""

from __future__ import annotations

import hashlib
import re
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REQUIRED_SOURCE_FILES = (
    "__init__.py",
    "__main__.py",
    "analysis.py",
    "api.py",
    "cli.py",
    "columns.py",
    "config.py",
    "duckdb_export.py",
    "ep_summary_generator.py",
    "logging.py",
    "pipeline.py",
    "resources.py",
)


def get_version() -> str:
    """Read version from pyproject.toml."""
    pyproject_path = PROJECT_ROOT / "pyproject.toml"
    with open(pyproject_path, "rb") as f:
        data = tomllib.load(f)
    return data["project"]["version"]


def set_version(new_version: str) -> None:
    """Update version in pyproject.toml."""
    pyproject_path = PROJECT_ROOT / "pyproject.toml"
    lines = pyproject_path.read_text(encoding="utf-8").splitlines(keepends=True)
    updated_lines = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith('version = "') and stripped.endswith('"'):
            indent = line[: len(line) - len(line.lstrip())]
            updated_lines.append(f'{indent}version = "{new_version}"\n')
        else:
            updated_lines.append(line)
    pyproject_path.write_text("".join(updated_lines), encoding="utf-8")
    print(f"✅ Updated version to {new_version}")


def prompt_version(current_version: str) -> str:
    """Prompt user for version, return version string."""
    print(f"\n📦 Current version: {current_version}")
    while True:
        new_version = input(
            "Enter new version (or press Enter to keep current): "
        ).strip()
        if not new_version:
            return current_version
        # Basic semantic version validation: digits and dots, optional pre-release
        if re.match(r"^\d+(\.\d+)*([a-zA-Z0-9+-]*)?$", new_version):
            return new_version
        print("Invalid version format. Use semantic version like 1.2.3 or 1.2.3-alpha")


def generate_checksums(dist_dir: Path) -> None:
    """Generate SHA256 checksums for all built artifacts."""
    checksums = {}
    for artifact in sorted(dist_dir.glob("*")):
        if artifact.is_file() and artifact.name != "SHA256SUMS":
            sha256 = hashlib.sha256()
            with open(artifact, "rb") as f:
                while True:
                    chunk = f.read(4096)
                    if not chunk:
                        break
                    sha256.update(chunk)
            checksums[artifact.name] = sha256.hexdigest()

    # Write checksums file
    checksums_file = dist_dir / "SHA256SUMS"
    with open(checksums_file, "w", encoding="utf-8") as f:
        for filename, digest in checksums.items():
            f.write(f"{digest}  {filename}\n")
    print(f"✅ Checksums generated: {checksums_file}")


def check_dependencies() -> int:
    """Check that required build tools are installed."""
    required = {
        "build": "Build backend",
    }
    missing = []

    for tool, description in required.items():
        result = subprocess.run(
            [sys.executable, "-m", tool, "--version"],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            missing.append(f"  {tool}: {description}")

    if missing:
        print("❌ Missing build dependencies:")
        for item in missing:
            print(item)
        print("\nInstall with: pip install build")
        return 1
    return 0


def check_pyproject() -> int:
    """Validate pyproject.toml exists and has required fields."""
    pyproject_path = PROJECT_ROOT / "pyproject.toml"
    if not pyproject_path.exists():
        print(f"❌ pyproject.toml not found at {pyproject_path}")
        return 1

    try:
        with open(pyproject_path, "rb") as f:
            data = tomllib.load(f)
    except Exception as e:
        print(f"❌ Failed to parse pyproject.toml: {e}")
        return 1

    # Check required fields
    project = data.get("project", {})
    required_fields = ["name", "version", "description"]
    missing = [f for f in required_fields if f not in project]

    if missing:
        print(f"❌ Missing required fields in pyproject.toml: {missing}")
        return 1

    print(
        f"✅ pyproject.toml valid (name: {project['name']}, version: {project['version']})"
    )
    return 0


def check_source_structure() -> int:
    """Check that source directories and key files exist."""
    package_dir = PROJECT_ROOT / "src" / "rollup"
    if not package_dir.exists():
        print(f"❌ package directory not found at {package_dir}")
        return 1

    missing = []
    for file in REQUIRED_SOURCE_FILES:
        path = package_dir / file
        if not path.exists():
            missing.append(str(path.relative_to(PROJECT_ROOT)))

    if missing:
        print("❌ Missing required files/directories:")
        for m in missing:
            print(f"  - {m}")
        return 1

    print("✅ Source structure valid")
    return 0


def check_readiness() -> int:
    """Run all pre-build checks without actually building."""
    print("🔍 Checking build readiness...\n")

    checks = [
        ("Dependencies", check_dependencies),
        ("pyproject.toml", check_pyproject),
        ("Source structure", check_source_structure),
    ]

    failed = 0
    for name, check_func in checks:
        print(f"Checking {name}...")
        if check_func() != 0:
            failed += 1
        print()  # blank line

    if failed > 0:
        print(f"❌ {failed} check(s) failed. Fix issues before building.")
        return 1

    print("✅ All checks passed! Ready to build.")
    print("   Run: python scripts/build.py package --no-version-prompt   (for Python package)")
    print("   Run: python scripts/build.py binary                        (for standalone binary)")
    print("   Run: python scripts/build.py all --no-version-prompt       (for everything)")
    return 0


def build_package() -> int:
    """Build Python package (wheel only)."""
    print("📦 Building Python package (wheel only)...")
    version = get_version()
    print(f"   Version: {version}")

    result = subprocess.run(
        [sys.executable, "-m", "build", "--wheel"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"❌ Package build failed:\n{result.stderr}")
        return 1
    print("✅ Wheel built successfully")

    # Generate checksums
    dist_dir = PROJECT_ROOT / "dist"
    generate_checksums(dist_dir)

    return 0


def build_binary() -> int:
    """Build standalone PyInstaller binary using rollup.spec."""
    spec_path = PROJECT_ROOT / "rollup.spec"
    if not spec_path.exists():
        print(f"❌ PyInstaller spec not found at {spec_path}")
        return 1

    print("🧊 Building PyInstaller binary...")
    result = subprocess.run(
        [sys.executable, "-m", "PyInstaller", "-y", "rollup.spec"],
        cwd=PROJECT_ROOT,
    )
    if result.returncode != 0:
        print("❌ Binary build failed")
        return result.returncode

    print("✅ Binary built successfully")
    return 0


def maybe_prompt_for_version(no_version_prompt: bool) -> int:
    """Prompt for a package version unless explicitly disabled."""
    current_version = get_version()
    if no_version_prompt:
        print(f"📦 Keeping current version: {current_version}")
        return 0

    new_version = prompt_version(current_version)
    if new_version != current_version:
        set_version(new_version)
    return 0


def clean_build() -> int:
    """Clean build artifacts."""
    print("🧹 Cleaning build artifacts...")
    dist_dir = PROJECT_ROOT / "dist"
    build_dir = PROJECT_ROOT / "build"
    root_egg_dir = PROJECT_ROOT / "rollup.egg-info"
    src_egg_dir = PROJECT_ROOT / "src" / "rollup.egg-info"

    for directory in [dist_dir, build_dir, root_egg_dir, src_egg_dir]:
        if directory.exists():
            shutil.rmtree(directory)
            print(f"   Removed {directory}")

    print("✅ Clean complete!")
    return 0


def main(argv: list[str] | None = None) -> int:
    """Main entry point."""
    args = list(sys.argv[1:] if argv is None else argv)
    no_version_prompt = False
    if "--no-version-prompt" in args:
        args.remove("--no-version-prompt")
        no_version_prompt = True

    target = args[0] if args else "all"
    if len(args) > 1:
        print(f"❌ Unexpected arguments: {' '.join(args[1:])}")
        return 2

    if target == "check":
        return check_readiness()

    if target == "clean":
        return clean_build()

    if target in ("package", "all"):
        if maybe_prompt_for_version(no_version_prompt) != 0:
            return 1
        if check_dependencies() != 0:
            return 1
        if build_package() != 0:
            return 1

    if target in ("all", "binary"):
        if build_binary() != 0:
            return 1

    if target not in ("check", "clean", "package", "binary", "all"):
        print(f"❌ Unknown build target: {target}")
        print("   Expected one of: check, clean, package, binary, all")
        return 2

    version = get_version()
    print(f"\n✅ Build complete! Version: {version}")
    print(f"   Artifacts: {PROJECT_ROOT / 'dist'}/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
