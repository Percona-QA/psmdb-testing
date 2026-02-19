"""
Load platform config from platforms.yaml (with defaults merge).
"""
from __future__ import annotations

import os
from pathlib import Path

import yaml

_CONFIG_DIR = Path(__file__).resolve().parent
PLATFORMS_FILE = _CONFIG_DIR / "platforms.yaml"


def _expand_env(value: str) -> str:
    if isinstance(value, str) and value.startswith("${") and ":-" in value:
        # Simple ${VAR:-default} expansion
        inner = value[2:-1].strip()
        if ":-" in inner:
            var_name, default = inner.split(":-", 1)
            return os.environ.get(var_name.strip(), default.strip())
    return value


def _deep_merge(base: dict, override: dict) -> dict:
    out = dict(base)
    for k, v in override.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def _expand_image(platform: dict, defaults: dict) -> str:
    image = platform.get("image") or defaults.get("image", "")
    return _expand_env(str(image)) if isinstance(image, str) else str(image)


def load_platforms(path: Path | None = None) -> dict:
    """Load platforms.yaml and return {platform_name: config} with defaults merged."""
    path = path or PLATFORMS_FILE
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    defaults = data.get("defaults", {})
    platforms = data.get("platforms", {})
    result = {}
    for name, plat in platforms.items():
        merged = _deep_merge(defaults, plat)
        if "image" in merged:
            merged["image"] = _expand_image(merged, defaults)
        result[name] = merged
    return result


def get_platform(name: str, path: Path | None = None) -> dict | None:
    """Get a single platform config by name."""
    platforms = load_platforms(path)
    return platforms.get(name)
