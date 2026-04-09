import toml
import os
from pathlib import Path

def load_config(path=None):
    if path is None:
        path = Path(__file__).parent / "config.toml"

    config = toml.load(path)

    # env 치환
    if "api" in config and "api_key" in config["api"]:
        val = config["api"]["api_key"]
        if isinstance(val, str) and val.startswith("${") and val.endswith("}"):
            key = val[2:-1]
            config["api"]["api_key"] = os.getenv(key)

    return config