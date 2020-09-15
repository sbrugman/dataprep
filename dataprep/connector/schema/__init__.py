"""Module contains the loaded config schema."""

from json import load as jload
from pathlib import Path
from typing import Dict, Any

import jsonschema

from .defs import *

with open(f"{Path(__file__).parent}/schema.json", "r") as f:
    CONFIG_SCHEMA = jload(f)


def parse_config(config: Dict[str, Any]) -> ConfigDef:
    """Parse and validate the config dict."""
    jsonschema.validate(
        config, CONFIG_SCHEMA
    )  # This will throw errors if validate failed

    return ConfigDef(val=config)
