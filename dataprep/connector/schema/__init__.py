"""
Module contains the loaded config schema.
"""
from json import load as jload
from pathlib import Path
from typing import Dict, Any, TypeVar

import jsonschema
from stringcase import snakecase

from .defs import ConfigDef

with open(f"{Path(__file__).parent}/schema.json", "r") as f:
    CONFIG_SCHEMA = jload(f)


def parse_config(config: Dict[str, Any]) -> ConfigDef:
    jsonschema.validate(
        config, CONFIG_SCHEMA
    )  # This will throw errors if validate failed

    return ConfigDef.from_value(config)
