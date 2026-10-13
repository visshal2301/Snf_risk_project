import os
import re
import yaml
from pathlib import Path
from typing import Any, Dict
try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*args, **kwargs):
        pass



def _resolve_env_vars(data: Any) -> Any:
    """Recursively resolve ${VAR_NAME} placeholders in strings."""
    pattern = re.compile(r"\$\{([^}^{]+)\}")
    if isinstance(data, dict):
        return {k: _resolve_env_vars(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [_resolve_env_vars(i) for i in data]
    elif isinstance(data, str):
        matches = pattern.findall(data)
        for match in matches:
            env_val = os.getenv(match, "")
            data = data.replace(f"${{{match}}}", env_val)
        return data
    return data


def load_config(config_path: str) -> Dict[str, Any]:
    """Loads YAML configuration file and resolves environment variables."""
    load_dotenv()
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found at: {config_path}")

    with open(path, "r", encoding="utf-8") as f:
        raw_config = yaml.safe_load(f)

    return _resolve_env_vars(raw_config) or {}

