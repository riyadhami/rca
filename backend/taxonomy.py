import json
import os

_JSON_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "final_verified_taxonomy.json")
with open(_JSON_PATH) as _f:
    taxonomy_mapping: dict[str, dict[str, list[str]]] = json.load(_f)
