import json
import os

_JSON_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "category_subcategory.json")
with open(_JSON_PATH) as _f:
    taxonomy_mapping: dict[str, list[str]] = json.load(_f)
