import json
import os

_JSON_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "final_verified_taxonomy.json")
with open(_JSON_PATH) as _f:
    _data = json.load(_f)

# Support both {"RCA_TAXONOMY": [...]} wrapper and a bare array
_flat_list: list[dict] = (
    _data.get("RCA_TAXONOMY", _data) if isinstance(_data, dict) else _data
)

# Build nested dict: { category: { sub_category: [issue, ...] } }
taxonomy_mapping: dict[str, dict[str, list[str]]] = {}
for _entry in _flat_list:
    _cat = (_entry.get("category") or "").strip()
    _sub = (_entry.get("sub_category") or "").strip()
    _iss = (_entry.get("issue") or "").strip()
    if not _cat:
        continue
    if _cat not in taxonomy_mapping:
        taxonomy_mapping[_cat] = {}
    if _sub not in taxonomy_mapping[_cat]:
        taxonomy_mapping[_cat][_sub] = []
    if _iss and _iss not in taxonomy_mapping[_cat][_sub]:
        taxonomy_mapping[_cat][_sub].append(_iss)
