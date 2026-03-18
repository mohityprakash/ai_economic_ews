from __future__ import annotations


class ValidationError(ValueError):
    pass


def require_keys(rows: list[dict[str, object]], keys: list[str], dataset_name: str) -> None:
    if not rows:
        raise ValidationError(f"{dataset_name} is empty")
    missing = [key for key in keys if key not in rows[0]]
    if missing:
        raise ValidationError(f"{dataset_name} is missing required keys: {missing}")
