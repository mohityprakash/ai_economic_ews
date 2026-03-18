from __future__ import annotations

from math import isclose


def min_max_normalize(values: list[float], lower: float = 0.0, upper: float = 100.0) -> list[float]:
    min_value = min(values)
    max_value = max(values)
    if isclose(min_value, max_value):
        return [50.0 for _ in values]
    scale = upper - lower
    return [lower + ((value - min_value) / (max_value - min_value)) * scale for value in values]


def compute_hhi(shares: list[float]) -> float:
    return sum(share * share for share in shares)
