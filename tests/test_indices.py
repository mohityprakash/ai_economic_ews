from __future__ import annotations

from ews.indices.calculations import compute_hhi, min_max_normalize


def test_min_max_normalize_bounds() -> None:
    normalized = min_max_normalize([1, 2, 3])
    assert normalized[0] == 0
    assert normalized[-1] == 100


def test_compute_hhi() -> None:
    assert round(compute_hhi([0.5, 0.3, 0.2]), 2) == 0.38
