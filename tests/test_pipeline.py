from __future__ import annotations

from pathlib import Path

from ews.process.pipeline import run_pipeline


def test_pipeline_builds_required_marts() -> None:
    artifacts = run_pipeline()
    expected = {
        "overview_indices",
        "affordability_timeseries",
        "occupation_panel",
        "labor_index",
        "concentration_index",
        "macro_fragility",
        "data_freshness",
    }
    assert expected.issubset(set(artifacts.mart_paths))
    assert Path("app/dashboard_data.json").exists()
