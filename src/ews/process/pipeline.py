from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean

from ews.config.settings import AppConfig
from ews.indices.calculations import compute_hhi, min_max_normalize
from ews.ingest.adapters import IngestResult, load_ai_pricing, load_capability_benchmarks, load_macro_labor, load_occupation_labor
from ews.utils.io import read_csv, read_json, write_csv, write_json
from ews.utils.logging import get_logger
from ews.utils.validation import require_keys

logger = get_logger(__name__)


@dataclass(slots=True)
class PipelineArtifacts:
    raw_metadata: list[dict[str, object]]
    mart_paths: dict[str, str]
    built_at: str


def _write_raw_snapshot(config: AppConfig, result: IngestResult) -> None:
    snapshot_dir = config.paths.raw / result.dataset_name
    write_csv(snapshot_dir / f"{result.dataset_name}.csv", result.rows)
    write_json(snapshot_dir / f"{result.dataset_name}_metadata.json", result.snapshot_payload)


def _group_by(rows: list[dict[str, object]], key: str) -> dict[object, list[dict[str, object]]]:
    grouped: dict[object, list[dict[str, object]]] = {}
    for row in rows:
        grouped.setdefault(row[key], []).append(row)
    return grouped


def run_ingestion(config: AppConfig) -> list[IngestResult]:
    results = [load_ai_pricing(), load_capability_benchmarks(), load_macro_labor(), load_occupation_labor()]
    for result in results:
        _write_raw_snapshot(config, result)
    return results


def build_processed_tables(config: AppConfig) -> dict[str, list[dict[str, object]]]:
    pricing = read_csv(config.paths.raw / "ai_pricing" / "ai_pricing.csv")
    benchmarks = read_csv(config.paths.raw / "capability_benchmarks" / "capability_benchmarks.csv")
    macro = read_csv(config.paths.raw / "macro_labor" / "macro_labor.csv")
    occupation = read_csv(config.paths.raw / "occupation_labor" / "occupation_labor.csv")
    exposure = read_csv(config.paths.reference / "occupation_exposure.csv")
    concentration = read_csv(config.paths.reference / "concentration_reference.csv")

    for rows, keys, name in [
        (pricing, ["date", "provider", "model", "input_price_usd", "output_price_usd", "capability_proxy"], "ai_pricing"),
        (benchmarks, ["date", "provider", "model", "mmlu", "gpqa", "swe_bench"], "capability_benchmarks"),
        (macro, ["date", "series", "value"], "macro_labor"),
        (occupation, ["date", "occ_code", "postings_index", "employment_index", "annual_wage_usd"], "occupation_labor"),
    ]:
        require_keys(rows, keys, name)

    benchmark_lookup = {(row["date"], row["provider"], row["model"]): row for row in benchmarks}
    cost_capability: list[dict[str, object]] = []
    affordability_by_date: dict[str, list[float]] = {}
    capability_by_date: dict[str, list[float]] = {}
    cost_by_date: dict[str, list[float]] = {}
    for row in pricing:
        match = benchmark_lookup[(row["date"], row["provider"], row["model"])]
        capability_score = mean([float(match["mmlu"]), float(match["gpqa"]), float(match["swe_bench"])])
        weighted_cost = (float(row["input_price_usd"]) + float(row["output_price_usd"])) / 2
        affordability_raw = capability_score / weighted_cost
        record = {
            "date": row["date"],
            "provider": row["provider"],
            "model": row["model"],
            "capability_score": round(capability_score, 4),
            "cost_per_1m_weighted": round(weighted_cost, 4),
            "affordability_raw": round(affordability_raw, 6),
        }
        cost_capability.append(record)
        affordability_by_date.setdefault(row["date"], []).append(affordability_raw)
        capability_by_date.setdefault(row["date"], []).append(capability_score)
        cost_by_date.setdefault(row["date"], []).append(weighted_cost)

    sorted_dates = sorted(affordability_by_date)
    affordability_timeseries = [
        {
            "date": date,
            "capability_score": round(mean(capability_by_date[date]), 4),
            "cost_per_1m_weighted": round(mean(cost_by_date[date]), 4),
            "affordability_raw": round(mean(affordability_by_date[date]), 6),
        }
        for date in sorted_dates
    ]
    affordability_norm = min_max_normalize([float(row["affordability_raw"]) for row in affordability_timeseries])
    for row, normalized in zip(affordability_timeseries, affordability_norm, strict=True):
        row["ai_affordability_index"] = round(normalized, 2)
    affordability_lookup = {row["date"]: float(row["ai_affordability_index"]) for row in affordability_timeseries}

    exposure_lookup = {row["occ_code"]: row for row in exposure}
    all_wages = [float(row["annual_wage_usd"]) for row in occupation]
    avg_wage = mean(all_wages)
    occupation_panel: list[dict[str, object]] = []
    labor_summary_temp: dict[str, dict[str, list[float] | float]] = {}
    for row in occupation:
        exposure_row = exposure_lookup[row["occ_code"]]
        affordability = affordability_lookup[row["date"]]
        wage_indexed = float(row["annual_wage_usd"]) / avg_wage
        substitution_pressure = float(exposure_row["ai_exposure"]) * affordability * wage_indexed
        risk_score = substitution_pressure * (100 / float(row["postings_index"])) * (100 / float(row["employment_index"]))
        enriched = {
            **row,
            "occupation": exposure_row["occupation"],
            "sector": exposure_row["sector"],
            "ai_exposure": float(exposure_row["ai_exposure"]),
            "ai_affordability_index": affordability,
            "wage_indexed": round(wage_indexed, 4),
            "substitution_pressure": round(substitution_pressure, 4),
            "risk_score": round(risk_score, 4),
        }
        occupation_panel.append(enriched)
        date_bucket = labor_summary_temp.setdefault(row["date"], {"weighted_sum": 0.0, "employment_total": 0.0, "high": [], "low": []})
        employment = float(row["employment_index"])
        date_bucket["weighted_sum"] += risk_score * employment
        date_bucket["employment_total"] += employment
        if float(exposure_row["ai_exposure"]) >= 0.65:
            date_bucket["high"].append(float(row["postings_index"]))
        else:
            date_bucket["low"].append(float(row["postings_index"]))

    labor_index = []
    for date in sorted(labor_summary_temp):
        bucket = labor_summary_temp[date]
        labor_index.append({
            "date": date,
            "labor_substitution_pressure_raw": bucket["weighted_sum"] / bucket["employment_total"],
            "high_exposure_postings": round(mean(bucket["high"]), 2),
            "low_exposure_postings": round(mean(bucket["low"]), 2),
        })
    labor_norm = min_max_normalize([float(row["labor_substitution_pressure_raw"]) for row in labor_index])
    for row, normalized in zip(labor_index, labor_norm, strict=True):
        row["labor_substitution_pressure_index"] = round(normalized, 2)

    concentration_grouped = _group_by(concentration, "date")
    concentration_index = []
    for date in sorted(concentration_grouped):
        shares = [float(row["share"]) for row in concentration_grouped[date]]
        concentration_index.append({"date": date, "hhi": round(compute_hhi(shares), 4)})
    concentration_norm = min_max_normalize([float(row["hhi"]) for row in concentration_index])
    for row, normalized in zip(concentration_index, concentration_norm, strict=True):
        row["ai_concentration_index"] = round(normalized, 2)
    concentration_lookup = {row["date"]: float(row["ai_concentration_index"]) for row in concentration_index}

    macro_grouped = _group_by(macro, "date")
    fragility = []
    for date in sorted(macro_grouped):
        row_map = {row["series"]: float(row["value"]) for row in macro_grouped[date]}
        fragility.append({
            "date": date,
            **row_map,
            "wage_productivity_gap": row_map["labor_productivity_index"] - row_map["real_wage_index"],
            "job_openings_stress": 100 - row_map["job_openings_index"],
            "ai_affordability_index": affordability_lookup.get(date, affordability_timeseries[-1]["ai_affordability_index"]),
        })
    wage_gap_norm = min_max_normalize([float(row["wage_productivity_gap"]) for row in fragility])
    unemployment_norm = min_max_normalize([float(row["unemployment_rate"]) for row in fragility])
    job_stress_norm = min_max_normalize([float(row["job_openings_stress"]) for row in fragility])
    sector_norm = min_max_normalize([float(row["sector_stress_index"]) for row in fragility])
    for idx, row in enumerate(fragility):
        raw = 0.30 * wage_gap_norm[idx] + 0.25 * unemployment_norm[idx] + 0.20 * job_stress_norm[idx] + 0.15 * sector_norm[idx] + 0.10 * float(row["ai_affordability_index"])
        row["fragility_raw"] = round(raw, 4)
    fragility_norm = min_max_normalize([float(row["fragility_raw"]) for row in fragility])
    for row, normalized in zip(fragility, fragility_norm, strict=True):
        row["economic_fragility_index"] = round(normalized, 2)

    fragility_lookup = {row["date"]: row for row in fragility}
    labor_lookup = {row["date"]: row for row in labor_index}

    def latest_value_on_or_before(lookup: dict[str, object], target_date: str, value_key: str) -> float:
        eligible_dates = [date for date in lookup if date <= target_date]
        chosen = max(eligible_dates) if eligible_dates else min(lookup)
        return float(lookup[chosen][value_key])

    overview_indices = []
    for date in sorted_dates:
        overview_indices.append({
            "date": date,
            "ai_affordability_index": affordability_lookup[date],
            "labor_substitution_pressure_index": latest_value_on_or_before(labor_lookup, date, "labor_substitution_pressure_index"),
            "ai_concentration_index": latest_value_on_or_before({k: {"ai_concentration_index": v} for k, v in concentration_lookup.items()}, date, "ai_concentration_index"),
            "economic_fragility_index": latest_value_on_or_before(fragility_lookup, date, "economic_fragility_index"),
        })

    processed = {
        "cost_capability": cost_capability,
        "affordability_timeseries": affordability_timeseries,
        "occupation_panel": occupation_panel,
        "labor_index": labor_index,
        "concentration_index": concentration_index,
        "macro_fragility": fragility,
        "overview_indices": overview_indices,
    }
    for name, rows in processed.items():
        write_csv(config.paths.processed / f"{name}.csv", rows)
        write_json(config.paths.processed / f"{name}.json", rows)
    return processed


def build_marts(config: AppConfig, processed: dict[str, list[dict[str, object]]]) -> dict[str, str]:
    mart_paths: dict[str, str] = {}
    for name, rows in processed.items():
        csv_path = config.paths.marts / f"{name}.csv"
        json_path = config.paths.marts / f"{name}.json"
        write_csv(csv_path, rows)
        write_json(json_path, rows)
        mart_paths[name] = str(json_path.relative_to(config.root))

    source_registry = read_csv(config.paths.reference / "source_registry.csv")
    registry_lookup = {row["source_id"]: row for row in source_registry}
    freshness_rows = []
    for dataset in ["ai_pricing", "capability_benchmarks", "macro_labor", "occupation_labor"]:
        metadata = read_json(config.paths.raw / dataset / f"{dataset}_metadata.json")
        registry_row = registry_lookup.get(dataset, {})
        freshness_rows.append({
            "dataset": dataset,
            "captured_at": metadata.get("captured_at"),
            "mode": metadata.get("mode"),
            "status": "fallback" if "fallback" in str(metadata.get("mode")) else "source_backed",
            "description": registry_row.get("description", ""),
            "category": registry_row.get("category", ""),
        })
    write_csv(config.paths.marts / "data_freshness.csv", freshness_rows)
    write_json(config.paths.marts / "data_freshness.json", freshness_rows)
    mart_paths["data_freshness"] = str((config.paths.marts / "data_freshness.json").relative_to(config.root))
    return mart_paths


def build_dashboard_assets(config: AppConfig, processed: dict[str, list[dict[str, object]]]) -> None:
    freshness = read_json(config.paths.marts / "data_freshness.json")
    shares = read_csv(config.paths.reference / "concentration_reference.csv")
    registry = read_csv(config.paths.reference / "source_registry.csv")
    payload = {
        "overview": processed["overview_indices"],
        "affordability": processed["affordability_timeseries"],
        "cost_capability": processed["cost_capability"],
        "occupation_panel": processed["occupation_panel"],
        "labor_index": processed["labor_index"],
        "concentration_index": processed["concentration_index"],
        "macro_fragility": processed["macro_fragility"],
        "freshness": freshness,
        "shares": shares,
        "source_registry": registry,
    }
    write_json(config.root / "app" / "dashboard_data.json", payload)


def run_pipeline() -> PipelineArtifacts:
    config = AppConfig.discover()
    results = run_ingestion(config)
    processed = build_processed_tables(config)
    mart_paths = build_marts(config, processed)
    build_dashboard_assets(config, processed)
    artifacts = PipelineArtifacts(
        raw_metadata=[{"dataset_name": result.dataset_name, "used_fallback": result.used_fallback, "source_url": result.source_url, **result.snapshot_payload} for result in results],
        mart_paths=mart_paths,
        built_at=datetime.now(UTC).isoformat(),
    )
    write_json(config.paths.marts / "pipeline_artifacts.json", asdict(artifacts))
    logger.info("Pipeline finished successfully")
    return artifacts
