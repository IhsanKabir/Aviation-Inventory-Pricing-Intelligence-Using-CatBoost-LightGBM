from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_AIRPORT_COUNTRIES = REPO_ROOT / "config" / "airport_countries.json"
DEFAULT_MARKET_PRIORS = REPO_ROOT / "config" / "market_priors.json"


def _load_json(path: Path) -> dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def load_market_priors(path: str | Path | None = None) -> dict[str, Any]:
    base = {
        "labor_flow_origin_countries": ["BD", "IN", "PK", "NP"],
        "middle_east_destination_countries": ["SA", "AE", "QA", "OM", "KW", "BH"],
        "ksa_airports": ["JED", "RUH", "MED", "DMM"],
        "thailand_tourism_airports": ["BKK", "DMK", "HKT", "CNX", "KBV"],
        "hub_spoke_airlines": ["SQ", "EK", "QR", "MH", "TG", "WY", "SV", "UL", "CZ"],
        "lcc_airlines": ["6E", "G9", "3L", "8D", "FZ", "AK", "OD", "BS", "2A", "VQ", "F8"],
        "return_oriented_airlines": ["BG", "BS"],
        "yield_route_overrides": {"high": [], "tourism": [], "balanced": []},
        "horizon_windows": {
            "visa_window_max_days": 7,
            "short_max_days": 30,
            "mid_max_days": 90,
            "long_max_days": 180,
        },
    }

    cfg_path = Path(path) if path else DEFAULT_MARKET_PRIORS
    cfg = _load_json(cfg_path)
    if cfg:
        base.update({k: v for k, v in cfg.items() if k in base})
        if isinstance(cfg.get("horizon_windows"), dict):
            base["horizon_windows"].update(cfg["horizon_windows"])
        if isinstance(cfg.get("yield_route_overrides"), dict):
            y = dict(base["yield_route_overrides"])
            y.update(cfg["yield_route_overrides"])
            base["yield_route_overrides"] = y
    return base


def load_airport_country_map(path: str | Path | None = None) -> dict[str, str]:
    cfg_path = Path(path) if path else DEFAULT_AIRPORT_COUNTRIES
    raw = _load_json(cfg_path)
    out: dict[str, str] = {}
    for k, v in raw.items():
        kk = str(k or "").strip().upper()
        vv = str(v or "").strip().upper()
        if kk and vv:
            out[kk] = vv
    return out


def _norm_set(values: Any) -> set[str]:
    out: set[str] = set()
    if not isinstance(values, (list, tuple, set)):
        return out
    for v in values:
        s = str(v or "").strip().upper()
        if s:
            out.add(s)
    return out


def _ensure_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(index=df.index, dtype="object")
    return df[col]


def apply_market_priors(
    df: pd.DataFrame,
    *,
    origin_col: str = "origin",
    destination_col: str = "destination",
    airline_col: str = "airline",
    days_to_departure_col: str = "days_to_departure",
    route_col: str = "route_key",
    priors_path: str | Path | None = None,
    airport_country_path: str | Path | None = None,
) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    out = df.copy()
    priors = load_market_priors(priors_path)
    airport_country = load_airport_country_map(airport_country_path)

    labor_countries = _norm_set(priors.get("labor_flow_origin_countries"))
    me_countries = _norm_set(priors.get("middle_east_destination_countries"))
    ksa_airports = _norm_set(priors.get("ksa_airports"))
    thai_airports = _norm_set(priors.get("thailand_tourism_airports"))
    hub_airlines = _norm_set(priors.get("hub_spoke_airlines"))
    lcc_airlines = _norm_set(priors.get("lcc_airlines"))
    return_airlines = _norm_set(priors.get("return_oriented_airlines"))
    route_overrides = priors.get("yield_route_overrides") if isinstance(priors.get("yield_route_overrides"), dict) else {}
    yield_high = _norm_set(route_overrides.get("high"))
    yield_tourism = _norm_set(route_overrides.get("tourism"))
    yield_balanced = _norm_set(route_overrides.get("balanced"))

    origin = _ensure_series(out, origin_col).astype(str).str.strip().str.upper()
    dest = _ensure_series(out, destination_col).astype(str).str.strip().str.upper()
    airline = _ensure_series(out, airline_col).astype(str).str.strip().str.upper()
    route_key = _ensure_series(out, route_col).astype(str).str.strip().str.upper()
    if route_col not in out.columns:
        route_key = origin.fillna("") + "-" + dest.fillna("")
        out[route_col] = route_key

    origin_country = origin.map(lambda x: airport_country.get(x, "UNK"))
    dest_country = dest.map(lambda x: airport_country.get(x, "UNK"))
    out["origin_country"] = origin_country
    out["destination_country"] = dest_country

    is_me_route = dest_country.isin(me_countries) | origin_country.isin(me_countries)
    is_ksa_route = dest.isin(ksa_airports) | origin.isin(ksa_airports)
    is_th_tourism = dest.isin(thai_airports) | origin.isin(thai_airports)
    is_labor_outbound = origin_country.isin(labor_countries) & dest_country.isin(me_countries)
    is_labor_return = origin_country.isin(me_countries) & dest_country.isin(labor_countries)
    is_hub_spoke = airline.isin(hub_airlines)
    is_lcc = airline.isin(lcc_airlines)
    is_return_oriented = airline.isin(return_airlines)

    out["market_is_middle_east"] = is_me_route.astype(int)
    out["market_is_ksa"] = is_ksa_route.astype(int)
    out["market_is_thailand_tourism"] = is_th_tourism.astype(int)
    out["market_is_labor_outbound"] = is_labor_outbound.astype(int)
    out["market_is_labor_return"] = is_labor_return.astype(int)
    out["airline_is_hub_spoke"] = is_hub_spoke.astype(int)
    out["airline_is_lcc"] = is_lcc.astype(int)
    out["airline_is_return_oriented"] = is_return_oriented.astype(int)

    out["airline_model_proxy"] = pd.Series("hybrid", index=out.index, dtype="object")
    out.loc[is_hub_spoke, "airline_model_proxy"] = "hub_spoke"
    out.loc[is_lcc, "airline_model_proxy"] = "lcc"

    out["trip_purpose_proxy"] = pd.Series("general", index=out.index, dtype="object")
    out.loc[is_labor_outbound, "trip_purpose_proxy"] = "labor_outbound"
    out.loc[is_labor_return, "trip_purpose_proxy"] = "labor_return"
    out.loc[is_th_tourism & ~(is_labor_outbound | is_labor_return), "trip_purpose_proxy"] = "tourism"

    out["yield_class_proxy"] = pd.Series("unknown", index=out.index, dtype="object")
    out.loc[route_key.isin(yield_high), "yield_class_proxy"] = "high"
    out.loc[route_key.isin(yield_tourism), "yield_class_proxy"] = "tourism"
    out.loc[route_key.isin(yield_balanced), "yield_class_proxy"] = "balanced"
    out.loc[(out["yield_class_proxy"] == "unknown") & is_ksa_route, "yield_class_proxy"] = "high"
    out.loc[(out["yield_class_proxy"] == "unknown") & is_me_route, "yield_class_proxy"] = "medium_high"

    dtd = pd.to_numeric(_ensure_series(out, days_to_departure_col), errors="coerce")
    horizon_windows = priors.get("horizon_windows") if isinstance(priors.get("horizon_windows"), dict) else {}
    visa_max = int(horizon_windows.get("visa_window_max_days", 7))
    short_max = int(horizon_windows.get("short_max_days", 30))
    mid_max = int(horizon_windows.get("mid_max_days", 90))
    long_max = int(horizon_windows.get("long_max_days", 180))

    out["horizon_is_visa_window"] = (dtd <= visa_max).fillna(False).astype(int)
    out["horizon_is_long_window"] = (dtd >= mid_max).fillna(False).astype(int)
    out["horizon_bucket_proxy"] = pd.Series("unknown", index=out.index, dtype="object")
    out.loc[dtd.notna() & (dtd <= visa_max), "horizon_bucket_proxy"] = "D0_visa"
    out.loc[dtd.notna() & (dtd > visa_max) & (dtd <= short_max), "horizon_bucket_proxy"] = "D8_30"
    out.loc[dtd.notna() & (dtd > short_max) & (dtd <= mid_max), "horizon_bucket_proxy"] = "D31_90"
    out.loc[dtd.notna() & (dtd > mid_max) & (dtd <= long_max), "horizon_bucket_proxy"] = "D91_180"
    out.loc[dtd.notna() & (dtd > long_max), "horizon_bucket_proxy"] = "D181p"

    return out
