from __future__ import annotations

import os
from datetime import date
from pathlib import Path


# These settings keep the project easy to rerun across notebooks and dbt.
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "bigdata-financeargentina")
BIGQUERY_LOCATION = os.getenv("BIGQUERY_LOCATION", "US")
ANNUALIZATION_DAYS = 252
DEFAULT_START_DATE = os.getenv("MARKET_DATA_START_DATE", "2019-01-01")
DEFAULT_END_DATE = os.getenv("MARKET_DATA_END_DATE", date.today().isoformat())
CHART_OUTPUT_DIR = Path("outputs") / "charts"

DATASETS = {
    "raw": "raw_market",
    "processed": "processed_market",
    "analytics": "analytics_market",
    "dbt_processed": "dbt_processed_market",
    "serving": "serving_market",
}

RAW_TABLES = {
    "stock_prices": "stock_prices",
    "factor_prices": "factor_prices",
}

PROCESSED_TABLES = {
    "base_stock_prices": "base_stock_prices",
    "base_factor_prices": "base_factor_prices",
    "asset_returns": "asset_returns",
    "factor_returns": "factor_returns",
    "stock_metrics": "stock_metrics",
    "beta_metrics": "beta_metrics",
    "correlation_matrix_long": "correlation_matrix_long",
}

ANALYTICS_TABLES = {
    "basket_definitions": "basket_definitions",
    "horizon_definitions": "horizon_definitions",
    "basket_horizon_weights": "basket_horizon_weights",
    "basket_horizon_metrics": "basket_horizon_metrics",
    "basket_horizon_method_comparison": "basket_horizon_method_comparison",
    "basket_horizon_contributions": "basket_horizon_contributions",
    "monte_carlo_paths": "monte_carlo_paths",
    "monte_carlo_summary": "monte_carlo_summary",
    "investor_recommendation_summary": "investor_recommendation_summary",
    "metric_data_dictionary": "metric_data_dictionary",
}

FACTOR_SYMBOLS = {
    "MERVAL": "^MERV",
    "USDARS": "ARS=X",
    "VIX": "^VIX",
    "EEM": "EEM",
    "US10Y": "^TNX",
}

BASKETS = {
    "short_term_tactical": {
        "label": "Short-Term Tactical Basket",
        "tickers": ["GGAL.BA", "BMA.BA", "YPFD.BA", "EDN.BA", "TRAN.BA"],
        "sectors": {
            "GGAL.BA": "financials",
            "BMA.BA": "financials",
            "YPFD.BA": "energy",
            "EDN.BA": "utilities",
            "TRAN.BA": "utilities",
        },
        "macro_rationale": (
            "High beta mix tilted to financials and regulated energy-sensitive names "
            "for tactical positioning."
        ),
    },
    "medium_term_balanced": {
        "label": "Medium-Term Balanced Basket",
        "tickers": ["PAMP.BA", "CEPU.BA", "BYMA.BA", "TXAR.BA", "BMA.BA"],
        "sectors": {
            "PAMP.BA": "energy",
            "CEPU.BA": "utilities",
            "BYMA.BA": "financial_infrastructure",
            "TXAR.BA": "materials",
            "BMA.BA": "financials",
        },
        "macro_rationale": (
            "Balanced exposure across energy, utilities, market infrastructure, steel, "
            "and banking for mid-cycle participation."
        ),
    },
    "long_term_structural": {
        "label": "Long-Term Structural Basket",
        "tickers": ["PAMP.BA", "TGSU2.BA", "CEPU.BA", "BYMA.BA", "TXAR.BA"],
        "sectors": {
            "PAMP.BA": "energy",
            "TGSU2.BA": "energy_infrastructure",
            "CEPU.BA": "utilities",
            "BYMA.BA": "financial_infrastructure",
            "TXAR.BA": "materials",
        },
        "macro_rationale": (
            "Structural basket built around energy infrastructure, utilities, and "
            "capital-market leverage to longer reform cycles."
        ),
    },
}

HORIZONS = {
    "short_horizon": {
        "label": "Short Horizon",
        "lookback_days": 126,
        "evaluation_days": 63,
        "simulation_days": 63,
        "max_weight": 0.40,
        "min_weight": 0.05,
        "investor_profile_anchor": "aggressive",
        "description": (
            "Half-year estimation window with one-quarter evaluation and simulation "
            "horizon for tactical investors."
        ),
    },
    "medium_horizon": {
        "label": "Medium Horizon",
        "lookback_days": 252,
        "evaluation_days": 126,
        "simulation_days": 126,
        "max_weight": 0.35,
        "min_weight": 0.05,
        "investor_profile_anchor": "balanced",
        "description": (
            "One-year estimation window with half-year evaluation and simulation horizon "
            "for balanced investors."
        ),
    },
    "long_horizon": {
        "label": "Long Horizon",
        "lookback_days": 504,
        "evaluation_days": 252,
        "simulation_days": 252,
        "max_weight": 0.30,
        "min_weight": 0.05,
        "investor_profile_anchor": "conservative",
        "description": (
            "Two-year estimation window with one-year evaluation and simulation horizon "
            "for structural investors."
        ),
    },
}

OPTIMIZATION_METHODS = [
    "equal_weight",
    "max_sharpe",
    "min_volatility",
    "risk_parity",
]

SIMULATION_SETTINGS = {
    "initial_value": 100.0,
    "num_simulations": 2000,
    "store_path_count": 50,
    "confidence_level": 0.95,
    "random_seed": 42,
}

EQUITY_SYMBOLS = sorted(
    {
        ticker
        for basket_config in BASKETS.values()
        for ticker in basket_config["tickers"]
    }
)


def basket_rows(run_id: str, ingestion_timestamp) -> list[dict]:
    """Return the canonical basket membership rows for warehouse storage."""
    rows = []
    for basket_name, basket_config in BASKETS.items():
        for basket_order, ticker in enumerate(basket_config["tickers"], start=1):
            rows.append(
                {
                    "basket_name": basket_name,
                    "basket_label": basket_config["label"],
                    "ticker": ticker,
                    "basket_order": basket_order,
                    "sector": basket_config["sectors"].get(ticker),
                    "macro_rationale": basket_config["macro_rationale"],
                    "run_id": run_id,
                    "ingestion_timestamp": ingestion_timestamp,
                }
            )
    return rows


def horizon_rows(run_id: str, ingestion_timestamp) -> list[dict]:
    """Return the canonical horizon definition rows for warehouse storage."""
    rows = []
    for horizon_name, horizon_config in HORIZONS.items():
        rows.append(
            {
                "horizon_name": horizon_name,
                "horizon_label": horizon_config["label"],
                "lookback_days": horizon_config["lookback_days"],
                "evaluation_days": horizon_config["evaluation_days"],
                "simulation_days": horizon_config["simulation_days"],
                "max_weight": horizon_config["max_weight"],
                "min_weight": horizon_config["min_weight"],
                "investor_profile_anchor": horizon_config["investor_profile_anchor"],
                "description": horizon_config["description"],
                "run_id": run_id,
                "ingestion_timestamp": ingestion_timestamp,
            }
        )
    return rows
