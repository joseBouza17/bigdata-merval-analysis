from __future__ import annotations

import numpy as np
import pandas as pd


def run_monte_carlo(
    returns_window: pd.DataFrame,
    weights: pd.Series,
    simulation_days: int,
    num_simulations: int,
    initial_value: float,
    random_seed: int,
    store_path_count: int,
    confidence_level: float,
) -> tuple[pd.DataFrame, dict, np.ndarray]:
    """Simulate future basket paths and return the sampled paths, summary, and final values."""
    cleaned = returns_window[weights.index].dropna().astype(float)
    mean_vector = cleaned.mean().values
    covariance_matrix = cleaned.cov().values + np.eye(cleaned.shape[1]) * 1e-12
    rng = np.random.default_rng(random_seed)

    # The path simulation stays intentionally simple for an academic project.
    simulated_asset_returns = rng.multivariate_normal(
        mean=mean_vector,
        cov=covariance_matrix,
        size=(num_simulations, simulation_days),
    )
    portfolio_returns = simulated_asset_returns @ weights.values
    cumulative_log_returns = portfolio_returns.cumsum(axis=1)
    portfolio_values = initial_value * np.exp(cumulative_log_returns)
    final_values = portfolio_values[:, -1]

    running_peaks = np.maximum.accumulate(portfolio_values, axis=1)
    drawdowns = portfolio_values / running_peaks - 1.0
    max_drawdowns = drawdowns.min(axis=1)

    alpha = 1.0 - confidence_level
    cutoff_value = np.quantile(final_values, alpha)
    worst_tail = final_values[final_values <= cutoff_value]

    sample_size = min(store_path_count, num_simulations)
    sampled_paths = portfolio_values[:sample_size]
    paths_frame = (
        pd.DataFrame(sampled_paths)
        .reset_index(names="simulation_id")
        .melt(id_vars="simulation_id", var_name="step", value_name="portfolio_value")
    )
    paths_frame["step"] = paths_frame["step"].astype(int) + 1

    summary = {
        "initial_value": float(initial_value),
        "num_simulations": int(num_simulations),
        "simulation_days": int(simulation_days),
        "mean_final_value": float(final_values.mean()),
        "median_final_value": float(np.median(final_values)),
        "min_final_value": float(final_values.min()),
        "max_final_value": float(final_values.max()),
        "percentile_5": float(np.quantile(final_values, 0.05)),
        "percentile_25": float(np.quantile(final_values, 0.25)),
        "percentile_75": float(np.quantile(final_values, 0.75)),
        "percentile_95": float(np.quantile(final_values, 0.95)),
        "probability_of_loss": float(np.mean(final_values < initial_value)),
        "expected_return_simulated": float(final_values.mean() / initial_value - 1.0),
        "var_95": float(initial_value - np.quantile(final_values, 0.05)),
        "cvar_95": float(initial_value - worst_tail.mean()) if worst_tail.size else np.nan,
        "max_drawdown_p50": float(np.quantile(max_drawdowns, 0.50)),
        "max_drawdown_p95": float(np.quantile(max_drawdowns, 0.05)),
    }
    return paths_frame, summary, final_values
