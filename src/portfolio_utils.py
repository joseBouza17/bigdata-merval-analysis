from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd
from scipy.optimize import minimize

from src.config import ANALYTICS_TABLES, ANNUALIZATION_DAYS, PROCESSED_TABLES


def annualize_return(mean_daily_return: float, trading_days: int = ANNUALIZATION_DAYS) -> float:
    """Scale an average daily return to an annualized return."""
    return float(mean_daily_return) * float(trading_days)


def annualize_volatility(daily_volatility: float, trading_days: int = ANNUALIZATION_DAYS) -> float:
    """Scale a daily volatility estimate to annualized volatility."""
    return float(daily_volatility) * np.sqrt(float(trading_days))


def sharpe_ratio(expected_return: float, volatility: float, risk_free_rate: float = 0.0) -> float:
    """Compute a simple Sharpe ratio from annualized inputs."""
    if volatility is None or np.isnan(volatility) or volatility <= 0:
        return np.nan
    return (float(expected_return) - float(risk_free_rate)) / float(volatility)


def compute_beta(asset_returns: pd.Series, factor_returns: pd.Series | None) -> float:
    """Estimate beta with a simple covariance-over-variance approach."""
    if factor_returns is None:
        return np.nan
    aligned = pd.concat([asset_returns, factor_returns], axis=1).dropna()
    if aligned.shape[0] < 10:
        return np.nan
    factor_var = aligned.iloc[:, 1].var(ddof=0)
    if factor_var <= 0 or np.isnan(factor_var):
        return np.nan
    covariance = np.cov(aligned.iloc[:, 0], aligned.iloc[:, 1], ddof=0)[0, 1]
    return float(covariance / factor_var)


def max_drawdown_from_returns(log_returns: pd.Series) -> float:
    """Measure the worst peak-to-trough loss from a log-return series."""
    if log_returns.empty:
        return np.nan
    wealth_index = np.exp(log_returns.cumsum())
    running_peak = wealth_index.cummax()
    drawdown = wealth_index / running_peak - 1.0
    return float(drawdown.min())


def downside_deviation(return_series: pd.Series, target_return: float = 0.0) -> float:
    """Calculate annualized downside deviation for Sortino analysis."""
    clean = return_series.dropna()
    if clean.empty:
        return np.nan
    negative_returns = clean[clean < target_return]
    if len(negative_returns) < 2:
        return np.nan
    return annualize_volatility(float(negative_returns.std(ddof=0)))


def downside_frequency(return_series: pd.Series, target_return: float = 0.0) -> float:
    """Calculate the share of observations that fall below the target return."""
    clean = return_series.dropna()
    if clean.empty:
        return np.nan
    return float((clean < target_return).mean())


def calculate_sortino(returns: pd.Series, rf_daily: pd.Series | float | None = None) -> float:
    """Calculate a Sortino ratio using annualized excess return over downside deviation."""
    clean = returns.dropna()
    if clean.empty:
        return np.nan

    if isinstance(rf_daily, pd.Series):
        rf_mean = rf_daily.dropna().mean() if not rf_daily.dropna().empty else 0.0
    elif rf_daily is None:
        rf_mean = 0.0
    else:
        rf_mean = float(rf_daily)

    annualized_excess_return = annualize_return(clean.mean() - rf_mean)
    dd = downside_deviation(clean)
    if dd == 0 or np.isnan(dd):
        return np.nan
    return float(annualized_excess_return / dd)


def calculate_calmar(returns: pd.Series) -> float:
    """Calculate Calmar ratio as annualized return divided by absolute max drawdown."""
    clean = returns.dropna()
    if clean.empty:
        return np.nan
    annualized_return = annualize_return(clean.mean())
    mdd = max_drawdown_from_returns(clean)
    if mdd == 0 or np.isnan(mdd):
        return np.nan
    return float(annualized_return / abs(mdd))


def classify_stock(row: pd.Series) -> str:
    """Assign a simple stock profile for presentation use."""
    if row.get("sharpe_ratio", np.nan) >= 1.0 and row.get("volatility", np.inf) < 0.35:
        return "conservative"
    if row.get("beta_vs_merval", np.nan) > 1.2 and row.get("average_return", -np.inf) > 0.20:
        return "growth"
    if row.get("volatility", 0.0) > 0.45:
        return "aggressive"
    return "balanced"


def concentration_hhi(weights: pd.Series) -> float:
    """Measure concentration with the Herfindahl-Hirschman Index."""
    return float(np.square(weights).sum())


def effective_number_of_assets(weights: pd.Series) -> float:
    """Translate concentration into an intuitive effective asset count."""
    hhi = concentration_hhi(weights)
    if hhi <= 0:
        return np.nan
    return float(1.0 / hhi)


def diversification_ratio(weights: pd.Series, covariance_matrix: pd.DataFrame) -> float:
    """Compare weighted stand-alone risk to covariance-aware portfolio risk."""
    covariance = covariance_matrix.loc[weights.index, weights.index]
    asset_volatility = np.sqrt(np.diag(covariance.values))
    weighted_average_asset_volatility = float(weights.values @ asset_volatility)
    portfolio_volatility = float(np.sqrt(weights.values.T @ covariance.values @ weights.values))
    if portfolio_volatility <= 0 or np.isnan(portfolio_volatility):
        return np.nan
    return weighted_average_asset_volatility / portfolio_volatility


def diversification_effect(weights: pd.Series, covariance_matrix: pd.DataFrame) -> float:
    """Show how much risk falls once correlations are considered."""
    covariance = covariance_matrix.loc[weights.index, weights.index]
    asset_volatility = np.sqrt(np.diag(covariance.values))
    weighted_average_asset_volatility = float(weights.values @ asset_volatility)
    portfolio_volatility = float(np.sqrt(weights.values.T @ covariance.values @ weights.values))
    return float(weighted_average_asset_volatility - portfolio_volatility)


def validate_weights(
    weights: pd.Series,
    min_weight: float = 0.0,
    max_weight: float = 1.0,
    tolerance: float = 1e-6,
) -> pd.Series:
    """Validate long-only portfolio weights."""
    cleaned = weights.astype(float).copy()
    if (cleaned < (min_weight - tolerance)).any():
        raise ValueError("Weights fall below the configured minimum bound.")
    if (cleaned > (max_weight + tolerance)).any():
        raise ValueError("Weights exceed the configured maximum bound.")
    total_weight = cleaned.sum()
    if not np.isclose(total_weight, 1.0, atol=tolerance):
        raise ValueError(f"Weights must sum to 1. Current sum: {total_weight:.8f}")
    return cleaned


def equal_weight_weights(tickers: Iterable[str]) -> pd.Series:
    """Create a simple equal-weight baseline for a basket."""
    tickers = list(tickers)
    if not tickers:
        raise ValueError("At least one ticker is required.")
    equal_weight = 1.0 / len(tickers)
    return pd.Series(equal_weight, index=tickers, dtype="float64")


def _returns_inputs(returns_window: pd.DataFrame):
    """Prepare the return matrix used by the optimization routines."""
    cleaned = returns_window.dropna().astype(float)
    if cleaned.empty:
        raise ValueError("Return window is empty after dropping nulls.")
    mean_vector = cleaned.mean().values
    covariance_matrix = cleaned.cov().values
    covariance_matrix = covariance_matrix + np.eye(covariance_matrix.shape[0]) * 1e-12
    tickers = list(cleaned.columns)
    return cleaned, tickers, mean_vector, covariance_matrix


def _bounds(num_assets: int, min_weight: float, max_weight: float):
    """Build the SLSQP bounds tuple once per optimization run."""
    return tuple((min_weight, max_weight) for _ in range(num_assets))


def _solve_optimization(objective, initial_guess, bounds):
    """Solve a long-only optimization with weights summing to 1."""
    constraints = ({"type": "eq", "fun": lambda w: np.sum(w) - 1.0},)
    result = minimize(
        objective,
        initial_guess,
        method="SLSQP",
        bounds=bounds,
        constraints=constraints,
        options={"maxiter": 500, "ftol": 1e-12},
    )
    if not result.success:
        return initial_guess
    return result.x


def optimize_max_sharpe(
    returns_window: pd.DataFrame,
    min_weight: float,
    max_weight: float,
    trading_days: int = ANNUALIZATION_DAYS,
) -> pd.Series:
    """Optimize for the highest estimated Sharpe ratio within the basket."""
    cleaned, tickers, mean_vector, covariance_matrix = _returns_inputs(returns_window)
    initial_guess = np.repeat(1.0 / len(tickers), len(tickers))
    bounds = _bounds(len(tickers), min_weight, max_weight)

    # This objective keeps the notebook logic short and easy to explain.
    def objective(weights):
        portfolio_return = annualize_return(weights @ mean_vector, trading_days)
        portfolio_vol = annualize_volatility(
            np.sqrt(weights.T @ covariance_matrix @ weights),
            trading_days,
        )
        score = sharpe_ratio(portfolio_return, portfolio_vol)
        return -score if np.isfinite(score) else 1e9

    optimized = _solve_optimization(objective, initial_guess, bounds)
    return pd.Series(optimized, index=tickers, dtype="float64")


def optimize_min_volatility(
    returns_window: pd.DataFrame,
    min_weight: float,
    max_weight: float,
    trading_days: int = ANNUALIZATION_DAYS,
) -> pd.Series:
    """Optimize for the lowest estimated volatility within the basket."""
    cleaned, tickers, _, covariance_matrix = _returns_inputs(returns_window)
    initial_guess = np.repeat(1.0 / len(tickers), len(tickers))
    bounds = _bounds(len(tickers), min_weight, max_weight)

    def objective(weights):
        return annualize_volatility(np.sqrt(weights.T @ covariance_matrix @ weights), trading_days)

    optimized = _solve_optimization(objective, initial_guess, bounds)
    return pd.Series(optimized, index=tickers, dtype="float64")


def optimize_risk_parity(
    returns_window: pd.DataFrame,
    min_weight: float,
    max_weight: float,
) -> pd.Series:
    """Approximate equal risk contribution with a simple SLSQP objective."""
    cleaned, tickers, _, covariance_matrix = _returns_inputs(returns_window)
    initial_guess = np.repeat(1.0 / len(tickers), len(tickers))
    bounds = _bounds(len(tickers), min_weight, max_weight)

    def objective(weights):
        portfolio_vol = np.sqrt(weights.T @ covariance_matrix @ weights)
        if portfolio_vol <= 0 or np.isnan(portfolio_vol):
            return 1e9
        marginal_contrib = covariance_matrix @ weights / portfolio_vol
        risk_contrib = weights * marginal_contrib
        target_contrib = np.repeat(portfolio_vol / len(weights), len(weights))
        return np.square(risk_contrib - target_contrib).sum()

    optimized = _solve_optimization(objective, initial_guess, bounds)
    return pd.Series(optimized, index=tickers, dtype="float64")


def _lookup_series(frame: pd.DataFrame, column_name: str, index: pd.Index) -> pd.Series:
    """Safely fetch a column after reindexing a lookup table."""
    if column_name not in frame.columns:
        return pd.Series(np.nan, index=index, dtype="float64")
    return frame.reindex(index)[column_name].astype("float64")


def _rank_score(series: pd.Series, higher_is_better: bool = True) -> pd.Series:
    """Convert a metric into a 0-1 rank score where 1 is best."""
    values = pd.to_numeric(series, errors="coerce")
    scores = values.rank(method="average", pct=True, ascending=higher_is_better)
    return scores.fillna(0.0).astype("float64")


def _score_within_horizon(
    frame: pd.DataFrame,
    column_name: str,
    higher_is_better: bool = True,
) -> pd.Series:
    """Rank a metric within each horizon so different horizons are not mixed directly."""
    return frame.groupby("horizon_name", group_keys=False)[column_name].transform(
        lambda series: _rank_score(series, higher_is_better=higher_is_better)
    )


def compute_portfolio_contributions(
    returns_window: pd.DataFrame,
    weights: pd.Series,
    trading_days: int = ANNUALIZATION_DAYS,
) -> pd.DataFrame:
    """Calculate contribution to return and contribution to risk by ticker."""
    cleaned_returns = returns_window[weights.index].dropna()
    mean_daily_returns = cleaned_returns.mean()
    cov_daily = cleaned_returns.cov()

    contribution_to_return = weights * mean_daily_returns.apply(
        lambda value: annualize_return(value, trading_days)
    )

    portfolio_variance_daily = float(weights.T @ cov_daily.values @ weights)
    portfolio_vol_daily = np.sqrt(portfolio_variance_daily)

    # Contribution to risk uses marginal contribution from the covariance matrix.
    if np.isclose(portfolio_vol_daily, 0):
        contribution_to_risk_pct = pd.Series(np.nan, index=weights.index, dtype="float64")
    else:
        marginal_contrib = cov_daily.values @ weights.values
        component_contrib = weights.values * marginal_contrib / portfolio_vol_daily
        risk_total = component_contrib.sum()
        contribution_to_risk_pct = pd.Series(
            component_contrib / risk_total if not np.isclose(risk_total, 0) else np.nan,
            index=weights.index,
            dtype="float64",
        )

    return pd.DataFrame(
        {
            "ticker": weights.index,
            "weight": weights.values,
            "contribution_to_return": contribution_to_return.reindex(weights.index).values,
            "contribution_to_risk_pct": contribution_to_risk_pct.reindex(weights.index).values,
        }
    )


def evaluate_portfolio(
    estimation_returns: pd.DataFrame,
    evaluation_returns: pd.DataFrame,
    weights: pd.Series,
    stock_metrics: pd.DataFrame,
    beta_metrics: pd.DataFrame,
    trading_days: int = ANNUALIZATION_DAYS,
) -> dict:
    """Calculate the main historical metrics for one basket-horizon-method combination."""
    weights = weights.astype(float)
    estimation_returns = estimation_returns[weights.index].dropna()
    evaluation_returns = evaluation_returns[weights.index].dropna()
    stock_lookup = stock_metrics.set_index("ticker") if "ticker" in stock_metrics.columns else stock_metrics.copy()
    beta_lookup = beta_metrics.set_index("ticker") if "ticker" in beta_metrics.columns else beta_metrics.copy()

    mean_vector = estimation_returns.mean()
    covariance_matrix = estimation_returns.cov()
    portfolio_daily_returns = evaluation_returns @ weights
    downside_returns = portfolio_daily_returns[portfolio_daily_returns < 0]

    expected_portfolio_return = annualize_return(float(weights @ mean_vector), trading_days)
    portfolio_volatility = annualize_volatility(
        float(
            np.sqrt(
                weights.values.T
                @ covariance_matrix.loc[weights.index, weights.index].values
                @ weights.values
            )
        ),
        trading_days,
    )
    weighted_sharpe = sharpe_ratio(expected_portfolio_return, portfolio_volatility)
    max_drawdown = max_drawdown_from_returns(portfolio_daily_returns)
    realized_cumulative_return = float(np.exp(portfolio_daily_returns.sum()) - 1.0)
    downside_deviation_value = annualize_volatility(
        float(downside_returns.std(ddof=0) if not downside_returns.empty else 0.0),
        trading_days,
    )
    sortino_ratio = calculate_sortino(portfolio_daily_returns, 0.0)
    calmar_ratio = calculate_calmar(portfolio_daily_returns)
    avg_pairwise_correlation = float(
        evaluation_returns.corr().where(~np.eye(len(weights), dtype=bool)).stack().mean()
    )

    beta_vs_merval = _lookup_series(stock_lookup, "beta_vs_merval", weights.index)
    beta_vs_eem = _lookup_series(beta_lookup, "beta_vs_eem", weights.index)
    corr_with_fx = _lookup_series(stock_lookup, "corr_with_fx", weights.index)
    corr_with_merval = _lookup_series(stock_lookup, "corr_with_merval", weights.index)

    weighted_beta_merval = float((weights * beta_vs_merval.fillna(0)).sum())
    weighted_beta_eem = float((weights * beta_vs_eem.fillna(0)).sum())
    weighted_corr_fx = float((weights * corr_with_fx.fillna(0)).sum())
    weighted_corr_merval = float((weights * corr_with_merval.fillna(0)).sum())

    return {
        "expected_portfolio_return": expected_portfolio_return,
        "portfolio_volatility": portfolio_volatility,
        "weighted_sharpe": weighted_sharpe,
        "sortino_ratio": sortino_ratio,
        "calmar_ratio": calmar_ratio,
        "max_drawdown": max_drawdown,
        "realized_cumulative_return": realized_cumulative_return,
        "downside_deviation": downside_deviation_value,
        "concentration_risk_hhi": concentration_hhi(weights),
        "effective_number_of_assets": effective_number_of_assets(weights),
        "diversification_ratio": diversification_ratio(weights, covariance_matrix),
        "diversification_effect": annualize_volatility(
            diversification_effect(weights, covariance_matrix),
            trading_days,
        ),
        "average_pairwise_correlation": avg_pairwise_correlation,
        "weighted_beta_merval": weighted_beta_merval,
        "weighted_beta_eem": weighted_beta_eem,
        "weighted_corr_fx": weighted_corr_fx,
        "weighted_corr_merval": weighted_corr_merval,
        "num_assets": int(len(weights)),
    }


def attach_risk_profiles(
    metrics_df: pd.DataFrame,
    monte_carlo_df: pd.DataFrame,
) -> pd.DataFrame:
    """Add heuristic basket-horizon risk labels after simulation results are available."""
    enriched = metrics_df.copy()
    simulation_lookup = monte_carlo_df.set_index("basket_horizon_id")
    profile_rows = []

    for _, row in enriched.iterrows():
        simulation_row = (
            simulation_lookup.loc[row["basket_horizon_id"]]
            if row["basket_horizon_id"] in simulation_lookup.index
            else pd.Series(dtype="object")
        )
        risk_profile, risk_profile_reason = classify_basket_horizon_profile(row, simulation_row)
        profile_rows.append(
            {
                "basket_horizon_id": row["basket_horizon_id"],
                "risk_profile": risk_profile,
                "risk_profile_reason": risk_profile_reason,
            }
        )

    profile_df = pd.DataFrame(profile_rows)
    return enriched.merge(profile_df, on="basket_horizon_id", how="left")


def classify_basket_horizon_profile(metric_row: pd.Series, simulation_row: pd.Series) -> tuple[str, str]:
    """Create a plain-English risk label for one basket-horizon allocation."""
    probability_of_loss = simulation_row.get("probability_of_loss", np.nan)
    weighted_beta_merval = metric_row.get("weighted_beta_merval", np.nan)
    portfolio_volatility = metric_row.get("portfolio_volatility", np.nan)
    weighted_corr_fx = metric_row.get("weighted_corr_fx", 0.0)

    if abs(weighted_corr_fx) >= 0.45 and probability_of_loss >= 0.35:
        return (
            "high-risk fx-sensitive",
            "High downside probability and strong FX sensitivity make this allocation especially fragile.",
        )
    if portfolio_volatility <= 0.20 and (pd.isna(weighted_beta_merval) or weighted_beta_merval <= 0.90) and probability_of_loss <= 0.25:
        return (
            "conservative",
            "Low volatility, contained market beta, and a relatively low probability of loss support a conservative label.",
        )
    if portfolio_volatility >= 0.35 or (pd.notna(weighted_beta_merval) and weighted_beta_merval >= 1.20):
        return (
            "aggressive",
            "High volatility and strong market beta suggest larger upside-downside swings.",
        )
    if metric_row.get("expected_portfolio_return", 0.0) >= 0.20 and weighted_beta_merval >= 1.05:
        return (
            "growth",
            "Higher return ambition and above-market beta make this allocation more growth-oriented.",
        )
    return (
        "balanced",
        "Risk and return sit between defensive and aggressive thresholds.",
    )


def _winner_reason(row: pd.Series) -> str:
    """Explain why one method won inside a basket-horizon cell."""
    if row.get("tail_resilience_score", 0.0) >= 0.75 and row.get("risk_control_score", 0.0) >= 0.60:
        return "Best downside resilience after balancing simulation tail risk and historical drawdown."
    if row.get("return_score", 0.0) >= 0.75 and row.get("risk_adjusted_score", 0.0) >= 0.60:
        return "Best upside profile after applying no-short and horizon-specific weight constraints."
    if row.get("diversification_quality_score", 0.0) >= 0.75:
        return "Best diversification and concentration trade-off for this basket-horizon cell."
    return "Best balanced score across return quality, downside risk, and diversification."


def build_method_comparison(metrics_df: pd.DataFrame, monte_carlo_df: pd.DataFrame) -> pd.DataFrame:
    """Rank weighting methods within each basket-horizon cell and basket winners within each horizon."""
    comparison = metrics_df.merge(
        monte_carlo_df[
            [
                "basket_horizon_id",
                "initial_value",
                "probability_of_loss",
                "var_95",
                "cvar_95",
                "mean_final_value",
                "median_final_value",
                "expected_return_simulated",
                "max_drawdown_p50",
                "max_drawdown_p95",
            ]
        ],
        on="basket_horizon_id",
        how="left",
    )

    initial_value = pd.to_numeric(comparison["initial_value"], errors="coerce").replace(0, np.nan)
    comparison["var_95_pct_of_initial"] = pd.to_numeric(
        comparison["var_95"],
        errors="coerce",
    ) / initial_value
    comparison["cvar_95_pct_of_initial"] = pd.to_numeric(
        comparison["cvar_95"],
        errors="coerce",
    ) / initial_value
    comparison["simulation_tail_drawdown_abs"] = pd.to_numeric(
        comparison["max_drawdown_p95"],
        errors="coerce",
    ).abs()
    comparison["historical_drawdown_abs"] = pd.to_numeric(
        comparison["max_drawdown"],
        errors="coerce",
    ).abs()

    sortino_score = _score_within_horizon(comparison, "sortino_ratio", higher_is_better=True)
    sharpe_score = _score_within_horizon(comparison, "weighted_sharpe", higher_is_better=True)
    comparison["risk_adjusted_score"] = sortino_score.where(
        pd.to_numeric(comparison["sortino_ratio"], errors="coerce").notna(),
        sharpe_score,
    )
    comparison["return_score"] = _score_within_horizon(
        comparison,
        "expected_portfolio_return",
        higher_is_better=True,
    )
    comparison["risk_control_score"] = (
        _score_within_horizon(comparison, "portfolio_volatility", higher_is_better=False) * 0.50
        + _score_within_horizon(comparison, "historical_drawdown_abs", higher_is_better=False) * 0.50
    )
    comparison["tail_resilience_score"] = (
        _score_within_horizon(comparison, "probability_of_loss", higher_is_better=False) * 0.40
        + _score_within_horizon(comparison, "cvar_95_pct_of_initial", higher_is_better=False) * 0.40
        + _score_within_horizon(comparison, "simulation_tail_drawdown_abs", higher_is_better=False)
        * 0.20
    )
    comparison["diversification_quality_score"] = (
        _score_within_horizon(comparison, "concentration_risk_hhi", higher_is_better=False) * 0.60
        + _score_within_horizon(comparison, "diversification_ratio", higher_is_better=True) * 0.40
    )

    comparison["selection_score_version"] = "balanced_rank_v2"
    comparison["selection_score"] = 100.0 * (
        comparison["risk_adjusted_score"] * 0.30
        + comparison["return_score"] * 0.15
        + comparison["risk_control_score"] * 0.20
        + comparison["tail_resilience_score"] * 0.20
        + comparison["diversification_quality_score"] * 0.15
    )

    baseline = comparison[comparison["weighting_method"] == "equal_weight"][
        [
            "basket_name",
            "horizon_name",
            "expected_portfolio_return",
            "weighted_sharpe",
            "probability_of_loss",
        ]
    ].rename(
        columns={
            "expected_portfolio_return": "equal_weight_expected_return",
            "weighted_sharpe": "equal_weight_sharpe",
            "probability_of_loss": "equal_weight_probability_of_loss",
        }
    )

    comparison = comparison.merge(
        baseline,
        on=["basket_name", "horizon_name"],
        how="left",
    )
    comparison["equal_weight_return_delta"] = (
        comparison["expected_portfolio_return"] - comparison["equal_weight_expected_return"]
    )
    comparison["equal_weight_sharpe_delta"] = comparison["weighted_sharpe"] - comparison["equal_weight_sharpe"]
    comparison["equal_weight_probability_of_loss_delta"] = (
        comparison["probability_of_loss"] - comparison["equal_weight_probability_of_loss"]
    )

    comparison["method_rank_within_basket_horizon"] = comparison.groupby(
        ["basket_name", "horizon_name"]
    )["selection_score"].rank(method="dense", ascending=False)
    comparison["is_best_method_for_basket_horizon"] = comparison["method_rank_within_basket_horizon"] == 1

    basket_best_scores = (
        comparison.groupby(["horizon_name", "basket_name"], as_index=False)["selection_score"]
        .max()
        .rename(columns={"selection_score": "basket_best_selection_score"})
    )
    basket_best_scores["basket_rank_within_horizon"] = basket_best_scores.groupby("horizon_name")[
        "basket_best_selection_score"
    ].rank(method="dense", ascending=False)
    comparison = comparison.merge(basket_best_scores, on=["horizon_name", "basket_name"], how="left")
    comparison["is_best_basket_for_horizon"] = (
        comparison["is_best_method_for_basket_horizon"]
        & (comparison["basket_rank_within_horizon"] == 1)
    )
    comparison["horizon_winner_reason"] = comparison.apply(_winner_reason, axis=1)
    return comparison


def _key_risk(row: pd.Series) -> str:
    """Summarize the main residual risk in one sentence."""
    if row.get("concentration_risk_hhi", 0.0) >= 0.26:
        return "High concentration risk remains inside the basket."
    if abs(row.get("weighted_corr_fx", 0.0)) >= 0.45:
        return "The allocation stays sensitive to FX shocks."
    if row.get("weighted_beta_merval", 0.0) >= 1.10:
        return "The allocation remains highly exposed to broad MERVAL swings."
    return "Macro regulation and energy-policy shocks remain the key residual risks."


def build_investor_recommendations(comparison_df: pd.DataFrame) -> pd.DataFrame:
    """Recommend the best basket-horizon combination for each investor profile."""
    best_methods = comparison_df[comparison_df["is_best_method_for_basket_horizon"]].copy()
    recommendations = []

    horizon_winners = best_methods[best_methods["is_best_basket_for_horizon"]].copy()
    for _, row in horizon_winners.iterrows():
        recommendations.append(
            {
                "recommendation_scope": "horizon",
                "recommendation_key": row["horizon_name"],
                "investor_profile": None,
                "horizon_name": row["horizon_name"],
                "basket_name": row["basket_name"],
                "weighting_method": row["weighting_method"],
                "basket_horizon_id": row["basket_horizon_id"],
                "recommendation_rank": 1,
                "selection_score": row["selection_score"],
                "recommendation_reason": row["horizon_winner_reason"],
                "key_risk": _key_risk(row),
            }
        )

    if not best_methods.empty:
        conservative = best_methods.sort_values(
            by=[
                "tail_resilience_score",
                "risk_control_score",
                "probability_of_loss",
                "cvar_95_pct_of_initial",
                "selection_score",
            ],
            ascending=[False, False, True, True, False],
        ).iloc[0]
        balanced = best_methods.sort_values(
            by=[
                "selection_score",
                "risk_adjusted_score",
                "diversification_quality_score",
                "tail_resilience_score",
            ],
            ascending=[False, False, False, False],
        ).iloc[0]
        aggressive = best_methods.sort_values(
            by=["return_score", "expected_portfolio_return", "risk_adjusted_score", "selection_score"],
            ascending=[False, False, False, False],
        ).iloc[0]

        profile_map = {
            "conservative": (
                conservative,
                "Strongest downside-resilience score after comparing basket-horizon winners.",
            ),
            "balanced": (
                balanced,
                "Best balanced score across return quality, risk control, tail risk, and diversification.",
            ),
            "aggressive": (
                aggressive,
                "Highest upside score after optimization within long-only horizon constraints.",
            ),
        }

        for profile_name, (row, reason) in profile_map.items():
            recommendations.append(
                {
                    "recommendation_scope": "investor_profile",
                    "recommendation_key": profile_name,
                    "investor_profile": profile_name,
                    "horizon_name": row["horizon_name"],
                    "basket_name": row["basket_name"],
                    "weighting_method": row["weighting_method"],
                    "basket_horizon_id": row["basket_horizon_id"],
                    "recommendation_rank": 1,
                    "selection_score": row["selection_score"],
                    "recommendation_reason": reason,
                    "key_risk": _key_risk(row),
                }
            )

    return pd.DataFrame(recommendations)


def build_metric_data_dictionary(run_id: str, ingestion_timestamp) -> pd.DataFrame:
    """Create a compact data dictionary for the main processed and analytics outputs."""
    records = [
        {
            "table_name": f"processed_market.{PROCESSED_TABLES['asset_returns']}",
            "column_name": "log_return",
            "data_type": "FLOAT",
            "definition": "Natural log of the price relative between consecutive observations.",
            "transformation_logic": "Calculated as LN(price_t / price_t-1) after sorting by ticker and date.",
            "business_use": "Core return input for optimization, correlation, and simulation.",
        },
        {
            "table_name": f"processed_market.{PROCESSED_TABLES['asset_returns']}",
            "column_name": "usd_adjusted_return",
            "data_type": "FLOAT",
            "definition": "Daily stock return adjusted for ARS versus USD movement.",
            "transformation_logic": "Calculated as stock log return minus the USDARS log return.",
            "business_use": "Helps frame local equity performance from a harder-currency perspective.",
        },
        {
            "table_name": f"processed_market.{PROCESSED_TABLES['asset_returns']}",
            "column_name": "excess_return",
            "data_type": "FLOAT",
            "definition": "Daily asset return net of the risk-free proxy.",
            "transformation_logic": "Calculated as log_return minus aligned risk_free_daily.",
            "business_use": "Supports Sharpe and other risk-adjusted metrics.",
        },
        {
            "table_name": f"processed_market.{PROCESSED_TABLES['stock_metrics']}",
            "column_name": "volatility",
            "data_type": "FLOAT",
            "definition": "Annualized standard deviation of daily log returns.",
            "transformation_logic": "Daily standard deviation multiplied by sqrt(252).",
            "business_use": "Main stand-alone risk metric at the stock level.",
        },
        {
            "table_name": f"processed_market.{PROCESSED_TABLES['stock_metrics']}",
            "column_name": "sharpe_ratio",
            "data_type": "FLOAT",
            "definition": "Risk-adjusted return relative to the risk-free proxy.",
            "transformation_logic": "Annualized excess return divided by annualized volatility.",
            "business_use": "Compares return quality across stocks.",
        },
        {
            "table_name": f"processed_market.{PROCESSED_TABLES['stock_metrics']}",
            "column_name": "sortino_ratio",
            "data_type": "FLOAT",
            "definition": "Downside-aware risk-adjusted return metric.",
            "transformation_logic": "Annualized excess return divided by annualized downside deviation.",
            "business_use": "Highlights how well a stock rewarded downside risk.",
        },
        {
            "table_name": f"processed_market.{PROCESSED_TABLES['stock_metrics']}",
            "column_name": "calmar_ratio",
            "data_type": "FLOAT",
            "definition": "Return scaled by the absolute maximum drawdown.",
            "transformation_logic": "Annualized return divided by absolute max drawdown.",
            "business_use": "Shows how efficiently return was generated versus drawdown pain.",
        },
        {
            "table_name": f"processed_market.{PROCESSED_TABLES['stock_metrics']}",
            "column_name": "downside_frequency",
            "data_type": "FLOAT",
            "definition": "Share of daily returns that finished below zero.",
            "transformation_logic": "Count of negative daily returns divided by total valid daily returns.",
            "business_use": "Adds a simple downside frequency view alongside volatility and drawdown.",
        },
        {
            "table_name": f"processed_market.{PROCESSED_TABLES['stock_metrics']}",
            "column_name": "stock_type",
            "data_type": "STRING",
            "definition": "Simple descriptive stock label used in presentation and interpretation.",
            "transformation_logic": "Assigned from rule-based thresholds on volatility, beta, and return quality.",
            "business_use": "Makes stock-level risk style easier to explain to an investor.",
        },
        {
            "table_name": f"processed_market.{PROCESSED_TABLES['beta_metrics']}",
            "column_name": "beta_vs_merval",
            "data_type": "FLOAT",
            "definition": "Sensitivity of stock return to MERVAL return.",
            "transformation_logic": "Estimated as covariance-over-variance against MERVAL daily returns.",
            "business_use": "Measures local market systematic risk.",
        },
        {
            "table_name": f"processed_market.{PROCESSED_TABLES['correlation_matrix_long']}",
            "column_name": "correlation",
            "data_type": "FLOAT",
            "definition": "Pairwise linear correlation between two stock return series.",
            "transformation_logic": "Calculated from aligned daily return pairs and stored in long form.",
            "business_use": "Used for diversification analysis and concentration diagnostics.",
        },
        {
            "table_name": f"analytics_market.{ANALYTICS_TABLES['basket_horizon_metrics']}",
            "column_name": "expected_portfolio_return",
            "data_type": "FLOAT",
            "definition": "Annualized expected return for one basket-horizon-method combination.",
            "transformation_logic": "Calculated from the estimation-window mean daily return and annualized.",
            "business_use": "Summarizes the upside expectation for each basket-horizon choice.",
        },
        {
            "table_name": f"analytics_market.{ANALYTICS_TABLES['basket_horizon_metrics']}",
            "column_name": "portfolio_volatility",
            "data_type": "FLOAT",
            "definition": "Annualized volatility of the weighted basket return series.",
            "transformation_logic": "Portfolio daily volatility multiplied by sqrt(252).",
            "business_use": "Main risk input for comparing methods and horizons.",
        },
        {
            "table_name": f"analytics_market.{ANALYTICS_TABLES['basket_horizon_metrics']}",
            "column_name": "weighted_sharpe",
            "data_type": "FLOAT",
            "definition": "Sharpe ratio for the basket-horizon allocation.",
            "transformation_logic": "Annualized expected return divided by annualized volatility.",
            "business_use": "Core risk-adjusted comparison metric across weighting methods.",
        },
        {
            "table_name": f"analytics_market.{ANALYTICS_TABLES['basket_horizon_metrics']}",
            "column_name": "diversification_effect",
            "data_type": "FLOAT",
            "definition": "Risk reduction from combining imperfectly correlated stocks.",
            "transformation_logic": "Weighted average asset volatility minus portfolio volatility.",
            "business_use": "Shows whether the basket is gaining meaningful diversification.",
        },
        {
            "table_name": f"analytics_market.{ANALYTICS_TABLES['basket_horizon_metrics']}",
            "column_name": "concentration_risk_hhi",
            "data_type": "FLOAT",
            "definition": "Herfindahl-Hirschman Index of the allocation weights.",
            "transformation_logic": "Calculated as the sum of squared portfolio weights.",
            "business_use": "Measures how concentrated the optimized basket remains.",
        },
        {
            "table_name": f"analytics_market.{ANALYTICS_TABLES['basket_horizon_metrics']}",
            "column_name": "risk_profile",
            "data_type": "STRING",
            "definition": "Heuristic label describing the final basket-horizon risk style.",
            "transformation_logic": "Assigned from rule-based thresholds on volatility, beta, probability of loss, and FX sensitivity.",
            "business_use": "Helps translate quantitative results into an investor-facing message.",
        },
        {
            "table_name": f"analytics_market.{ANALYTICS_TABLES['basket_horizon_contributions']}",
            "column_name": "contribution_to_return",
            "data_type": "FLOAT",
            "definition": "Annualized contribution of each stock to basket return.",
            "transformation_logic": "Portfolio weight multiplied by each stock's annualized return estimate.",
            "business_use": "Explains which holdings drive the upside of each optimized basket.",
        },
        {
            "table_name": f"analytics_market.{ANALYTICS_TABLES['basket_horizon_contributions']}",
            "column_name": "contribution_to_risk_pct",
            "data_type": "FLOAT",
            "definition": "Share of total portfolio risk coming from each stock.",
            "transformation_logic": "Derived from component risk contribution implied by the covariance matrix.",
            "business_use": "Shows where concentration and downside pressure are really coming from.",
        },
        {
            "table_name": f"analytics_market.{ANALYTICS_TABLES['monte_carlo_summary']}",
            "column_name": "probability_of_loss",
            "data_type": "FLOAT",
            "definition": "Share of simulations ending below the initial value.",
            "transformation_logic": "Count of final values below the initial value divided by total simulations.",
            "business_use": "Easy downside-risk measure for investor discussion.",
        },
        {
            "table_name": f"analytics_market.{ANALYTICS_TABLES['monte_carlo_summary']}",
            "column_name": "var_95",
            "data_type": "FLOAT",
            "definition": "95% Value at Risk measured from the 5th percentile of final values.",
            "transformation_logic": "Initial value minus the 5th percentile of simulated final values.",
            "business_use": "Summarizes a stressed-but-not-extreme downside threshold.",
        },
        {
            "table_name": f"analytics_market.{ANALYTICS_TABLES['monte_carlo_summary']}",
            "column_name": "cvar_95",
            "data_type": "FLOAT",
            "definition": "95% Conditional Value at Risk based on the worst simulated tail.",
            "transformation_logic": "Initial value minus the average of final values at or below the 5th percentile.",
            "business_use": "Captures expected loss severity once the downside tail is hit.",
        },
        {
            "table_name": f"analytics_market.{ANALYTICS_TABLES['basket_horizon_method_comparison']}",
            "column_name": "selection_score",
            "data_type": "FLOAT",
            "definition": "Balanced 0-100 score used to choose the best method for each basket-horizon combination.",
            "transformation_logic": "Weighted rank score within each horizon using return quality, risk control, tail resilience, and diversification components.",
            "business_use": "Primary method-selection metric for identifying the best strategy and weights in each basket-horizon cell.",
        },
        {
            "table_name": f"analytics_market.{ANALYTICS_TABLES['basket_horizon_method_comparison']}",
            "column_name": "tail_resilience_score",
            "data_type": "FLOAT",
            "definition": "0-1 rank score summarizing downside simulation resilience.",
            "transformation_logic": "Combines ranked probability of loss, CVaR as a share of initial value, and simulated tail drawdown.",
            "business_use": "Separates downside and tail-risk quality from pure return or Sharpe performance.",
        },
        {
            "table_name": f"analytics_market.{ANALYTICS_TABLES['investor_recommendation_summary']}",
            "column_name": "recommendation_reason",
            "data_type": "STRING",
            "definition": "Plain-English explanation for the selected basket-horizon recommendation.",
            "transformation_logic": "Generated from the comparison and risk-scoring rules.",
            "business_use": "Gives a report-ready interpretation of why a basket-horizon choice won.",
        },
    ]

    data_dictionary = pd.DataFrame(records)
    data_dictionary["run_id"] = run_id
    data_dictionary["ingestion_timestamp"] = ingestion_timestamp
    return data_dictionary
