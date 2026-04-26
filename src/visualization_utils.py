from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.config import BASKETS, CHART_OUTPUT_DIR, HORIZONS


def ensure_chart_output_dir(output_dir: Path | None = None) -> Path:
    """Create the chart directory once and reuse it everywhere."""
    target_dir = output_dir or CHART_OUTPUT_DIR
    target_dir.mkdir(parents=True, exist_ok=True)
    return target_dir


def save_overview_heatmap(comparison_df: pd.DataFrame, output_dir: Path | None = None) -> Path:
    """Create one 3x3 summary heatmap for the presentation overview."""
    target_dir = ensure_chart_output_dir(output_dir)
    winners = comparison_df[comparison_df["is_best_method_for_basket_horizon"]].copy()

    horizon_order = list(HORIZONS.keys())
    basket_order = list(BASKETS.keys())
    score_matrix = np.full((len(horizon_order), len(basket_order)), np.nan)

    fig, ax = plt.subplots(figsize=(13, 8))
    for i, horizon_name in enumerate(horizon_order):
        for j, basket_name in enumerate(basket_order):
            row = winners[
                (winners["horizon_name"] == horizon_name) & (winners["basket_name"] == basket_name)
            ]
            if row.empty:
                continue
            value = float(row["selection_score"].iloc[0])
            score_matrix[i, j] = value

    image = ax.imshow(score_matrix, cmap="YlGn", aspect="auto")
    ax.set_xticks(range(len(basket_order)))
    ax.set_xticklabels([BASKETS[name]["label"] for name in basket_order], rotation=20, ha="right")
    ax.set_yticks(range(len(horizon_order)))
    ax.set_yticklabels([HORIZONS[name]["label"] for name in horizon_order])
    ax.set_title("Winning Method by Basket and Horizon")

    for i, horizon_name in enumerate(horizon_order):
        for j, basket_name in enumerate(basket_order):
            row = winners[
                (winners["horizon_name"] == horizon_name) & (winners["basket_name"] == basket_name)
            ]
            if row.empty:
                continue
            winner = row.iloc[0]
            annotation = (
                f"{winner['weighting_method']}\n"
                f"Score {winner['selection_score']:.2f}\n"
                f"Loss {winner['probability_of_loss']:.1%}"
            )
            ax.text(j, i, annotation, ha="center", va="center", fontsize=9)

    fig.colorbar(image, ax=ax, shrink=0.8, label="Selection Score")
    output_path = target_dir / "overview_basket_horizon_heatmap.png"
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
    return output_path


def save_investor_profile_overview(
    recommendations_df: pd.DataFrame,
    comparison_df: pd.DataFrame,
    output_dir: Path | None = None,
) -> Path:
    """Create one presentation chart that maps investor profiles to the best combinations."""
    target_dir = ensure_chart_output_dir(output_dir)
    profile_rows = recommendations_df[
        recommendations_df["recommendation_scope"] == "investor_profile"
    ].copy()
    merged = profile_rows.merge(
        comparison_df[
            [
                "basket_horizon_id",
                "basket_label",
                "horizon_label",
                "selection_score",
                "probability_of_loss",
                "weighted_sharpe",
            ]
        ],
        on="basket_horizon_id",
        how="left",
    )
    profile_order = ["conservative", "balanced", "aggressive"]
    merged["profile_order"] = merged["investor_profile"].apply(
        lambda value: profile_order.index(value) if value in profile_order else len(profile_order)
    )
    merged = merged.sort_values("profile_order").drop(columns="profile_order")

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.axis("off")
    table_rows = []
    for _, row in merged.iterrows():
        table_rows.append(
            [
                row["investor_profile"],
                row["basket_label"],
                row["horizon_label"],
                row["weighting_method"],
                f"{row['weighted_sharpe']:.2f}",
                f"{row['probability_of_loss']:.1%}",
            ]
        )

    table = ax.table(
        cellText=table_rows,
        colLabels=["Profile", "Basket", "Horizon", "Method", "Sharpe", "Prob. Loss"],
        loc="center",
        cellLoc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2, 1.8)
    ax.set_title("Investor Profile Recommendations", pad=20)
    output_path = target_dir / "overview_investor_profile_recommendations.png"
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return output_path


def save_basket_horizon_fact_sheet(
    weights_df: pd.DataFrame,
    contributions_df: pd.DataFrame,
    metrics_row: pd.Series,
    simulation_row: pd.Series,
    final_values: np.ndarray,
    output_dir: Path | None = None,
) -> Path:
    """Create one winner-first fact sheet for a single basket-horizon cell."""
    target_dir = ensure_chart_output_dir(output_dir)
    basket_name = metrics_row["basket_name"]
    horizon_name = metrics_row["horizon_name"]

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    weights_plot = weights_df.sort_values("weight", ascending=False)
    contrib_return_plot = contributions_df.sort_values("contribution_to_return", ascending=False)
    contrib_risk_plot = contributions_df.sort_values("contribution_to_risk_pct", ascending=False)

    axes[0, 0].bar(weights_plot["ticker"], weights_plot["weight"], color="#1f77b4")
    axes[0, 0].set_title("Winning Weights")
    axes[0, 0].tick_params(axis="x", rotation=45)
    axes[0, 0].set_ylabel("Weight")

    axes[0, 1].bar(
        contrib_return_plot["ticker"],
        contrib_return_plot["contribution_to_return"],
        color="#2ca02c",
    )
    axes[0, 1].set_title("Contribution to Return")
    axes[0, 1].tick_params(axis="x", rotation=45)
    axes[0, 1].set_ylabel("Annualized Contribution")

    axes[1, 0].bar(
        contrib_risk_plot["ticker"],
        contrib_risk_plot["contribution_to_risk_pct"],
        color="#ff7f0e",
    )
    axes[1, 0].set_title("Contribution to Risk")
    axes[1, 0].tick_params(axis="x", rotation=45)
    axes[1, 0].set_ylabel("Risk Share")

    axes[1, 1].hist(final_values, bins=40, alpha=0.8, color="#9467bd")
    axes[1, 1].axvline(
        simulation_row["initial_value"],
        color="red",
        linestyle="--",
        linewidth=2,
        label="Initial Value",
    )
    axes[1, 1].axvline(
        np.quantile(final_values, 0.05),
        color="orange",
        linestyle="--",
        linewidth=2,
        label="5th Percentile",
    )
    axes[1, 1].set_title("Monte Carlo Final Value Distribution")
    axes[1, 1].set_xlabel("Final Value")
    axes[1, 1].set_ylabel("Frequency")
    axes[1, 1].legend()
    summary_text = (
        f"Mean {simulation_row['mean_final_value']:.2f}\n"
        f"Median {simulation_row['median_final_value']:.2f}\n"
        f"Prob. Loss {simulation_row['probability_of_loss']:.1%}\n"
        f"VaR95 {simulation_row['var_95']:.2f}\n"
        f"CVaR95 {simulation_row['cvar_95']:.2f}"
    )
    axes[1, 1].text(
        0.98,
        0.98,
        summary_text,
        transform=axes[1, 1].transAxes,
        ha="right",
        va="top",
        fontsize=9,
        bbox={"facecolor": "white", "alpha": 0.85, "edgecolor": "#cccccc"},
    )

    headline = (
        f"{metrics_row['basket_label']} | {metrics_row['horizon_label']} | "
        f"Winner: {metrics_row['weighting_method']}\n"
        f"Sharpe {metrics_row['weighted_sharpe']:.2f} | "
        f"Prob. Loss {simulation_row['probability_of_loss']:.1%} | "
        f"VaR95 {simulation_row['var_95']:.2f} | CVaR95 {simulation_row['cvar_95']:.2f}"
    )
    fig.suptitle(headline, fontsize=13)
    fig.tight_layout(rect=[0, 0, 1, 0.94])

    output_path = target_dir / f"{basket_name}__{horizon_name}__fact_sheet.png"
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
    return output_path
