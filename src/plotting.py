#!/usr/bin/env python3
"""Gera gráficos a partir das correlações calculadas na execução local."""
from pathlib import Path
from typing import Optional, Sequence


def plot_correlations(
    results: Sequence[tuple[str, Optional[float]]],
    output: Path,
    source_name: str,
) -> None:
    """Salva um gráfico de barras horizontais com os coeficientes de Pearson."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    defined_results = sorted(
        ((feature, value) for feature, value in results if value is not None),
        key=lambda item: item[1],
    )

    figure_height = max(4.0, len(defined_results) * 0.32)
    figure, axis = plt.subplots(figsize=(10, figure_height))
    try:
        if defined_results:
            features = [feature for feature, _ in defined_results]
            values = [value for _, value in defined_results]
            colors = ["#2a9d8f" if value >= 0 else "#e76f51" for value in values]
            axis.barh(features, values, color=colors)
            axis.set_xlim(-1, 1)
            axis.axvline(0, color="black", linewidth=0.8)
            axis.set_xlabel("Correlação de Pearson")
            axis.set_ylabel("Variável")
        else:
            axis.text(
                0.5,
                0.5,
                "Nenhuma correlação definida para estas variáveis.",
                ha="center",
                va="center",
                transform=axis.transAxes,
            )
            axis.set_axis_off()

        axis.set_title(f"Correlação com a temperatura — {source_name}")
        figure.tight_layout()
        output.parent.mkdir(parents=True, exist_ok=True)
        figure.savefig(output, dpi=150, bbox_inches="tight")
    finally:
        plt.close(figure)
