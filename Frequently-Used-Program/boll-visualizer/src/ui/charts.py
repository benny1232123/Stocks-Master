from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go


def build_bollinger_figure(
    df: pd.DataFrame,
    stock_code: str,
    stock_name: str,
    window: int,
    k: float,
) -> go.Figure:
    fig = go.Figure()
    if df.empty:
        fig.update_layout(title="无可绘制数据")
        return fig

    x = pd.to_datetime(df["date"], errors="coerce")

    fig.add_trace(go.Scatter(x=x, y=df["close"], mode="lines", name="Close", line={"width": 1.8}))
    fig.add_trace(go.Scatter(x=x, y=df["MA"], mode="lines", name=f"MA{window}", line={"width": 1.4}))

    fig.add_trace(
        go.Scatter(
            x=x,
            y=df["Upper"],
            mode="lines",
            name=f"Upper (k={k})",
            line={"width": 1.0},
        )
    )
    fig.add_trace(
        go.Scatter(
            x=x,
            y=df["Lower"],
            mode="lines",
            name=f"Lower (k={k})",
            line={"width": 1.0},
            fill="tonexty",
            fillcolor="rgba(99, 110, 250, 0.12)",
        )
    )

    title = f"{stock_code} {stock_name}".strip()
    fig.update_layout(
        title=f"{title} 布林带 (MA{window}, k={k})",
        xaxis_title="日期",
        yaxis_title="价格",
        hovermode="x unified",
        legend={"orientation": "h"},
        margin={"l": 20, "r": 20, "t": 60, "b": 20},
    )
    return fig
