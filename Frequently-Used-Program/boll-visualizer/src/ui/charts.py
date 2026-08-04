from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def build_bollinger_figure(
    df: pd.DataFrame,
    stock_code: str,
    stock_name: str,
    window: int,
    k: float,
    near_ratio: float = 1.015,
) -> go.Figure:
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        row_heights=[0.72, 0.28],
        vertical_spacing=0.06,
    )
    if df.empty:
        fig.update_layout(title="无可绘制数据")
        return fig

    x = pd.to_datetime(df["date"], errors="coerce")
    open_price = pd.to_numeric(df.get("open"), errors="coerce")
    high_price = pd.to_numeric(df.get("high"), errors="coerce")
    low_price = pd.to_numeric(df.get("low"), errors="coerce")
    close_price = pd.to_numeric(df.get("close"), errors="coerce")
    volume = pd.to_numeric(df.get("volume"), errors="coerce")
    ma = pd.to_numeric(df.get("MA"), errors="coerce")
    upper = pd.to_numeric(df.get("Upper"), errors="coerce")
    lower = pd.to_numeric(df.get("Lower"), errors="coerce")

    fig.add_trace(
        go.Candlestick(
            x=x,
            open=open_price,
            high=high_price,
            low=low_price,
            close=close_price,
            name="K线",
            increasing_line_color="#ef4444",
            decreasing_line_color="#10b981",
        ),
        row=1,
        col=1,
    )

    fig.add_trace(
        go.Scatter(
            x=x,
            y=ma,
            mode="lines",
            name=f"MA{window}",
            line={"width": 1.4, "color": "#1d4ed8"},
        ),
        row=1,
        col=1,
    )

    fig.add_trace(
        go.Scatter(
            x=x,
            y=upper,
            mode="lines",
            name=f"Upper (k={k})",
            line={"width": 1.0, "color": "#6366f1"},
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=x,
            y=lower,
            mode="lines",
            name=f"Lower (k={k})",
            line={"width": 1.0, "color": "#6366f1"},
            fill="tonexty",
            fillcolor="rgba(99, 110, 250, 0.12)",
        ),
        row=1,
        col=1,
    )

    if not close_price.empty and not lower.empty:
        oversold_mask = close_price < lower
        near_lower_mask = (~oversold_mask) & (close_price <= lower * float(near_ratio))
        if oversold_mask.any():
            fig.add_trace(
                go.Scatter(
                    x=x[oversold_mask],
                    y=close_price[oversold_mask],
                    mode="markers",
                    marker={"size": 8, "color": "#dc2626", "symbol": "triangle-down"},
                    name="低于下轨",
                ),
                row=1,
                col=1,
            )
        if near_lower_mask.any():
            fig.add_trace(
                go.Scatter(
                    x=x[near_lower_mask],
                    y=close_price[near_lower_mask],
                    mode="markers",
                    marker={"size": 7, "color": "#f59e0b", "symbol": "circle"},
                    name="接近下轨",
                ),
                row=1,
                col=1,
            )

    volume_colors = ["#ef4444" if c >= o else "#10b981" for o, c in zip(open_price, close_price)]
    fig.add_trace(
        go.Bar(
            x=x,
            y=volume,
            name="成交量",
            marker_color=volume_colors,
            opacity=0.8,
        ),
        row=2,
        col=1,
    )

    title = f"{stock_code} {stock_name}".strip()
    fig.update_layout(
        title=f"{title} 交易视图 (K线 + Boll + 成交量)",
        template="plotly_white",
        hovermode="x unified",
        legend={"orientation": "h", "y": 1.02, "x": 0.01},
        margin={"l": 20, "r": 20, "t": 60, "b": 20},
        xaxis_rangeslider_visible=False,
    )
    fig.update_yaxes(title_text="价格", row=1, col=1)
    fig.update_yaxes(title_text="成交量", row=2, col=1)
    fig.update_xaxes(title_text="日期", row=2, col=1)
    return fig
