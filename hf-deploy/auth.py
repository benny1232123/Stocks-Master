"""Stocks-Master 共享认证模块。

所有页面通过 check_auth() 实现密码保护。
密码通过 Hugging Face Spaces Secrets 配置（变量名 APP_PASSWORD），
未配置时默认使用 "stockmaster2024"。
"""
from __future__ import annotations

import streamlit as st


def check_auth() -> None:
    """门控：未认证则显示登录界面并 stop。"""
    if st.session_state.get("authenticated", False):
        return

    st.set_page_config(
        page_title="Stocks-Master",
        page_icon="📊",
        layout="centered",
    )

    st.title("🔐 Stocks-Master")
    st.caption("A股智能选股与持仓管理系统")

    # 简单密码门
    pwd = st.text_input(
        "访问密码",
        type="password",
        placeholder="请输入密码",
    )
    if st.button("进入系统", type="primary", use_container_width=True):
        correct = st.secrets.get("APP_PASSWORD", "stockmaster2024")
        if pwd == correct:
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("密码错误，请重试")

    st.stop()
