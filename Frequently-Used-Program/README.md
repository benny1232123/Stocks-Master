# Frequently-Used-Program Script Groups

This folder contains many Python scripts. Use this quick grouping to find the right one faster.

## 1) BOLL main flow (daily use)

- `auto_notify_boll.py`: Main daily pipeline (selection + notification)
- `Stock-Selection-Boll.py`: Core BOLL selection logic
- `Stock-Selection-Boll-All.py`: Batch/all-stock BOLL selection

## 2) CCTV/news/theme strategies (optional)

- `Stock-Selection-CCTV-Sectors.py`: CCTV sector strategy
- `Stock-Selection-News.py`: News-based strategy
- `Stock-Selection-Ashare-Theme-Turnover.py`: Theme turnover strategy
- `Stock-Selection-Relativity.py`: Relativity strategy

## 3) Analysis

- `Stock-Analysis.py`: Stock analysis utility

## 4) Maintenance and supporting tools

- `cleanup_stock_data.py`: Cleanup for stock_data files
- `test_cctv_sectors_strategy.py`: Test script for CCTV strategy
- `boll-visualizer/`: Optional Streamlit visual UI
- `update`: Update helper script

## Suggested daily path

1. Use root `stocks-master.bat` as the unified launcher.
2. Only open scripts in group 1 unless you need optional strategies.
3. Run cleanup regularly to keep `stock_data/` small.
