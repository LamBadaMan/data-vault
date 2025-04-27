# Data-Vault

## Description
`data-vault` is a Python project that provides a collection of classes and functions to simplify fetching and managing financial market data from Bloomberg via `bbmirror client`. It provides a high-level, object-oriented interface to retrieve chains, metadata, and historical OHLCV data for financial instruments.

Currently, the projects supports Futures only — with full support for historical contract chains, metadata, and price history — but it is built with extensibility in mind to support other asset classes (Equities, FX, Bonds) in the near future.

## Features
- Easy-to-use interface for interacting with Bloomberg data
- Fetch historical futures chains
- Retrieve comprehensive metadata including trading hours, delivery dates, and contract specifications.
- Download full OHLCV history (Open, High, Low, Close, Volume, Open Interest) with automatic date chunking.
- Built on Polars for fast, efficient dataframe handling.


```python
from data_vault import FUTURES
from datetime import datetime

today = datetime.today().strftime("%Y%m%d")

active_contract = "COA Comdty"
coa = FUTURES(active_contract)

(coa
   .fetch_chains(start_date="19900101", end_date=today)
   .fetch_meta()
   .fetch_ohlc()
)

```
