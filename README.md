# Data-Vault

## Description
`data-vault` is a Python project that provides a collection of classes and functions to simplify fetching and managing financial market data from various sources: 
- It provides a high-level, object-oriented interface to retrieve chains, metadata, and historical OHLCV data for financial instruments from Bloomberg
- It fetches EU gas storage (ASGI) and LNG storage (ALSI) data from gie.eu


## Notable Features
- Easy-to-use interface for interacting with Bloomberg data
- Fetch historical futures chains and equity index members
- Retrieve comprehensive metadata including trading hours, delivery dates, and contract specifications
- Download full OHLCV history with automatic date chunking
- Built on Polars for fast, efficient dataframe handling

## Example

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
