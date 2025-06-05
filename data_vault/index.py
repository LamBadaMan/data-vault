import polars as pl
from fastdata_client import bdp, bdh, bds, bdip
from datetime import datetime, timedelta
from tqdm.auto import tqdm
from typing import Optional

class INDEX:
    def __init__(self, index: str):
        self.index = index
        self._ohlc_daily: Optional[pl.DataFrame] = None
        self._ohlc_1min: Optional[pl.DataFrame] = None

    def __get_or_raise(self, attr_name: str, fetch_method_name: str):
        value = getattr(self, attr_name)
        if value is None:
            raise AttributeError(f"{attr_name.lstrip('_')} not available yet. Please run `{fetch_method_name}()` first.")
        return value

    @property
    def ohlc_daily(self) -> pl.DataFrame:
        return self.__get_or_raise("_ohlc_daily", "fetch_ohlc_daily")
    
    @property
    def ohlc_1min(self) -> pl.DataFrame:
        return self.__get_or_raise("_ohlc_1min", "fetch_ohlc_1min")

    def fetch_ohlc_daily(self, start:datetime, end:datetime) -> "INDEX":

        payload = {
            "tickers": [self.index],
            "fields": [
                "PX_OPEN",
                "PX_HIGH",
                "PX_LOW",
                "PX_LAST",
                "PX_VOLUME",
            ],
            "start_date": start.strftime("%Y%m%d"),
            "end_date": end.strftime("%Y%m%d"),
            "options": {},
            "overrides": [],
        }

        response = bdh(payload)

        schema = {
            "security": pl.Utf8,
            "date": pl.String,
            "PX_OPEN": pl.Float64,
            "PX_HIGH": pl.Float64,
            "PX_LOW": pl.Float64,
            "PX_LAST": pl.Float64,
            "PX_VOLUME": pl.Float64,
        }

        df = pl.DataFrame(response, schema=schema)
        df = df.rename({col: col.lower() for col in df.columns}).rename({"security": "symbol"}).with_columns(
            pl.col("date").str.strptime(pl.Date, "%Y-%m-%dT%H:%M:%S%.3f").alias("date")
        )

        self._ohlc_daily = df.sort(["symbol", "date"])

        first_cols = ["symbol", "date"]
        self._ohlc_daily = self._ohlc_daily.select(first_cols + [col for col in self._ohlc_daily.columns if col not in first_cols])

        return self
    
    def fetch_ohlc_1min(self, start:datetime, end:datetime, interval:int=1) -> "INDEX":

        payload = {
            "ticker": self.index,
            "event_type": "TRADE",
            "interval": interval,
            "start_datetime": start.strftime("%Y-%m-%d"),
            "end_datetime": end.strftime("%Y-%m-%d"),
        }

        response = bdip(payload)

        schema = {
            "time": pl.String,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Float64,
            "numEvents": pl.Float64,
            "value": pl.Float64
        }

        df = pl.DataFrame(response, schema=schema)
        df = df.rename({col: col.lower() for col in df.columns}).rename({"time": "datetime","open":"px_open","high":"px_high","low":"px_low","close":"px_close","volume":"px_volume"}).with_columns([
            pl.col("datetime").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.3f").dt.replace_time_zone("UTC").alias("datetime"),
            pl.lit(self.index).alias("symbol")
        ])

        self._ohlc_1min = df.sort(["symbol", "datetime"])

        first_cols = ["symbol", "datetime"]
        self._ohlc_1min = self._ohlc_1min.select(first_cols + [col for col in self._ohlc_1min.columns if col not in first_cols])

        return self