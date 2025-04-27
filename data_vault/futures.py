import polars as pl
from bbmirror_client import bdp, bdh, bds
from datetime import datetime, timedelta
from tqdm.auto import tqdm
from typing import Optional


def is_valid_yyyymmdd(date: str) -> bool:
    """
    Checks whether a date string is in the 'YYYYMMDD' format.
    """
    try:
        datetime.strptime(date, "%Y%m%d")
        return True
    except ValueError:
        return False


class FUTURES:
    def __init__(self, active_contract: str):
        self.active_contract = active_contract
        self._chains: Optional[pl.DataFrame] = None
        self._symbols: Optional[list[str]] = None
        self._meta: Optional[pl.DataFrame] = None
        self._ohlc: Optional[pl.DataFrame] = None

    def __get_or_raise(self, attr_name: str, fetch_method_name: str):
        value = getattr(self, attr_name)
        if value is None:
            raise AttributeError(f"{attr_name.lstrip('_')} not available yet. Please run `{fetch_method_name}()` first.")
        return value

    @property
    def chains(self) -> pl.DataFrame:
        return self.__get_or_raise("_chains", "fetch_chains")

    @property
    def symbols(self) -> list[str]:
        return self.__get_or_raise("_symbols", "fetch_chains")

    @property
    def meta(self) -> pl.DataFrame:
        return self.__get_or_raise("_meta", "fetch_meta")

    @property
    def ohlc(self) -> pl.DataFrame:
        return self.__get_or_raise("_ohlc", "fetch_ohlc")

    def __fetch_chain(self, as_of: str) -> pl.DataFrame:
        if not is_valid_yyyymmdd(as_of):
            raise ValueError("Date must be in 'YYYYMMDD' format (e.g., '20250410')")

        payload = {
            "tickers": [self.active_contract],
            "fields": ["FUT_CHAIN"],
            "options": {},
            "overrides": [("CHAIN_DATE", as_of)],
        }

        response = bds(payload)
        return pl.DataFrame(response).rename({"Security Description": "symbol"}).with_columns(
            [pl.lit(as_of).str.strptime(pl.Date, "%Y%m%d").alias("as_of")]
        )

    def fetch_chains(self, start_date: str, end_date: str, progressbar: bool = True) -> "FUTURES":
        for date in [start_date, end_date]:
            if not is_valid_yyyymmdd(date):
                raise ValueError("Date must be in 'YYYYMMDD' format (e.g., '20250410')")

        start_dt = datetime.strptime(start_date, "%Y%m%d").replace(day=1)
        end_dt = datetime.strptime(end_date, "%Y%m%d").replace(day=1)

        dates = (
            pl.date_range(
                start=start_dt,
                end=end_dt,
                interval="1mo",
                eager=True,
            )
            .dt.strftime("%Y%m%d")
            .to_list()
        )

        iterator = tqdm(dates, desc="Fetching chains") if progressbar else dates
        chains = [self.__fetch_chain(as_of=date) for date in iterator]
        chains = pl.concat(chains, how="vertical", rechunk=True).sort(["as_of", "symbol"])[["as_of", "symbol"]]

        self._chains = chains
        self._symbols = chains["symbol"].unique().to_list()
        return self

    def fetch_meta(self) -> "FUTURES":
        if self._chains is None or self._symbols is None:
            raise AttributeError("Historical future chains must be fetched. Please run `fetch_chains()` first.")

        fields = [
            "ID_BB_GLOBAL_ULT_PARENT_CO_NAME",
            "FUT_EXCH_NAME_LONG",
            "DERIVATIVE_DELIVERY_TYPE",
            "QUOTE_UNITS",
            "QUOTED_CRNCY",
            "FUT_TRADING_UNITS",
            "FUT_FIRST_TRADE_DT",
            "LAST_TRADEABLE_DT",
            "FUT_NOTICE_FIRST",
            "FUT_DLV_DT_FIRST",
            "FUT_DLV_DT_LAST",
            "FUT_CONTRACT_DT",
            "FUT_CONT_SIZE",
            "FUT_TRADING_HRS",
            "FUT_INIT_SPEC_ML",
            "FUT_SEC_SPEC_ML",
            "FUT_INIT_HEDGE_ML",
            "FUT_SEC_HEDGE_ML",
        ]

        payload = {
            "tickers": self._symbols,
            "fields": fields,
            "options": {},
            "overrides": [],
        }

        response = bdp(payload)
        df = (
            pl.DataFrame(response)
            .rename({col: col.lower() for col in pl.DataFrame(response).columns})
            .rename({"security": "symbol"})
        )

        cols2date = [
            "fut_first_trade_dt",
            "last_tradeable_dt",
            "fut_notice_first",
            "fut_dlv_dt_first",
            "fut_dlv_dt_last",
        ]

        df = (
            df.with_columns(
                [pl.col(col).str.strptime(pl.Date, format="%Y-%m-%dT%H:%M:%S%.3f") for col in cols2date]
            )
            .with_columns(
                pl.col("fut_trading_hrs").str.split("-").alias("split_range"),
            )
            .with_columns(
                [
                    pl.col("split_range").list.get(0).str.strptime(pl.Time, format="%H:%M").alias("trading_hrs_start"),
                    pl.col("split_range").list.get(1).str.strptime(pl.Time, format="%H:%M").alias("trading_hrs_end"),
                ]
            )
            .drop(["fut_trading_hrs", "split_range"])
        ).sort(["fut_first_trade_dt", "symbol"])

        self._meta = df
        return self

    def fetch_ohlc(self, progressbar: bool = True) -> "FUTURES":
        if self._symbols is None:
            raise AttributeError("Historical future chains must be fetched. Please run `fetch_chains()` first.")

        start = datetime.strptime("19900101", "%Y%m%d")
        today = datetime.today()

        # Build 10-year chunks without overlapping
        chunks = []
        current_start = start

        while current_start <= today:
            try:
                current_end = current_start.replace(year=current_start.year + 10)
            except ValueError:
                # Handle leap years (e.g., Feb 29)
                current_end = current_start.replace(year=current_start.year + 10, day=28)

            if current_end > today:
                current_end = today

            chunks.append((current_start, current_end))
            current_start = current_end + timedelta(days=1)  # avoid overlaps by moving to the next day

        all_data = []

        iterator = tqdm(chunks, desc="Fetching OHLC") if progressbar else chunks

        for chunk_start, chunk_end in iterator:
            payload = {
                "tickers": self._symbols,
                "fields": [
                    "PX_OPEN",
                    "PX_HIGH",
                    "PX_LOW",
                    "PX_LAST",
                    "PX_VOLUME",
                    "OPEN_INT",
                ],
                "start_date": chunk_start.strftime("%Y%m%d"),
                "end_date": chunk_end.strftime("%Y%m%d"),
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
                "OPEN_INT": pl.Float64,
            }

            df = pl.DataFrame(response, schema=schema)
            df = df.rename({col: col.lower() for col in df.columns}).rename({"security": "symbol"}).with_columns(
                pl.col("date").str.strptime(pl.Date, "%Y-%m-%dT%H:%M:%S%.3f").alias("date")
            )

            all_data.append(df)

        self._ohlc =pl.concat(all_data, how="vertical", rechunk=True).sort(["symbol", "date"])

        first_cols = ["symbol", "date"]
        self._ohlc = self._ohlc.select(first_cols + [col for col in self._ohlc.columns if col not in first_cols])

        return self