import polars as pl
from fastdata_client import bdp, bdh, bds
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


class STOCKS:
    def __init__(self, index: str):
        self.index = index
        self._members: Optional[pl.DataFrame] = None
        self._symbols: Optional[list[str]] = None
        self._meta: Optional[pl.DataFrame] = None
        self._divs: Optional[pl.DataFrame] = None
        self._ohlc: Optional[pl.DataFrame] = None
        self._fundamentals: Optional[pl.DataFrame] = None

    def __get_or_raise(self, attr_name: str, fetch_method_name: str):
        value = getattr(self, attr_name)
        if value is None:
            raise AttributeError(f"{attr_name.lstrip('_')} not available yet. Please run `{fetch_method_name}()` first.")
        return value

    @property
    def members(self) -> pl.DataFrame:
        return self.__get_or_raise("_chains", "fetch_members")

    @property
    def symbols(self) -> list[str]:
        return self.__get_or_raise("_symbols", "fetch_symbols")

    @property
    def meta(self) -> pl.DataFrame:
        return self.__get_or_raise("_meta", "fetch_meta")

    @property
    def divs(self) -> pl.DataFrame:
        return self.__get_or_raise("_divs", "fetch_divs")

    @property
    def ohlc(self) -> pl.DataFrame:
        return self.__get_or_raise("_ohlc", "fetch_ohlc")
    
    @property
    def fundamentals(self) -> pl.DataFrame:
        return self.__get_or_raise("_ohlc", "fetch_fundamentals")

    def __fetch_constituents(self, as_of: str) -> pl.DataFrame:
        if not is_valid_yyyymmdd(as_of):
            raise ValueError("Date must be in 'YYYYMMDD' format (e.g., '20250410')")

        payload = {
            "tickers": [self.index],
            "fields": ["INDX_MWEIGHT_HIST"],
            "options": {},
            "overrides": [("END_DATE_OVERRIDE", as_of)],
        }

        response = bds(payload)
        return pl.DataFrame(response).rename({"Security Description": "symbol"}).with_columns(
            [pl.lit(as_of).str.strptime(pl.Date, "%Y%m%d").alias("as_of")]
        )

    def fetch_members(self, start_date: str, end_date: str, progressbar: bool = True) -> "STOCKS":
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

        iterator = tqdm(dates, desc="Fetching constituents") if progressbar else dates
        members = [self.__fetch_constituents(as_of=date) for date in iterator]
        members = pl.concat(members, how="vertical", rechunk=True).sort(["as_of", "symbol"])[["as_of", "symbol"]]

        self._members = members
        self._symbols = members["symbol"].unique().to_list()
        return self

    def fetch_meta(self) -> "STOCKS":
        if self._members is None or self._symbols is None:
            raise AttributeError("Historical index members must be fetched. Please run `fetch_members()` first.")

        fields = [
            "LONG_COMP_NAME",
            "GICS_SECTOR_NAME",
            "GICS_INDUSTRY_NAME",
            "GICS_INDUSTRY_GROUP_NAME",
            "GICS_SUB_INDUSTRY_NAME",
            "COUNTRY_ISO",
            "PRIMARY_EXCHANGE_NAME",
            "EXCH_CODE",
            "ID_MIC_PRIM_EXCH",
            "TRADING_DAY_START_TIME_EOD",
            "TRADING_DAY_END_TIME_EOD",
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
            "trading_day_start_time_eod",
            "trading_day_end_time_eod",
        ]

        df = (
            df.with_columns(
                [pl.col(col).str.strptime(pl.Date, format="%Y-%m-%dT%H:%M:%S%.3f") for col in cols2date]
            )
        ).sort(["symbol"])

        self._meta = df
        return self
    
    def fetch_divs(self) -> "STOCKS":
        if self._members is None or self._symbols is None:
            raise AttributeError("Historical index members must be fetched. Please run `fetch_members()` first.")

        fields = [
            "DVD_HIST_ALL",
        ]

        payload = {
            "tickers": self._symbols,
            "fields": fields,
            "options": {},
            "overrides": [],
        }

        response = bds(payload)
        df = (
            pl.DataFrame(response)
            .rename({col: col.lower() for col in pl.DataFrame(response).columns})
            .rename({"security": "symbol"})
        )

        cols2date = [
            "dvd_hist_all",
        ]

        df = (
            df.with_columns(
                [pl.col(col).str.strptime(pl.Date, format="%Y-%m-%dT%H:%M:%S%.3f") for col in cols2date]
            )
        ).sort(["symbol"])

        self._divs = df
        return self


    def fetch_ohlc(self, progressbar: bool = True) -> "STOCKS":
        if self._symbols is None:
            raise AttributeError("Historical index members must be fetched. Please run `fetch_members()` first.")

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
                    "CUR_MKT_CAP"
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
                "CUR_MKT_CAP": pl.Float64,
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