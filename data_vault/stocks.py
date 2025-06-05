import polars as pl
from fastdata_client import bdp, bdh, bds
from datetime import datetime, timedelta
from tqdm.auto import tqdm
from typing import Optional


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
        return self.__get_or_raise("_members", "fetch_members")

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
        return self.__get_or_raise("_fundamentals", "fetch_fundamentals")

    def __fetch_constituents(self, as_of: str) -> pl.DataFrame:

        payload = {
            "tickers": [self.index],
            "fields": ["INDX_MWEIGHT_HIST"],
            "options": {},
            "overrides": [("END_DATE_OVERRIDE", as_of)],
        }

        response = bds(payload)
        return pl.DataFrame(response).rename({"Index Member": "symbol"}).with_columns(
            [
                pl.lit(as_of).str.strptime(pl.Date, "%Y%m%d").alias("as_of"),
                (pl.col("symbol") + " Equity").alias("symbol"),
             
            ]
        ).drop(["Percent Weight"])

    def fetch_members(self, start_date: datetime, end_date: datetime, progressbar: bool = True) -> "STOCKS":

        start_dt = start_date.replace(day=1)
        end_dt = end_date.replace(day=1)

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
            "INDUSTRY_SECTOR",
            "INDUSTRY_GROUP",
            "INDUSTRY_SUBGROUP",
            "GICS_SECTOR_NAME",
            "GICS_INDUSTRY_NAME",
            "GICS_INDUSTRY_GROUP_NAME",
            "GICS_SUB_INDUSTRY_NAME",
            "COUNTRY_ISO",
            "CRNCY",
            "DVD_CRNCY",
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
                [pl.col(col).str.strptime(pl.Time, format="%H:%M:%S") for col in cols2date]
            )
        ).sort(["symbol"])

        self._meta = df
        return self
    
    def __fetch_div(self, symbol) -> pl.DataFrame:

        fields = [
            "DVD_HIST_ALL",
        ]

        payload = {
            "tickers": [symbol],
            "fields": fields,
            "options": {},
            "overrides": [],
        }

        response = bds(payload)
        cols2date = ["declared_date","ex_date","record_date","payable_date"]

        schema = {
            "Declared Date": pl.String,
            "Ex-Date": pl.String,
            "Record Date": pl.String,
            "Payable Date": pl.String,
            "Dividend Amount": pl.Float64,
            "Dividend Frequency": pl.String,
            "Dividend Type": pl.String,
        }

        try:
            df = (
                pl.DataFrame(response, schema=schema)
                .rename({col: col.lower().replace(" ","_").replace("-","_") for col in pl.DataFrame(response).columns})
                .with_columns([pl.lit(symbol).alias("symbol")])
                .with_columns(
                                [pl.col(col).str.strptime(pl.Date, format="%Y-%m-%dT%H:%M:%S%.3f") for col in cols2date]
                            )
            )

            return df
        
        except:
            return None

    def fetch_divs(self, progressbar: bool = True) -> "STOCKS":
        if self._members is None or self._symbols is None:
            raise AttributeError("Historical index members must be fetched. Please run `fetch_members()` first.")

        iterator = tqdm(self._symbols, desc="Fetching historical dividend and split data") if progressbar else self._symbols
        divs = [self.__fetch_div(symbol) for symbol in iterator]
        divs = [x for x in divs if x is not None]
        divs = pl.concat(divs, how="vertical", rechunk=True).sort(["symbol","declared_date"])

        first_cols = ["symbol"]
        divs = divs.select(first_cols + [col for col in divs.columns if col not in first_cols])

        self._divs = divs
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
                    "CUR_MKT_CAP",
                    "DAY_TO_DAY_TOT_RETURN_GROSS_DVDS"
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
                "DAY_TO_DAY_TOT_RETURN_GROSS_DVDS": pl.Float64,
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