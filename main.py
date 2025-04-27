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

