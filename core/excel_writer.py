# core/excel_writer.py
import pandas as pd
from pathlib import Path
from datetime import datetime

def append_rows_to_excel(path, route_code, rows):
    # rows: list of dicts
    df = pd.DataFrame(rows)
    file = Path(path)
    if file.exists():
        # open workbook and append as a new sheet named with route+timestamp
        with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
            sheet = f"{route_code}_{datetime.utcnow().strftime('%Y%m%d_%H%M')}"
            df.to_excel(writer, sheet_name=sheet, index=False)
    else:
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            sheet = f"{route_code}_{datetime.utcnow().strftime('%Y%m%d_%H%M')}"
            df.to_excel(writer, sheet_name=sheet, index=False)
