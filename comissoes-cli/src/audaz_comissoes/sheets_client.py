import os
import json
import base64
from functools import lru_cache

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


def _safe_headers(headers):
    safe = []
    used = {}
    for i, h in enumerate(headers):
        base = (str(h).strip() if h is not None and str(h).strip() != "" else f"COLUNA {i+1}")
        n = used.get(base, 0)
        name = base if n == 0 else f"{base}__{n+1}"
        used[base] = n + 1
        safe.append(name)
    return safe


def _get_records_tolerant(ws, header_row: int = 1):
    values = ws.get_all_values()
    if not values:
        return []
    headers = _safe_headers(values[header_row - 1])

    records = []
    for row in values[header_row:]:
        row = (row + [""] * len(headers))[:len(headers)]
        if all(not str(x).strip() for x in row):
            continue
        records.append(dict(zip(headers, row)))
    return records


def _records_to_df(records):
    df = pd.DataFrame(records)
    df.columns = pd.Index(map(str, df.columns)).str.strip()
    return df


@lru_cache(maxsize=1)
def _gc() -> gspread.Client:
    """
    Ordem:
      1) GSHEETS_CREDENTIALS_JSON_CONTENT (json puro ou base64)
      2) GSHEETS_CREDENTIALS_B64 (base64)
      3) GSHEETS_CREDENTIALS_JSON (caminho local)
    """
    content = os.getenv("GSHEETS_CREDENTIALS_JSON_CONTENT")
    if content:
        try:
            creds_dict = json.loads(content)
        except json.JSONDecodeError:
            decoded = base64.b64decode(content).decode("utf-8")
            creds_dict = json.loads(decoded)
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        return gspread.authorize(creds)

    b64 = os.getenv("GSHEETS_CREDENTIALS_B64")
    if b64:
        decoded = base64.b64decode(b64).decode("utf-8")
        creds_dict = json.loads(decoded)
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        return gspread.authorize(creds)

    path = os.getenv("GSHEETS_CREDENTIALS_JSON")
    if path and os.path.exists(path):
        creds = Credentials.from_service_account_file(path, scopes=SCOPES)
        return gspread.authorize(creds)

    raise RuntimeError(
        "Credenciais não encontradas. Defina GSHEETS_CREDENTIALS_JSON_CONTENT ou GSHEETS_CREDENTIALS_JSON."
    )


def _spreadsheet_key(env_name: str) -> str:
    k = (os.getenv(env_name) or "").strip()
    if not k:
        raise RuntimeError(f"Defina {env_name}. Ex.: GSHEETS_SPREADSHEET_KEY ou GSHEETS_PARAM_KEY.")
    return k


def read_sheet_by_gid(spreadsheet_key: str, gid: int | str) -> pd.DataFrame:
    gc = _gc()
    sh = gc.open_by_key(spreadsheet_key)
    gid_int = int(str(gid))

    alvo = None
    for ws in sh.worksheets():
        try:
            if int(getattr(ws, "id", -1)) == gid_int:
                alvo = ws
                break
        except Exception:
            pass

    if alvo is None:
        raise RuntimeError(f"GID {gid} não encontrado na planilha {spreadsheet_key}.")

    records = _get_records_tolerant(alvo)
    return _records_to_df(records)
