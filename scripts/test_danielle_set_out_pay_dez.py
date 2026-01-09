import os
import re
from datetime import datetime
from dateutil import parser as dtparser

import pandas as pd
from tabulate import tabulate

from audaz_comissoes.sheets_client import read_sheet_by_gid
from audaz_comissoes.mysql_repo import get_date_creation_by_shipment_number


def _to_yyyymm(d: datetime) -> str:
    return f"{d.year:04d}-{d.month:02d}"


_ISO_DATE_PREFIX = re.compile(r"^\s*\d{4}-\d{2}-\d{2}")


def _parse_date_any(x) -> datetime | None:
    """
    Parse robusto:
    - Se vier como Timestamp (pandas), converte.
    - Se vier ISO (YYYY-MM-DD...), usa dayfirst=False
    - Se vier BR (DD/MM/YYYY), usa dayfirst=True
    """
    if x is None:
        return None
    if isinstance(x, pd.Timestamp):
        return x.to_pydatetime()

    s = str(x).strip()
    if not s:
        return None

    try:
        if _ISO_DATE_PREFIX.match(s):
            return dtparser.parse(s, dayfirst=False)
        return dtparser.parse(s, dayfirst=True)
    except Exception:
        return None


def _norm_level(s: str) -> str:
    s = str(s or "").strip().lower()
    s = (
        s.replace("ã", "a").replace("á", "a").replace("à", "a").replace("â", "a")
        .replace("é", "e").replace("ê", "e")
        .replace("í", "i")
        .replace("ó", "o").replace("ô", "o")
        .replace("ú", "u")
        .replace("ç", "c")
    )
    return s


def parse_ptbr_money(x) -> float:
    """
    Converte valores do Sheets:
      - "4.374,34" -> 4374.34
      - "4374,34"  -> 4374.34
      - "R$ 4.374,34" -> 4374.34
      - número float/int -> float(x)
      - vazio -> 0.0
    """
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)

    s = str(x).strip()
    if not s:
        return 0.0

    s = s.replace("R$", "").replace(" ", "")

    if "," in s:
        s = s.replace(".", "").replace(",", ".")  # 4.374,34 -> 4374.34

    try:
        return float(s)
    except Exception:
        return 0.0


def get_percentual_sales_exec(df_niveis: pd.DataFrame, nivel: str) -> float:
    df = df_niveis.copy()
    df["Niveis"] = df.get("Niveis", "").astype(str)

    nk = _norm_level(nivel)
    row = df[df["Niveis"].apply(_norm_level) == nk]
    if row.empty:
        raise RuntimeError(f"Não achei o nível '{nivel}' na aba Níveis.")

    pct_str = str(row.iloc[0].get("Sales Executive") or "").strip()
    if not pct_str.endswith("%"):
        raise RuntimeError(f"Percentual inválido para Sales Executive/{nivel}: '{pct_str}'")

    pct_num = pct_str.replace("%", "").strip().replace(".", "").replace(",", ".")
    return float(pct_num) / 100.0


def filter_apurado(df_ap: pd.DataFrame, vendedor_nome: str, pay_yyyymm: str) -> tuple[pd.DataFrame, str]:
    """
    Filtra:
    - Vendedor contém 'Danielle'
    - Pay month == pay_yyyymm (usando coluna pay_col)
    Retorna (df_filtrado, pay_col_usada)
    """
    df = df_ap.copy()

    pay_col = os.getenv("PAY_DATE_COLUMN", "").strip()
    if not pay_col:
        pay_col = "Aprovada" if "Aprovada" in df.columns else ""

    if "Código" not in df.columns:
        raise RuntimeError("A aba Apurado precisa ter a coluna 'Código'.")
    if "Vendedor" not in df.columns:
        raise RuntimeError("A aba Apurado precisa ter a coluna 'Vendedor'.")

    df["Vendedor"] = df["Vendedor"].astype(str)
    df = df[df["Vendedor"].str.contains(vendedor_nome, case=False, na=False)].copy()

    if pay_col:
        df["_pay_dt"] = df[pay_col].apply(_parse_date_any)
        df["_pay_yyyymm"] = df["_pay_dt"].apply(lambda d: _to_yyyymm(d) if isinstance(d, datetime) else "")
        df = df[df["_pay_yyyymm"] == pay_yyyymm].copy()
    else:
        df["_pay_yyyymm"] = ""

    return df, pay_col


def run_case(
    df_niveis: pd.DataFrame,
    df_ap: pd.DataFrame,
    *,
    competencia_yyyymm: str,
    pay_yyyymm: str,
    vendedor_nome: str,
    nivel_forcado: str,
) -> tuple[float, float]:
    gp_col = "Profit Liquido"
    if gp_col not in df_ap.columns:
        raise RuntimeError("Não achei coluna de GP. Esperava 'Profit Liquido' na aba Apurado.")

    pct = get_percentual_sales_exec(df_niveis, nivel_forcado)
    df_d, pay_col = filter_apurado(df_ap, vendedor_nome, pay_yyyymm)

    debug_code = os.getenv("DEBUG_CODE", "").strip()
    debug = bool(debug_code)

    out_rows = []
    gp_total = 0.0
    com_total = 0.0

    for _, r in df_d.iterrows():
        codigo = str(r.get("Código") or "").strip()
        if not codigo:
            continue

        if debug and codigo != debug_code:
            continue

        dt_creation = get_date_creation_by_shipment_number(codigo)

        # Debug enxuto, mas certeiro
        if debug:
            vend_raw = str(r.get("Vendedor") or "")
            pay_raw = r.get(pay_col) if pay_col else None
            pay_dt = _parse_date_any(pay_raw) if pay_col else None
            pay_ok = (_to_yyyymm(pay_dt) == pay_yyyymm) if isinstance(pay_dt, datetime) else False
            comp_ok = (_to_yyyymm(dt_creation) == competencia_yyyymm) if dt_creation else False
            gp_raw = r.get(gp_col)
            gp_parsed = parse_ptbr_money(gp_raw)

            print("\n" + "=" * 90)
            print("DEBUG (run_case)")
            print(f"competencia: {competencia_yyyymm} | pay: {pay_yyyymm} | nivel: {nivel_forcado}")
            print("codigo:", repr(codigo))
            print("vendedor_raw:", repr(vend_raw))
            print("pay_col:", repr(pay_col), "| pay_raw:", repr(pay_raw), "| pay_dt:", pay_dt, "| pay_ok:", pay_ok)
            print("mysql_date_creation:", dt_creation, "| comp_ok:", comp_ok)
            print("gp_raw:", repr(gp_raw), "| gp_parsed:", gp_parsed)

        if not dt_creation:
            continue
        if _to_yyyymm(dt_creation) != competencia_yyyymm:
            continue

        gp = parse_ptbr_money(r.get(gp_col))
        valor_com = round(gp * pct, 2)

        gp_total += gp
        com_total += valor_com

        out_rows.append(
            {
                "codigo": codigo,
                "date_creation": dt_creation.strftime("%Y-%m-%d"),
                "pay_date": pay_yyyymm,
                "nivel": nivel_forcado,
                "percentual": f"{int(round(pct * 100))}%",
                "gp": gp,
                "valor_comissao": valor_com,
            }
        )

        if debug:
            print("=> ENTROU ✅" if out_rows else "=> NÃO entrou ❌")
            break

    # Se estiver debugando um código específico, não imprime tabelas completas
    if debug:
        return gp_total, com_total

    print("\n" + "=" * 80)
    print(f"{vendedor_nome} | Sales Executive | Competência {competencia_yyyymm} | Pay {pay_yyyymm}")
    print(f"Nível considerado: {nivel_forcado} | Percentual: {int(round(pct*100))}%")

    if not out_rows:
        print("Nenhum processo encontrado com esses filtros.")
        return 0.0, 0.0

    df_out = pd.DataFrame(out_rows).sort_values(["date_creation", "codigo"])
    print()
    print(tabulate(df_out, headers="keys", tablefmt="github", showindex=False))
    print()
    print(f"GP total: {gp_total:,.2f}")
    print(f"Comissão total: {com_total:,.2f}")

    return gp_total, com_total


def main():
    PARAM_KEY = os.getenv("GSHEETS_PARAM_KEY")
    ATLANTIS_KEY = os.getenv("GSHEETS_SPREADSHEET_KEY")
    if not PARAM_KEY or not ATLANTIS_KEY:
        raise RuntimeError("Defina GSHEETS_PARAM_KEY e GSHEETS_SPREADSHEET_KEY.")

    GID_NIVEIS = int(os.getenv("GSHEETS_NIVEIS_GID", "1124232309"))
    GID_APURADO = int(os.getenv("GSHEETS_OPERACOES_GID", "1259003990"))

    df_niveis = read_sheet_by_gid(PARAM_KEY, GID_NIVEIS)
    df_ap = read_sheet_by_gid(ATLANTIS_KEY, GID_APURADO)

    vendedor_nome = "Alisson"
    pay_yyyymm = "2025-12"

    cases = [
        {"competencia_yyyymm": "2025-08", "nivel_forcado": "Guardiao"},
        {"competencia_yyyymm": "2025-09", "nivel_forcado": "Guardiao"},
        {"competencia_yyyymm": "2025-10", "nivel_forcado": "Guardiao"},
    ]

    gp_geral = 0.0
    com_geral = 0.0

    for c in cases:
        gp_mes, com_mes = run_case(
            df_niveis,
            df_ap,
            competencia_yyyymm=c["competencia_yyyymm"],
            pay_yyyymm=pay_yyyymm,
            vendedor_nome=vendedor_nome,
            nivel_forcado=c["nivel_forcado"],
        )
        gp_geral += gp_mes
        com_geral += com_mes

    # Só imprime total se não estiver em DEBUG_CODE
    if not os.getenv("DEBUG_CODE", "").strip():
        print("\n" + "=" * 80)
        print("TOTAL GERAL (Ago/Set/Out com Pay 2025-12)")
        print(f"GP total geral: {gp_geral:,.2f}")
        print(f"Comissão total geral: {com_geral:,.2f}")


if __name__ == "__main__":
    main()
