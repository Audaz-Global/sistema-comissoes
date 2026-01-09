# scripts/test_sales_exec_comp_by_date_creation_and_date_comission.py
import os
from datetime import datetime

import pandas as pd
from tabulate import tabulate

from audaz_comissoes.sheets_client import read_sheet_by_gid
from audaz_comissoes.mysql_repo import get_dates_by_shipment_number


def _to_yyyymm(d: datetime) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def _norm_text(s: str) -> str:
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
    Converte valores pt-BR vindos do Sheets:
      - "4.374,34" -> 4374.34
      - "4374,34"  -> 4374.34
      - "R$ 4.374,34" -> 4374.34
      - número já float/int -> float(x)
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
        s = s.replace(".", "").replace(",", ".")

    try:
        return float(s)
    except Exception:
        return 0.0


def get_percentual_sales_exec(df_niveis: pd.DataFrame, nivel: str) -> float:
    if "Niveis" not in df_niveis.columns:
        raise RuntimeError("A aba Níveis precisa ter a coluna 'Niveis'.")

    df = df_niveis.copy()
    df["Niveis"] = df["Niveis"].astype(str)

    nk = _norm_text(nivel)
    row = df[df["Niveis"].apply(_norm_text) == nk]
    if row.empty:
        raise RuntimeError(f"Não achei o nível '{nivel}' na aba Níveis.")

    pct_str = str(row.iloc[0].get("Sales Executive") or "").strip()
    if not pct_str.endswith("%"):
        raise RuntimeError(f"Percentual inválido para Sales Executive/{nivel}: '{pct_str}'")

    pct_num = pct_str.replace("%", "").strip().replace(".", "").replace(",", ".")
    return float(pct_num) / 100.0


def get_sales_exec_people(df_meta: pd.DataFrame) -> list[dict]:
    required = ["Colaborador", "Email", "Função", "Nível"]
    for c in required:
        if c not in df_meta.columns:
            raise RuntimeError(f"A aba Meta Colaborador precisa ter a coluna '{c}'.")

    df = df_meta.copy()
    for c in required:
        df[c] = df[c].astype(str).fillna("")

    df["_func_norm"] = df["Função"].apply(_norm_text)
    df = df[df["_func_norm"] == _norm_text("Sales Executive")].copy()

    people = []
    for _, r in df.iterrows():
        nome = str(r.get("Colaborador") or "").strip()
        email = str(r.get("Email") or "").strip()
        nivel = str(r.get("Nível") or "").strip()
        if not nome or not nivel:
            continue
        people.append({"nome": nome, "email": email, "nivel": nivel})

    # uniq por email/nome
    seen = set()
    uniq = []
    for p in people:
        key = (p["email"] or p["nome"]).strip().lower()
        if key in seen:
            continue
        seen.add(key)
        uniq.append(p)

    return uniq


def filter_apurado_by_vendedor(df_ap: pd.DataFrame, nome: str) -> pd.DataFrame:
    if "Código" not in df_ap.columns:
        raise RuntimeError("A aba Apurado precisa ter a coluna 'Código'.")
    if "Vendedor" not in df_ap.columns:
        raise RuntimeError("A aba Apurado precisa ter a coluna 'Vendedor'.")

    df = df_ap.copy()
    df["Vendedor"] = df["Vendedor"].astype(str)
    return df[df["Vendedor"].str.contains(nome, case=False, na=False)].copy()


def _debug_precheck_in_apurados(df_ap: pd.DataFrame, debug_code: str):
    if not debug_code:
        return

    print("\n" + "-" * 90)
    print("DEBUG (pré-filtros) — procurando o código no Apurados inteiro")
    print("DEBUG_CODE:", repr(debug_code))

    if "Código" not in df_ap.columns:
        print("ERRO: Apurados não tem coluna 'Código'")
        return

    df_ap["_codigo_norm"] = df_ap["Código"].astype(str).str.strip()
    hits = df_ap[df_ap["_codigo_norm"] == debug_code]
    print("Encontrados (match exato):", len(hits))

    if not hits.empty:
        r = hits.iloc[0].to_dict()
        print("\nLinha (bruta) no Apurados:")
        print("Código:", repr(r.get("Código")))
        print("Vendedor:", repr(r.get("Vendedor")))
        print("Profit Liquido:", repr(r.get("Profit Liquido")))
    print("-" * 90)


def run_for_person(
    *,
    df_niveis: pd.DataFrame,
    df_ap: pd.DataFrame,
    nome: str,
    email: str,
    nivel: str,
    competencias: list[str],
    pay_yyyymm: str,
) -> tuple[float, float]:
    gp_col = "Profit Liquido"
    if gp_col not in df_ap.columns:
        raise RuntimeError("Não achei coluna de GP. Esperava 'Profit Liquido' na aba Apurado.")

    pct = get_percentual_sales_exec(df_niveis, nivel)
    df_person = filter_apurado_by_vendedor(df_ap, nome)

    debug_code = os.getenv("DEBUG_CODE", "").strip()

    gp_total_all = 0.0
    com_total_all = 0.0

    print("\n" + "#" * 100)
    print(f"{nome} <{email}> | Sales Executive | Nível: {nivel} | %: {int(round(pct*100))}%")
    print(f"Filtros: DATE_CREATION em {', '.join(competencias)} | DATE_COMISSION em {pay_yyyymm}")
    print("#" * 100)

    for comp in competencias:
        out_rows = []
        gp_total = 0.0
        com_total = 0.0

        for _, r in df_person.iterrows():
            codigo = str(r.get("Código") or "").strip()
            if not codigo:
                continue

            if debug_code and codigo != debug_code:
                continue

            dt_creation, dt_comission = get_dates_by_shipment_number(codigo)

            if debug_code:
                print("\n" + "=" * 90)
                print("DEBUG (dentro do run_for_person)")
                print("competencia:", comp, "| pay_yyyymm:", pay_yyyymm)
                print("codigo:", repr(codigo))
                print("vendedor_raw:", repr(r.get("Vendedor")))
                print("mysql_date_creation:", dt_creation, "| yyyymm:", (_to_yyyymm(dt_creation) if dt_creation else None))
                print("mysql_date_comission:", dt_comission, "| yyyymm:", (_to_yyyymm(dt_comission) if dt_comission else None))

            if not dt_creation or not dt_comission:
                if debug_code:
                    print("=> REPROVOU: dt_creation ou dt_comission está None")
                continue

            comp_ok = _to_yyyymm(dt_creation) == comp
            pay_ok = _to_yyyymm(dt_comission) == pay_yyyymm

            if not comp_ok:
                if debug_code:
                    print("=> REPROVOU no filtro de COMPETÊNCIA (DATE_CREATION)")
                continue

            if not pay_ok:
                if debug_code:
                    print("=> REPROVOU no filtro de PAY (DATE_COMISSION)")
                continue

            gp = parse_ptbr_money(r.get(gp_col))
            valor_com = round(gp * pct, 2)

            gp_total += gp
            com_total += valor_com

            out_rows.append({
                "codigo": codigo,
                "date_creation": dt_creation.strftime("%Y-%m-%d"),
                "date_comission": dt_comission.strftime("%Y-%m-%d"),
                "nivel": nivel,
                "percentual": f"{int(round(pct*100))}%",
                "gp": gp,
                "valor_comissao": valor_com,
            })

            if debug_code:
                print("=> ENTROU NA LISTA ✅")
                break

        print("\n" + "-" * 90)
        print(f"{nome} | Competência {comp} | DATE_COMISSION {pay_yyyymm}")

        if not out_rows:
            print("Nenhum processo encontrado com esses filtros.")
            continue

        df_out = pd.DataFrame(out_rows).sort_values(["date_creation", "codigo"])
        print()
        print(tabulate(df_out, headers="keys", tablefmt="github", showindex=False))
        print()
        print(f"GP total: {gp_total:,.2f}")
        print(f"Comissão total: {com_total:,.2f}")

        gp_total_all += gp_total
        com_total_all += com_total

    print("\n" + "=" * 90)
    print(f"TOTAL {nome} (Competências: {', '.join(competencias)} | DATE_COMISSION: {pay_yyyymm})")
    print(f"GP total: {gp_total_all:,.2f}")
    print(f"Comissão total: {com_total_all:,.2f}")
    print("=" * 90)

    return gp_total_all, com_total_all


def main():
    PARAM_KEY = os.getenv("GSHEETS_PARAM_KEY")
    ATLANTIS_KEY = os.getenv("GSHEETS_SPREADSHEET_KEY")
    if not PARAM_KEY or not ATLANTIS_KEY:
        raise RuntimeError("Defina GSHEETS_PARAM_KEY e GSHEETS_SPREADSHEET_KEY.")

    GID_NIVEIS = int(os.getenv("GSHEETS_NIVEIS_GID", "1124232309"))
    GID_META_COLAB = int(os.getenv("GSHEETS_META_COLABORADOR_GID", "873121416"))
    GID_APURADO = int(os.getenv("GSHEETS_OPERACOES_GID", "1259003990"))

    pay_yyyymm = os.getenv("TEST_PAY_YYYYMM", "2025-12").strip()
    competencias_raw = os.getenv("TEST_COMPETENCIAS", "2025-08,2025-09,2025-10").strip()
    competencias = [c.strip() for c in competencias_raw.split(",") if c.strip()]

    debug_code = os.getenv("DEBUG_CODE", "").strip()

    df_niveis = read_sheet_by_gid(PARAM_KEY, GID_NIVEIS)
    df_meta = read_sheet_by_gid(PARAM_KEY, GID_META_COLAB)
    df_ap = read_sheet_by_gid(ATLANTIS_KEY, GID_APURADO)

    _debug_precheck_in_apurados(df_ap, debug_code)

    people = get_sales_exec_people(df_meta)
    if not people:
        raise RuntimeError("Não encontrei ninguém com Função = Sales Executive na aba Meta Colaborador.")

    gp_geral = 0.0
    com_geral = 0.0

    for p in people:
        gp_p, com_p = run_for_person(
            df_niveis=df_niveis,
            df_ap=df_ap,
            nome=p["nome"],
            email=p["email"],
            nivel=p["nivel"],
            competencias=competencias,
            pay_yyyymm=pay_yyyymm,
        )
        gp_geral += gp_p
        com_geral += com_p

    print("\n" + "#" * 90)
    print(f"TOTAL GERAL (Sales Executive | {', '.join(competencias)} | DATE_COMISSION {pay_yyyymm})")
    print(f"GP total geral: {gp_geral:,.2f}")
    print(f"Comissão total geral: {com_geral:,.2f}")
    print("#" * 90)


if __name__ == "__main__":
    main()
