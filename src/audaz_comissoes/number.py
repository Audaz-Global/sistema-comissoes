# src/audaz_comissoes/number.py
from __future__ import annotations
import re
from decimal import Decimal, InvalidOperation

def parse_ptbr_number(x) -> float:
    """
    Converte valores no formato pt-BR para float.
    Exemplos:
      "4.374,34" -> 4374.34
      "4374,34"  -> 4374.34
      "4374.34"  -> 4374.34
      4374.34    -> 4374.34
      None / ""  -> 0.0
    """
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)

    s = str(x).strip()
    if not s:
        return 0.0

    # remove moeda/espacos e mantém dígitos, sinal, ponto e vírgula
    s = re.sub(r"[^\d,\.\-+]", "", s)

    # se tem vírgula, assume vírgula = decimal e ponto = milhar
    if "," in s:
        s = s.replace(".", "").replace(",", ".")  # 4.374,34 -> 4374.34

    try:
        return float(Decimal(s))
    except (InvalidOperation, ValueError):
        return 0.0
