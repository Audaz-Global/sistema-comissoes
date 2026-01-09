# src/audaz_comissoes/mysql_repo.py
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Tuple

import pymysql


def _mysql_conn():
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME", "atlantis"),
        port=int(os.getenv("DB_PORT", "3306")),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


@dataclass(frozen=True)
class ShipmentDates:
    date_creation: Optional[datetime]
    date_comission: Optional[datetime]


def get_shipment_dates_by_number(shipment_number: str) -> ShipmentDates:
    """
    M0020_SHIPMENT_HOUSE.SHIPMENT_NUMBER = Código (Sheets)
    Retorna DATE_CREATION e DATE_COMMISSION (datetime) ou None.
    """
    if not shipment_number:
        return ShipmentDates(None, None)

    sql = """
        SELECT DATE_CREATION, DATE_COMMISSION
        FROM M0020_SHIPMENT_HOUSE
        WHERE SHIPMENT_NUMBER = %s
        ORDER BY DATE_CREATION ASC
        LIMIT 1
    """

    conn = _mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (shipment_number,))
            row = cur.fetchone()
            if not row:
                return ShipmentDates(None, None)

            return ShipmentDates(
                date_creation=row.get("DATE_CREATION"),
                date_comission=row.get("DATE_COMMISSION"),
            )
    finally:
        conn.close()


def get_dates_by_shipment_number(shipment_number: str) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Compat com scripts:
      dt_creation, dt_comission = get_dates_by_shipment_number(codigo)
    """
    d = get_shipment_dates_by_number(shipment_number)
    return d.date_creation, d.date_comission
