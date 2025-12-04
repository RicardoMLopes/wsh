from fastapi import FastAPI, Depends, HTTPException, APIRouter
from sqlalchemy.orm import Session
from sqlalchemy import text
import logging
from connection.db_connection import Base, engine, SessionLocal
from pydantic import BaseModel

consults_rp = APIRouter()

logger = logging.getLogger("consultas")
logging.basicConfig(level=logging.INFO)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@consults_rp.get("/consulta/item")
def consulta_item(
    pn: str,
    reference: str,
    waybill: str,
    mostrar_totais: bool = False,
    db: Session = Depends(get_db)
):
    conn = db.connection().connection
    cursor = conn.cursor()
    resultado = {"putaway": None, "products": None, "totais": None}
    logger.info(f"Resultado SELECT: {resultado}")

    try:
        # Consulta whsproductsputaway
        sql_putaway = f"""
            SELECT id, Description, Position, siccode, Qty, processlines,
                   RevisedQty, StandardQty, LPSQty, UndeclaredSQty, BreakdownQty
            FROM whsproductsputaway
            WHERE PN = '{pn}' AND Reference = '{reference}' AND Waybill = '{waybill}'
              AND situationregistration <> 'E'
        """
        logger.info(f"Resultado SELECT: {sql_putaway}")
        cursor.execute(sql_putaway)
        row = cursor.fetchone()
        if row:
            resultado["putaway"] = {
                "id": row[0],
                "description": row[1],
                "position": row[2],
                "siccode": row[3],
                "qty": row[4],
                "processlines": row[5],
                "revisedqty": row[6],
                "standardqty": row[7],
                "lpsqty": row[8],
                "undeclaredsqty": row[9],
                "breakdownqty": row[10],
            }

        # Consulta whsproducts
        sql_products = f"""
            SELECT Description, Position, siccode
            FROM whsproducts
            WHERE PN = '{pn}' AND situationregistration <> 'E'
        """
        cursor.execute(sql_products)
        row = cursor.fetchone()
        if row:
            resultado["products"] = {
                "description": row[0],
                "position": row[1],
                "siccode": row[2],
            }

        # Consulta totais no log
        if mostrar_totais and not resultado["putaway"]:
            sql_totais = f"""
                SELECT SUM(StandardQty) AS SQty,
                       SUM(LPSQty) AS LQty,
                       SUM(UndeclaredSQty) AS UQty,
                       SUM(BreakdownQty) AS BQty
                FROM whsproductsputawaylog
                WHERE Reference = '{reference}' AND Waybill = '{waybill}' AND PN = '{pn}'
                  AND RevisedQty > 0 AND situationregistration <> 'E'
            """
            cursor.execute(sql_totais)
            row = cursor.fetchone()

            logger.info(f"totais: {row}")
            if row:
                resultado["totais"] = {
                    "standardqty": row[0],
                    "lpsqty": row[1],
                    "undeclaredsqty": row[2],
                    "breakdownqty": row[3],
                }

    finally:
        cursor.close()
        conn.close()

    return {"status": "ok", "resultado": resultado}