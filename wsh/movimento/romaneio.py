from fastapi import Depends, APIRouter, HTTPException
from pydantic import BaseModel
from typing import List
from connection.db_connection import SessionLocal
from sqlalchemy.orm import Session
from datetime import datetime
import logging

moviment_rp = APIRouter()

class PutawayItem(BaseModel):
    pn: str
    description: str
    referencia: str
    qtd: float
    waybill: str
    processlines: str


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ==================================================
#   ROTA PARA IMPORTAR OS DADOS ROMANEIO
#===================================================
logger = logging.getLogger("romaneio")
logging.basicConfig(level=logging.INFO)

@moviment_rp.post("/romaneio")
def putaway(items: List[PutawayItem], db: Session = Depends(get_db)):
    conn = db.connection().connection
    cursor = conn.cursor()

    total = 0
    inseridos = 0
    atualizados = 0
    ignorados = 0

    for item in items:
        total += 1
        logger.info(f"Recebido item: {item}")

        # Verifica se já existe
        cursor.execute("""
            SELECT * FROM whsproductsputaway
            WHERE Reference=%s AND Waybill=%s AND PN=%s AND situationregistration <> 'E'
        """, (item.referencia, item.waybill, item.pn))
        row = cursor.fetchone()
        logger.info(f"Resultado SELECT: {row}")

        if row:
            # Se RevisedQty = 0, faz UPDATE
            logger.info("Registro encontrado, verificando RevisedQty...")
            try:
                idx = [desc[0] for desc in cursor.description].index("RevisedQty")
                revised_qty = row[idx]
            except ValueError:
                logger.error("Coluna RevisedQty não encontrada na consulta!")
                revised_qty = None

            logger.info(f"RevisedQty = {revised_qty}")
            if revised_qty == 0:
                logger.info("Executando UPDATE...")
                cursor.execute("""
                    UPDATE whsproductsputaway
                    SET Qty=%s, Description=%s, processlines=%s, situationregistration='A', dateregistration=%s
                    WHERE Reference=%s AND Waybill=%s AND PN=%s
                """, (item.qtd, item.description, item.processlines, datetime.now(),
                      item.referencia, item.waybill, item.pn))
                atualizados += 1
            else:
                ignorados += 1
        else:
            # Faz INSERT
            logger.info("Nenhum registro encontrado, executando INSERT...")
            cursor.execute("SELECT MAX(Id) FROM whsproductsputaway")
            max_id = cursor.fetchone()[0] or 0
            new_id = max_id + 1
            logger.info(f"Novo Id calculado: {new_id}")

            cursor.execute("""
                INSERT INTO whsproductsputaway
                (Id, User_id, PN, Description, Reference, Qty, Waybill, processlines, datecreate, situationregistration, dateregistration)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (new_id, 0, item.pn, item.description, item.referencia, item.qtd,
                  item.waybill, item.processlines, datetime.now(), 'I', datetime.now()))
            inseridos += 1

    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Operação concluída com sucesso.")

    return {
        "status": "ok",
        "total": total,
        "inseridos": inseridos,
        "atualizados": atualizados,
        "ignorados": ignorados
    }