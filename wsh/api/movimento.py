import logging
from fastapi import Depends, APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text
import traceback
from connection.db_connection import SessionLocal
from datetime import datetime

api_rp = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("import_api")

class Putaway(BaseModel):
    idlog: int
    user_id: int
    pn: Optional[str] = None
    position: Optional[str] = None
    reference: Optional[str] = None
    datecreate: Optional[str] = None
    cont: Optional[str] = None
    status: Optional[str] = None
    confirm: Optional[str] = "N"
    dateregistration: Optional[str] = None
    synchronize: Optional[str] = "F"



@api_rp.post("/import/movimento")
def import_movimento(item: Putaway, db: Session = Depends(get_db)):
    try:
        conn = db.connection().connection
        cursor = conn.cursor()
        logging.info("Exibir dados: %s", item)
        # Se não vier data, usa agora
        datecreate = item.datecreate or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        dateregistration = item.dateregistration or datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        cursor.execute("""
            INSERT INTO whsmovementputaway
            (idlog, user_id, pn, position, reference, datecreate, cont, status, confirm, dateregistration, synchronize)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            item.idlog,
            item.user_id,
            item.pn,
            item.position,
            item.reference,
            datecreate,
            item.cont,
            item.status,
            item.confirm,
            dateregistration,
            item.synchronize
        ))

        conn.commit()
        conn.close()
        return {"message": "Dados inseridos com sucesso"}
    except Exception as e:

        print("Erro ao inserir:", e)
        traceback.print_exc()
        raise HTTPException(status_code=400, detail=str(e))
