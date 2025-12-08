from fastapi import FastAPI, Depends, HTTPException, APIRouter
from sqlalchemy.orm import Session
from sqlalchemy import text
from connection.db_connection import Base, engine, SessionLocal
import logging

user_rp = APIRouter()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("usuario_logger")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# 🔹 ROTA: Buscar usuário por login
@user_rp.get("/user")
def get_caduser(usuario: str, db: Session = Depends(get_db)):

    sql = text("""
        SELECT *
        FROM caduser
        WHERE users = :usuario
          AND situationregistration <> 'E'
    """)
    logger.info(f" Consulta SQL: {sql}")
    result = db.execute(sql, {"usuario": usuario}).fetchone()

    if not result:
        return {"found": False}

    return {"found": True, "data": dict(result._mapping)}