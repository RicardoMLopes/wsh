from fastapi import FastAPI, Depends, HTTPException, APIRouter
from pydantic import BaseModel
from typing import Optional
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

#======================================================================
#
#----------------------------------------------------------------------
@user_rp.get("/user/check")
def check_user(usuario: str, db: Session = Depends(get_db)):
    sql = text("""
        SELECT *
        FROM caduser
        WHERE users = :usuario
          AND situationregistration <> 'E'
    """)

    result = db.execute(sql, {"usuario": usuario}).fetchone()

    if not result:
        return {
            "found": False,
            "message": "Usuário não encontrado. Faça INSERT."
        }

    return {
        "found": True,
        "data": dict(result._mapping),
        "message": "Usuário encontrado. Faça UPDATE."
    }
# =====================================================================================
#
# -------------------------------------------------------------------------------------


@user_rp.post("/user/save")
def save_user(
    id: Optional[int],               # ← compatível com Python 3.9
    usuario: str,
    senha_criptografada: str,
    tipo_usuario: str,
    modo: str,                       # "insert" ou "update"
    db: Session = Depends(get_db),
):
    if modo == "update":
        sql_update = text("""
            UPDATE caduser
            SET newpassword = :senha,
                usertype = :usertype
            WHERE users = :usuario AND id= :id
        """)

        db.execute(sql_update, {
            "id": id,
            "senha": senha_criptografada,
            "usertype": tipo_usuario,
            "usuario": usuario
        })
        db.commit()

        return {"status": "updated"}

    elif modo == "insert":
        sql_insert = text("""
            INSERT INTO caduser 
                ( users, newpassword, usertype, situationregistration, dateregistration)
            VALUES 
                (:usuario, :senha, :usertype, 'I', NOW())
        """)

        db.execute(sql_insert, {
            "usuario": usuario,
            "senha": senha_criptografada,
            "usertype": tipo_usuario
        })
        db.commit()

        return {"status": "inserted"}

    else:
        return {"error": "Modo inválido. Use insert ou update."}
#===============================================================================
#
#-------------------------------------------------------------------------------
class BlockUserModel(BaseModel):
    usuario: int


@user_rp.post("/user/block")
def block_user(data: BlockUserModel, db: Session = Depends(get_db)):
    # logger.info("ENTROU NA ROTINA BLOQUEAR USER")

    sql = text("""
        UPDATE caduser
        SET situationregistration = 'E',
            dateregistration = NOW()
        WHERE id = :usuario
    """)

    logger.info(f" Consulta SQL: {sql}")

    db.execute(sql, {"usuario": data.usuario})
    db.commit()

    return {"status": "blocked", "usuario": data.usuario}
