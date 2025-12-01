from fastapi import FastAPI, Depends, HTTPException, APIRouter
from sqlalchemy.orm import Session
from sqlalchemy import text
from connection.db_connection import Base, engine, SessionLocal
from pydantic import BaseModel

login_rp = APIRouter()

class LoginSchema(BaseModel):
    users: str
    senha: str


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@login_rp.post("/login")
def login(dados: LoginSchema, db: Session = Depends(get_db)):

    # LOG: ver o que chegou do cliente
    print("📥 Recebido no Body:")
    print(dados)
    print("users =", dados.users)
    print("senha =", dados.senha)

    query = text("""
        SELECT id, users, newpassword
        FROM caduser
        WHERE users = :users
        LIMIT 1
    """)

    result = db.execute(query, {"users": dados.users}).fetchone()

    # LOG: ver resultado SQL
    print("📤 Resultado SQL =", result)

    if not result:
        raise HTTPException(status_code=404, detail="Usuário não encontrado")

    return {
        "msg": "Usuário localizado",
        "usuario_id": result.id,
        "users": result.users,
        "newpassword": result.newpassword
    }
