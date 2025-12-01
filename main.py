from fastapi import FastAPI, Depends
from connection.db_connection import Base, engine, SessionLocal
from wsh.user.login import login_rp

# Cria tabelas (se quiser)
Base.metadata.create_all(bind=engine)

app = FastAPI()


app.include_router(login_rp, prefix="", tags=["Login"])



@app.get("/")
def index():
    return {"status": "ok", "message": "API funcionando e pronta!"}

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()