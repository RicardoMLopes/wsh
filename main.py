from fastapi import FastAPI, Depends
from connection.db_connection import Base, engine, SessionLocal
from wsh.cadastro.products import products_rp
from wsh.consulta.consultasgerais import consults_rp
from wsh.consulta.products import consult_prod_rp
from wsh.movimento.romaneio import moviment_rp
from wsh.user.login import login_rp

# Cria tabelas (se quiser)
Base.metadata.create_all(bind=engine)

app = FastAPI()


app.include_router(login_rp, prefix="", tags=["Login"])
app.include_router(products_rp, prefix="", tags=["products"])
app.include_router(moviment_rp, prefix="", tags=["moviments"])
app.include_router(consults_rp, prefix="", tags=["consults"])
app.include_router(consult_prod_rp, prefix="", tags=["consult product"])




@app.get("/")
def index():
    return {"status": "ok", "message": "API funcionando e pronta!"}

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)