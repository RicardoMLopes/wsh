from fastapi import FastAPI, Depends
from connection.db_connection import Base, engine, SessionLocal
from wsh.cadastro.products import products_rp
from wsh.consulta.consultasgerais import consults_rp
from wsh.consulta.products import consult_prod_rp
from wsh.listagem.listamovimento import listagem_rp
from wsh.movimento.a020_a190 import a020_a190_rp
from wsh.movimento.acompanhamento import acompanhamento_rp
from wsh.movimento.cancelarmovimento import cancelputway_rp
from wsh.movimento.finishproductsputaway import putway_rp
from wsh.movimento.produtividade import produtividade_rp
from wsh.movimento.romaneio import moviment_rp
from wsh.user.login import login_rp
from wsh.user.user import user_rp

# Cria tabelas (se quiser)
Base.metadata.create_all(bind=engine)

app = FastAPI()


app.include_router(login_rp, prefix="", tags=["Login"])
app.include_router(user_rp, prefix="", tags=["users"])
app.include_router(products_rp, prefix="", tags=["products"])
app.include_router(moviment_rp, prefix="", tags=["moviments"])
app.include_router(consults_rp, prefix="", tags=["consults"])
app.include_router(consult_prod_rp, prefix="", tags=["consult product"])
app.include_router(acompanhamento_rp, prefix="", tags=["follow-up"])
app.include_router(listagem_rp, prefix="", tags=["movement list"])
app.include_router(produtividade_rp, prefix="", tags=["productivity"])
app.include_router(a020_a190_rp, prefix="", tags=["a020 a190"])
app.include_router(putway_rp, prefix="", tags=["finish process"])
app.include_router(cancelputway_rp, prefix="", tags=["cancel putway"])


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
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)