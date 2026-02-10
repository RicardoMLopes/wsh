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
from wsh.api.movimento import api_rp
from wsh.user.login import login_rp
from wsh.user.user import user_rp
from models import __all__, Version

app = FastAPI()

# Cria tabelas (se quiser)
Base.metadata.create_all(bind=engine)


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
app.include_router(api_rp, prefix="", tags=["api"])


@app.get("/")
def index():
    return {"status": "ok", "message": "API funcionando e pronta!"}

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.on_event("startup")
def startup_event():
    db = SessionLocal()
    try:
        last_version = db.query(Version).order_by(Version.id.desc()).first()

        if last_version:
            # Atualiza a versão existente
            last_version.number += 1
            db.commit()
            db.refresh(last_version)
            print(f"🚀 Versão atualizada para {last_version.number}")
        else:
            # Cria a primeira versão
            new_version = Version(number=1)
            db.add(new_version)
            db.commit()
            db.refresh(new_version)
            print(f"🚀 Primeira versão registrada: {new_version.number}")

    finally:
        db.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)