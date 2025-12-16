from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import text
from connection.db_connection import SessionLocal
from sqlalchemy.orm import Session

a020_a190_rp = APIRouter()

# ------------------------------
# MODELO DOS DADOS ENVIADOS
# ------------------------------
class Linha(BaseModel):
    flag: str
    campo01: str
    pn: str
    descricao: str
    qty: str
    referencia: str
    waybill: str
    processlines: str
    usarDescricaoPN: bool


class ImportacaoRequest(BaseModel):
    tipo: str                     # <- AGORA O TIPO VEM NO JSON
    linhas: list[Linha]



# ------------------------------------------------------
# FUNÇÃO PARA OBTER CONEXÃO
# ------------------------------------------------------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ------------------------------------------------------
# ROTA PRINCIPAL
# ------------------------------------------------------
@a020_a190_rp.post("/importar/a020_a190")
def importar_a020_a190(payload: ImportacaoRequest, db: Session = Depends(get_db)):

    tipo = payload.tipo.upper()
    inputtype = "national" if tipo == "A020" else "transfer"

    for linha in payload.linhas:

        if linha.campo01.strip() == "" or linha.flag.strip() == "S":
            continue

        # ----------------------------------------------------
        # VERIFICA SE JÁ EXISTE
        # ----------------------------------------------------
        consulta = text("""
            SELECT *
            FROM whsproductsputaway
            WHERE Reference = :reference
              AND Waybill = :waybill
              AND PN = :pn
              AND situationregistration <> 'E'
            LIMIT 1
        """)

        result = db.execute(consulta, {
            "reference": linha.referencia,
            "waybill": linha.waybill,
            "pn": linha.pn
        }).mappings().first()

        # ----------------------------------------------------
        # SE EXISTE → ATUALIZA
        # ----------------------------------------------------
        if result:

            revisedQty = result["RevisedQty"] if result["RevisedQty"] is not None else 0

            if revisedQty == 0 and linha.flag != "S":

                descricao = linha.descricao if not linha.usarDescricaoPN else linha.pn

                update_sql = text("""
                    UPDATE whsproductsputaway
                    SET Qty = :qty,
                        Description = :descricao,
                        processlines = :processlines,
                        inputtype = :inputtype,
                        situationregistration = 'A',
                        dateregistration = NOW()
                    WHERE Reference = :reference
                      AND Waybill = :waybill
                      AND PN = :pn
                """)

                db.execute(update_sql, {
                    "qty": linha.qty,
                    "descricao": descricao,
                    "processlines": linha.processlines,
                    "inputtype": inputtype,
                    "reference": linha.referencia,
                    "waybill": linha.waybill,
                    "pn": linha.pn
                })
                db.commit()

        # ----------------------------------------------------
        # SE NÃO EXISTE → INSERE
        # ----------------------------------------------------
        else:

            max_id = db.execute(text("SELECT COALESCE(MAX(ID), 0) AS id FROM whsproductsputaway")).scalar()
            max_id += 1

            descricao = linha.descricao if not linha.usarDescricaoPN else linha.pn

            insert_sql = text("""
                INSERT INTO whsproductsputaway
                    (Id, User_id, PN, Description, Reference, Qty,
                     Waybill, processlines, inputtype, datecreate,
                     situationregistration, dateregistration)
                VALUES
                    (:id, 0, :pn, :descricao, :reference, :qty,
                     :waybill, :processlines, :inputtype, NOW(),
                     'I', NOW())
            """)

            db.execute(insert_sql, {
                "id": max_id,
                "pn": linha.pn,
                "descricao": descricao,
                "reference": linha.referencia,
                "qty": linha.qty,
                "waybill": linha.waybill,
                "processlines": linha.processlines,
                "inputtype": inputtype
            })
            db.commit()

    return {"status": "OK", "mensagem": "Rotina executada com sucesso!"}