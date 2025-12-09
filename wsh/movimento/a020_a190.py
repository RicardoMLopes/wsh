from fastapi import APIRouter, Depends
from pydantic import BaseModel
from typing import List, Optional
import datetime
from connection.db_connection import SessionLocal
from sqlalchemy.orm import Session

a020_rp = APIRouter()

# ------------------------------
# MODELO DOS DADOS ENVIADOS
# ------------------------------
class LinhaEntrada(BaseModel):
    flag: str               # Cells[00]
    campo01: str            # Cells[01]
    pn: str                 # Cells[02]
    descricao: str          # Cells[03]
    qty: str                # Cells[04]
    referencia: str         # Cells[05]
    waybill: str            # Cells[06]
    processlines: str       # Cells[07]
    usarDescricaoPN: bool   # CBPnDescricao.Checked


class ImportacaoRequest(BaseModel):
    linhas: List[LinhaEntrada]


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
@a020_rp.post("/importar/nacional020")
def importar_nacional020(payload: ImportacaoRequest, db: Session = Depends(get_db)):

    for linha in payload.linhas:

        if linha.campo01.strip() == "" or linha.flag.strip() == "S":
            continue

        # -----------------------------------------
        # VERIFICA SE JÁ EXISTE
        # -----------------------------------------
        db.execute("""
            SELECT TOP 1 *
            FROM whsproductsputaway
            WHERE Reference = ?
              AND Waybill = ?
              AND PN = ?
              AND situationregistration <> 'E'
        """, (linha.referencia, linha.waybill, linha.pn))

        row = db.fetchone()

        # -----------------------------------------
        # 1) SE EXISTE → ATUALIZA
        # -----------------------------------------
        if row:

            revisedQty = row.RevisedQty if row.RevisedQty is not None else 0

            if revisedQty == 0 and linha.flag != "S":

                descricao = linha.descricao if not linha.usarDescricaoPN else linha.pn

                db.execute("""
                    UPDATE whsproductsputaway
                    SET Qty = ?,
                        Description = ?,
                        processlines = ?,
                        situationregistration = 'A',
                        dateregistration = GETDATE()
                    WHERE Reference = ?
                      AND Waybill = ?
                      AND PN = ?
                """, (
                    linha.qty,
                    descricao,
                    linha.processlines,
                    linha.referencia,
                    linha.waybill,
                    linha.pn
                ))
                db.commit()

        # -----------------------------------------
        # 2) SE NÃO EXISTE → INSERE
        # -----------------------------------------
        else:

            db.execute("SELECT MAX(ID) FROM whsproductsputaway")
            result = db.fetchone()
            max_id = result[0] if result[0] else 0
            max_id += 1

            descricao = linha.descricao if not linha.usarDescricaoPN else linha.pn

            db.execute("""
                INSERT INTO whsproductsputaway
                    (Id, User_id, PN, Description, Reference, Qty,
                     Waybill, processlines, datecreate,
                     situationregistration, dateregistration)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), ?, GETDATE())
            """, (
                max_id,
                0,
                linha.pn,
                descricao,
                linha.referencia,
                linha.qty,
                linha.waybill,
                linha.processlines,
                "I"
            ))

            db.commit()

    return {"status": "OK", "mensagem": "Rotina executada com sucesso!"}
