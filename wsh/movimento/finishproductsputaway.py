from fastapi import Depends, APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
from typing import List
from connection.db_connection import SessionLocal
from sqlalchemy.orm import Session
from datetime import datetime
import logging, time
from sqlalchemy import text


putway_rp = APIRouter()

# Logger configurado
logger = logging.getLogger("Finalizar")
logging.basicConfig(level=logging.INFO)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


#=========================================================================
#    ROTA 1 — Verificar PN sem lançamento
#-------------------------------------------------------------------------
@putway_rp.get("/check-missing")
def check_missing(ref: str, way: str, db: Session = Depends(get_db)):
    logger.info("=== ROTA /check-missing ===")
    logger.info(f"Parâmetros recebidos: ref={ref}, way={way}")

    try:
        sql = text("""
            SELECT *
            FROM whsproductsputaway
            WHERE Reference = :ref
              AND Waybill = :way
              AND Qty > 0 AND RevisedQty = 0
        """)

        logger.info("Executando SQL check_missing...")

        rows = db.execute(sql, {"ref": ref, "way": way}).fetchall()

        itens = [dict(r._mapping) for r in rows]

        # Verificação de múltiplos user_id
        multiple_user_ids = False
        user_ids_list = []

        multiple_user_ids = False

        for item in itens:
            raw_user_id = item.get("operator_id")
            logger.info(f"Exibir ids brutos: {raw_user_id}")

            if raw_user_id:
                # força para string e remove espaços
                user_id_str = str(raw_user_id).strip()
                # separa por vírgula e remove vazios
                ids = [uid.strip() for uid in user_id_str.split(",") if uid.strip()]
                logger.info(f"Exibir ids processados: {ids}")

                # se este registro tiver mais de um ID, já marca como múltiplos
                if len(ids) > 1:
                    multiple_user_ids = True
                    break  # não precisa continuar, já ac

        logger.info(f" multiple user ids: {multiple_user_ids}")
        resposta = {
            "found": len(rows) > 0,
            "count": len(rows),
            "items": itens,
            "multiple_user_ids": multiple_user_ids
        }

        # logger.info(f"Resposta enviada: {resposta}")
        logger.info("=== FIM /check-missing ===")

        return resposta

    except Exception as e:
        logger.error("ERRO na rota /check-missing")
        logger.error(f"Exception: {e}")
        return {"error": str(e)}

#============================================================================
# ROTA 2 — Finalizar processo no log
#----------------------------------------------------------------------------

class EndLogModel(BaseModel):
    ref: str
    way: str
    user_id: int

@putway_rp.post("/end-log")
def end_log(data: EndLogModel, db: Session = Depends(get_db)):
    sql = text("""
        UPDATE whsproductsputawaylog
        SET DateProcessEnd = NOW()
        WHERE Reference = :ref AND Waybill = :way AND User_Id = :user
    """)

    db.execute(sql, {"ref": data.ref, "way": data.way, "user": data.user_id})
    db.commit()
    return {"updated": True}

#===============================================================================
#  ROTA 3 — Buscar operadores ativos
#-------------------------------------------------------------------------------
@putway_rp.get("/active-users")
def active_users(ref: str, way: str, db: Session = Depends(get_db)):
    sql = text("""
        SELECT DateProcessEnd, operator_id, User_Id
        FROM whsproductsputawaylog
        WHERE Reference = :ref
        AND Waybill = :way
        AND DateProcessEnd IS NULL
        ORDER BY User_Id
    """)

    rows = db.execute(sql, {"ref": ref, "way": way}).fetchall()

    return {
        "count": len(rows),
        "users": [dict(r._mapping) for r in rows]
    }


#===============================================================================
# ROTA 4A — Atualizar operador final
#-------------------------------------------------------------------------------

class SetOperatorModel(BaseModel):
    ref: str
    way: str
    operator_id: str


@putway_rp.post("/set-operator")
def set_operator(data: SetOperatorModel, db: Session = Depends(get_db)):
    sql = text("""
        UPDATE whsproductsputaway
        SET operator_id = :op
        WHERE Reference = :ref AND Waybill = :way
    """)

    db.execute(sql, {"op": data.operator_id, "ref": data.ref, "way": data.way})
    db.commit()

    return {"updated": True}
#===============================================================================
# ROTA 4B — Finalizar processo
#-------------------------------------------------------------------------------

class FinalizeModel(BaseModel):
    ref: str
    way: str

@putway_rp.post("/finalize")
def finalize(data: FinalizeModel, db: Session = Depends(get_db)):
    sql = text("""
        UPDATE whsproductsputaway
        SET DateProcessEnd = NOW()
        WHERE Reference = :ref AND Waybill = :way
    """)

    db.execute(sql, {"ref": data.ref, "way": data.way})
    db.commit()

    return {"finalized": True}

#==============================================================================
#                       operator-finish
#------------------------------------------------------------------------------
class OperatorFinishModel(BaseModel):
    reference: str
    waybill: str
    operator: str
    operator_count: int   # quantidade de operadores ativos no Delphi


@putway_rp.post("/operator-finish")
def operator_finish(data: OperatorFinishModel, db: Session = Depends(get_db)):
    logger.info(f"Dados da Requisição: {data}")
    try:
        sql = text("""
                SELECT  DateProcessEnd
                FROM whsproductsputaway
                WHERE Reference =:ref
                AND   Waybill =:way
                AND   DateProcessEnd IS NULL

                """)
        rows = db.execute(sql, {"ref": data.reference, "way": data.waybill}).fetchall()



    # if data.operator_count > 1:
    #         sql = text("""
    #             UPDATE whsproductsputaway
    #             SET operator_id = :op
    #             WHERE Reference = :ref
    #             AND Waybill = :way
    #         """)
    #         logger.info(f"Existe mais de um OPERADOR: {sql}")
    #         db.execute(sql, {"op": data.operator, "ref": data.reference, "way": data.waybill})
    #         db.commit()
    #
    #         return {"updated": True, "finalized": False}
    #
    #     else:
    #         # Somente 1 operador ativo → finalizar processo
        if rows==None:
            sql = text("""
                UPDATE whsproductsputaway
                SET DateProcessEnd = NOW()
                WHERE Reference = :ref
                AND Waybill = :way
            """)
            logger.info(f"FINALIZAR PROCESSO: {sql}")
            db.execute(sql, {"ref": data.reference, "way": data.waybill})
            db.commit()

            return {"updated": True, "finalized": True}
        else:
            return{"updated": False, "finalized": False}


    except Exception as e:
        return {"error": str(e)}

