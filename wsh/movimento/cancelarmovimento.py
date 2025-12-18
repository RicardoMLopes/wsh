from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
import logging
from connection.db_connection import SessionLocal
from pydantic import BaseModel

cancelputway_rp = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


#===================================================================================
#   CANCELAMENTO DO MOVIMENTO
#-----------------------------------------------------------------------------------
class CancelMovementModel(BaseModel):
    log_id: int


@cancelputway_rp.post("/cancel-movement")
def cancel_movement(data: CancelMovementModel, db: Session = Depends(get_db)):
    try:
        # 1) Atualiza whsproductsputaway subtraindo quantidades
        sql_update_putaway = text("""
            UPDATE whsproductsputaway
            INNER JOIN whsproductsputawaylog
                ON whsproductsputaway.id = whsproductsputawaylog.Id_whsprod
            SET
                whsproductsputaway.RevisedQty =
                    whsproductsputaway.RevisedQty - whsproductsputawaylog.RevisedQty,
                whsproductsputaway.StandardQty =
                    whsproductsputaway.StandardQty - whsproductsputawaylog.StandardQty,
                whsproductsputaway.LPSQty =
                    whsproductsputaway.LPSQty - whsproductsputawaylog.LPSQty,
                whsproductsputaway.UndeclaredSQty =
                    whsproductsputaway.UndeclaredSQty - whsproductsputawaylog.UndeclaredSQty,
                whsproductsputaway.breakdownQty =
                    whsproductsputaway.breakdownQty - whsproductsputawaylog.breakdownQty
            WHERE whsproductsputawaylog.id = :log_id
              AND whsproductsputawaylog.situationregistration <> 'E'
        """)

        db.execute(sql_update_putaway, {"log_id": data.log_id})

        # 2) Marca o log como excluído
        sql_update_log = text("""
            UPDATE whsproductsputawaylog
            SET
                situationregistration = 'E',
                dateregistration = NOW()
            WHERE id = :log_id
        """)

        db.execute(sql_update_log, {"log_id": data.log_id})

        db.commit()

        return {
            "success": True,
            "log_id": data.log_id
        }

    except Exception as e:
        db.rollback()
        return {
            "success": False,
            "error": str(e)
        }

#================================================================================
#       ESTORNO DO MOVIMENTO
#--------------------------------------------------------------------------------

class ReversalMovementModel(BaseModel):
    log_id: int

@cancelputway_rp.post("/reversal-movement")
def reversal_movement(data: ReversalMovementModel, db: Session = Depends(get_db)):
    try:
        # 1) Devolve quantidades ao putaway
        sql_update_putaway = text("""
            UPDATE whsproductsputaway
            INNER JOIN whsproductsputawaylog
                ON whsproductsputaway.id = whsproductsputawaylog.Id_whsprod
            SET
                whsproductsputaway.RevisedQty =
                    whsproductsputaway.RevisedQty + whsproductsputawaylog.RevisedQty,
                whsproductsputaway.StandardQty =
                    whsproductsputaway.StandardQty + whsproductsputawaylog.StandardQty,
                whsproductsputaway.LPSQty =
                    whsproductsputaway.LPSQty + whsproductsputawaylog.LPSQty,
                whsproductsputaway.UndeclaredSQty =
                    whsproductsputaway.UndeclaredSQty + whsproductsputawaylog.UndeclaredSQty,
                whsproductsputaway.breakdownQty =
                    whsproductsputaway.breakdownQty + whsproductsputawaylog.breakdownQty
            WHERE whsproductsputawaylog.id = :log_id
              AND whsproductsputawaylog.situationregistration = 'E'
        """)

        result = db.execute(sql_update_putaway, {"log_id": data.log_id})

        if result.rowcount == 0:
            db.rollback()
            return {
                "success": False,
                "message": "Movimento não está cancelado ou não encontrado"
            }

        # 2) Reativa o log
        sql_update_log = text("""
            UPDATE whsproductsputawaylog
            SET
                situationregistration = 'A',
                dateregistration = NOW()
            WHERE id = :log_id
              AND situationregistration = 'E'
        """)

        db.execute(sql_update_log, {"log_id": data.log_id})

        db.commit()

        return {
            "success": True,
            "log_id": data.log_id
        }

    except Exception as e:
        db.rollback()
        return {
            "success": False,
            "error": str(e)
        }

# =======================================================================================
#
# =======================================================================================
class PutawayRequest(BaseModel):
    reference: str
    waybill: str

@cancelputway_rp.post("/putaway/reset-date-process")
def reset_date_process(
    data: PutawayRequest,
    db: Session = Depends(get_db)
):
    try:

        sql1 = text("""
        UPDATE whsproductsputaway
           SET DateProcessEnd = NULL
         WHERE `Reference` = :ref
           AND `Waybill`  = :waybill
        """)

        sql2 = text("""
        UPDATE whsproductsputawaylog
           SET DateProcessEnd = NULL
         WHERE `Reference` = :ref
           AND `Waybill`  = :waybill
        """)

        params = {
            "ref": data.reference,
            "waybill": data.waybill
        }

        r1 = db.execute(sql1, params).rowcount
        r2 = db.execute(sql2, params).rowcount

        db.commit()



        return {
            "success": True,
            "rows_affected": r1 + r2
        }

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


