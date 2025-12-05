from typing import Optional

from fastapi import FastAPI, Depends, HTTPException, APIRouter
from sqlalchemy import text
import traceback
import logging
from connection.db_connection import Base, engine, SessionLocal

consult_prod_rp = APIRouter()

logger = logging.getLogger("products")
logging.basicConfig(level=logging.INFO)


@consult_prod_rp.get("/putaway/{id}")
def get_putaway(id: int):
    db = SessionLocal()
    try:
        sql = text("""
            SELECT
                id,
                Id_whsprod,
                pn,
                User_id,
                Description,
                Position,
                Qty,
                RevisedQty,
                siccode,
                reference,
                datecreate,
                typeprint,
                CONCAT(
                    'PN: ', pn, '\n',
                    'classe: ', siccode, '\n',
                    'Position: ', Position, '\n',
                    'ref: ', reference, '\n',
                    'Date: ', datecreate
                ) AS qrcode
            FROM whsproductsputawaylog
            WHERE id = :id
        """)

        result = db.execute(sql, {"id": id}).fetchone()

        if result is None:
            raise HTTPException(status_code=404, detail="Registro não encontrado")

        return {"status": "ok", "data": dict(result._mapping)}

    except Exception as e:

        traceback.print_exc()   # <-- mostra o erro real
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        db.close()


@consult_prod_rp.get("/productsputaway")
@consult_prod_rp.get("/productsputaway/{pn}")
def products_putaway(pn: Optional[str] = None):


    db = SessionLocal()
    try:
        if pn:
            sql = text("""
                SELECT *
                FROM whsproductsputaway
                WHERE pn = :pn AND situationregistration != 'E'
            """)
            result = db.execute(sql, {"pn": pn}).fetchall()
        else:
            sql = text("""
                SELECT *
                FROM whsproductsputaway
                WHERE situationregistration != 'E'
            """)
            result = db.execute(sql).fetchall()

        if not result:
            raise HTTPException(status_code=404, detail="Registro não encontrado")

        # transforma cada linha em dict
        data = [dict(row._mapping) for row in result]

        return {"status": "ok", "data": data}

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()