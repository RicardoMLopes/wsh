from typing import Optional

from fastapi import FastAPI, Depends, HTTPException, APIRouter
from fastapi.encoders import jsonable_encoder
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
def products_putaway(
    pn: Optional[str] = None,
    position: Optional[str] = None,
    id: Optional[int] = None
):
    db = SessionLocal()
    try:
        base_sql = """
            SELECT *
            FROM whsproductsputaway
            WHERE situationregistration != 'E'
        """

        params = {}

        if id:
            base_sql += " AND id = :id"
            params["id"] = id

        if pn:
            base_sql += " AND pn = :pn"
            params["pn"] = pn

        if position:
            base_sql += " AND Position LIKE :position"
            params["position"] = f"%{position}%"

        result = db.execute(text(base_sql), params).fetchall()

        data = [dict(row._mapping) for row in result]

        return jsonable_encoder({
            "status": "ok",
            "data": data
        })

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        db.close()

