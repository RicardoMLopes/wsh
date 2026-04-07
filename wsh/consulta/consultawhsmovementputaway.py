from typing import Optional

from fastapi import FastAPI, Depends, HTTPException, APIRouter
from fastapi.encoders import jsonable_encoder
from sqlalchemy import text
import traceback
import logging
from connection.db_connection import Base, engine, SessionLocal

consult_mov_putaway = APIRouter()



@consult_mov_putaway.get("/consultmovputaway")
@consult_mov_putaway.get("/consultmovputaway/{pn}")
def movement_putaway(
    pn: Optional[str] = None,
    user_id: Optional[int] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    id: Optional[int] = None,
    position: Optional[str] = None,
    reference: Optional[str] = None,
):
    db = SessionLocal()
    try:
        base_sql = """
            SELECT A.*, B.users
            FROM whsmovementputaway A
            LEFT JOIN caduser B ON B.id = A.user_id
            WHERE A.situationregistration != 'E'
        """

        params = {}

        if id:
            base_sql += " AND A.id = :id"
            params["id"] = id

        if pn:
            base_sql += " AND A.pn = :pn"
            params["pn"] = pn

        if reference:
            base_sql += " AND A.reference = :reference"
            params["reference"] = reference

        if position:
            base_sql += " AND A.position LIKE :position"
            params["position"] = f"%{position}%"

        if user_id:
            base_sql += " AND B.id = :user_id"
            params["user_id"] = user_id

        if date_from:
            base_sql += " AND A.dateregistration >= :date_from"
            params["date_from"] = date_from

        if date_to:
            base_sql += " AND A.dateregistration <= :date_to"
            params["date_to"] = date_to

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