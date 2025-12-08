from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session
import logging
from datetime import datetime
from connection.db_connection import SessionLocal

listagem_rp = APIRouter()

# Logger configurado
logger = logging.getLogger("listageral")
logging.basicConfig(level=logging.INFO)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@listagem_rp.get("/listageral")
def get_listageral(
    tipo: str,
    controle: str = "",
    waybill: str = "",
    codigoitem: str = "",
    situacao: int = 0,
    status: int = 0,
    processend: int = 0,
    operador: str = "",
    filtro_data: int = 0,
    dataini: str = "",
    datafim: str = "",
    ordem: int = 0,
    db: Session = Depends(get_db)
):

    logger.info("📌 Iniciando rota /listageral")

    try:
        sql = " SELECT * FROM "
        sql += "whsproductsputaway " if tipo == "G" else "whsproductsputawaylog "
        sql += " WHERE Id > 0 "

        # filtros
        if controle:
            sql += f" AND Reference = '{controle}'"

        if waybill:
            sql += f" AND Waybill = '{waybill}'"

        if codigoitem:
            sql += f" AND PN = '{codigoitem}'"

        # situação do registro
        if situacao == 0:
            sql += " AND situationregistration <> 'E' "
        elif situacao == 1:
            sql += " AND situationregistration = 'E' "

        # operador
        if operador and operador.strip() != "":
            sql += f" AND User_Id = '{operador}'"

        # ORDER
        order_fields = [
            "",
            "Id",
            "User_id",
            "PN",
            "Description",
            "Qty",
            "RevisedQty",
            "Position",
            "dateregistration",
            "siccode",
            "Reference",
            "Waybill",
            "operator_id",
            "processlines",
            "AAF",
            "grn1",
            "grn",
            "GRN3",
            "RNC",
            "processdate"
        ]

        if ordem > 0:
            sql += f" ORDER BY {order_fields[ordem]}"

        logger.info(f"🟦 SQL Final Gerado:\n{sql}")

        result = db.execute(text(sql)).fetchall()

        return {
            "success": True,
            "sql": sql,
            "data": [dict(r._mapping) for r in result]
        }

    except Exception as e:
        logger.error(f"❌ ERRO NA CONSULTA /listageral: {e}")
        return {
            "success": False,
            "error": str(e),
            "data": []
        }
