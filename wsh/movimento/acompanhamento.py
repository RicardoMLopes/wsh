from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
import logging
from connection.db_connection import SessionLocal

acompanhamento_rp = APIRouter()

logger = logging.getLogger("acompanhamento")
logging.basicConfig(level=logging.INFO)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@acompanhamento_rp.get("/acompanhamento")
def get_acompanhamento(
    dataini: str,
    datafim: str,
    processend: int = 0,
    hora: int = 0,
    ordenacao: int = 0,
    db: Session = Depends(get_db)
):

    logger.info("⏳ Montando SQL acompanhamento...")

    sql = """
        SELECT 
            MIN(Rom.datecreate) AS DtCreat,
            Rom.Reference,
            COUNT(Rom.id) AS LinRec,
            Rom.aaf,
            Rom.grn1,
            Rom.GRN,
            Rom.grn3,
            Log.User_id,
            COUNT(Log.id) AS Linhas,
            CAST(Log.dateregistration AS DATE) AS DtLanc,
            MIN(Rom.DateProcessStart) AS DtIni,
            MAX(Rom.DateProcessEnd) AS DtFim,
            Rom.processlines,
            Rom.rnc
        FROM whsproductsputawaylog AS Log
        INNER JOIN whsproductsputaway Rom ON
            Rom.Reference = Log.Reference AND
            Rom.Waybill   = Log.Waybill   AND
            Rom.PN        = Log.PN
        WHERE Rom.datecreate BETWEEN :dataini AND :datafim
    """

    if processend == 1:
        sql += " AND Rom.DateProcessEnd IS NOT NULL "

    sql += """
        GROUP BY 
            CAST(Rom.datecreate AS DATE),
            Rom.Reference,
            Rom.aaf,
            Rom.grn1,
            Rom.GRN,
            Rom.grn3,
            Log.User_id,
            CAST(Log.dateregistration AS DATE),
            Rom.processlines,
            Rom.rnc
    """

    # 🔽 ORDENACAO
    if ordenacao == 0:
        sql += " ORDER BY DtLanc, Log.User_id "
    else:
        sql += " ORDER BY Log.User_id, DtLanc "

    logger.info(f"🔍 SQL Gerado Acompanhamento: {sql}")

    # Executa query
    result = db.execute(
        text(sql),
        {"dataini": dataini, "datafim": datafim}
    ).fetchall()

    dados = [dict(r._mapping) for r in result]

    logger.info(f"🚀 Registros encontrados: {len(dados)}")

    return {
        "success": True,
        "data": dados
    }
