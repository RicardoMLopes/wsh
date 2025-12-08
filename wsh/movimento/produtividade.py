from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
import logging
from connection.db_connection import SessionLocal

produtividade_rp = APIRouter()

logger = logging.getLogger("produtividade")
logging.basicConfig(level=logging.INFO)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@produtividade_rp.get("/produtividade")
def get_produtividade(
    dataini: str,
    datafim: str,
    processend: int = 0,
    hora: int = 0,
    ordenacao: int = 0,       # 0 = agrupar por operador, 1 = agrupar por hora
    db: Session = Depends(get_db)
):

    logger.info("⏳ Montando SQL produtividade...")

    sql = """
        SELECT User_id,
               COUNT(id) AS Linhas,
               SUM(RevisedQty) AS Pecas
    """

    if hora == 1:
        sql += ", HOUR(dateregistration) AS Hora "

    sql += """
        FROM whsproductsputawaylog
        WHERE datecreate BETWEEN :dataini AND :datafim
          AND situationregistration <> 'E'
    """

    if processend == 1:
        sql += " AND DateProcessEnd IS NOT NULL "

    # GROUP BY
    if hora == 1:
        sql += " GROUP BY HOUR(dateregistration), User_id "
    else:
        sql += " GROUP BY User_id "

    # ORDER BY
    if hora == 0:
        sql += " ORDER BY User_id "
    else:
        if ordenacao == 0:
            sql += " ORDER BY User_id, HOUR(dateregistration) "
        else:
            sql += " ORDER BY HOUR(dateregistration), User_id "

    logger.info(f"🔍 SQL Gerado Produtividade: {sql}")

    result = db.execute(
        text(sql),
        {"dataini": dataini, "datafim": datafim}
    ).fetchall()

    dados = [dict(r._mapping) for r in result]

    logger.info(f"🚀 Registros encontrados: {len(dados)}")

    return {
        "success": True,
        "sql": sql,
        "data": dados
    }
