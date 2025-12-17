from fastapi import FastAPI, Depends, HTTPException, APIRouter
from sqlalchemy.orm import Session
from typing import Optional
import logging
from connection.db_connection import Base, engine, SessionLocal
from pydantic import BaseModel

consults_rp = APIRouter()

logger = logging.getLogger("consultas")
logging.basicConfig(level=logging.INFO)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@consults_rp.get("/consulta/item")
def consulta_item(
    pn: str,
    reference: str,
    waybill: str,
    mostrar_totais: bool = False,
    db: Session = Depends(get_db)
):
    conn = db.connection().connection
    cursor = conn.cursor()
    resultado = {"putaway": None, "products": None, "totais": None}
    logger.info(f"Resultado SELECT: {resultado}")

    try:
        # Consulta whsproductsputaway
        sql_putaway = f"""
            SELECT id, Description, Position, siccode, Qty, processlines,
                   RevisedQty, StandardQty, LPSQty, UndeclaredSQty, BreakdownQty
            FROM whsproductsputaway
            WHERE PN = '{pn}' AND Reference = '{reference}' AND Waybill = '{waybill}'
              AND situationregistration <> 'E'
        """
        logger.info(f"Resultado SELECT: {sql_putaway}")
        cursor.execute(sql_putaway)
        row = cursor.fetchone()
        if row:
            resultado["putaway"] = {
                "id": row[0],
                "description": row[1],
                "position": row[2],
                "siccode": row[3],
                "qty": row[4],
                "processlines": row[5],
                "revisedqty": row[6],
                "standardqty": row[7],
                "lpsqty": row[8],
                "undeclaredsqty": row[9],
                "breakdownqty": row[10],
            }

        # Consulta whsproducts
        sql_products = f"""
            SELECT Description, Position, siccode
            FROM whsproducts
            WHERE PN = '{pn}' AND situationregistration <> 'E'
            order by id desc 
        """
        cursor.execute(sql_products)
        row = cursor.fetchone()
        if row:
            resultado["products"] = {
                "description": row[0],
                "position": row[1],
                "siccode": row[2],
            }

        # Consulta totais no log
        if mostrar_totais and not resultado["putaway"]:
            sql_totais = f"""
                SELECT SUM(StandardQty) AS SQty,
                       SUM(LPSQty) AS LQty,
                       SUM(UndeclaredSQty) AS UQty,
                       SUM(BreakdownQty) AS BQty
                FROM whsproductsputawaylog
                WHERE Reference = '{reference}' AND Waybill = '{waybill}' AND PN = '{pn}'
                  AND RevisedQty > 0 AND situationregistration <> 'E'
            """
            cursor.execute(sql_totais)
            row = cursor.fetchone()

            logger.info(f"totais: {row}")
            if row:
                resultado["totais"] = {
                    "standardqty": row[0],
                    "lpsqty": row[1],
                    "undeclaredsqty": row[2],
                    "breakdownqty": row[3],
                }

    finally:
        cursor.close()
        conn.close()

    return {"status": "ok", "resultado": resultado}

#===================================================================
# CONSULTA PARA IMPRESSÃO
#-------------------------------------------------------------------
@consults_rp.get("/consulta/etiquetas")
def consulta_etiquetas(
    pn: Optional[str] = None,
    id_whsprod: Optional[int] = None,
    db: Session = Depends(get_db)
):
    logger.info(">>> Iniciando rotina consulta_etiquetas")
    logger.debug(f"Parâmetros recebidos: pn={pn}, id_whsprod={id_whsprod}")

    conn = db.connection().connection
    cursor = conn.cursor()
    try:
        sql = """
            SELECT id, Id_whsprod, pn, User_id, Description, Position, Qty, RevisedQty,
                   siccode, reference, breakdownQty, datecreate, typeprint, print,
                   CONCAT('PN: ', pn, '\nclasse: ', siccode, '\nPosition: ', position,
                          '\nref: ', reference, '\nDate: ', datecreate) AS qrcode
            FROM whsproductsputawaylog
        """
        if pn:
            sql += f" WHERE PN = '{pn}'"
            logger.info(f"Filtro aplicado: PN = {pn}")
        elif id_whsprod:
            sql += f" WHERE Id_whsprod = {id_whsprod}"
            logger.info(f"Filtro aplicado: Id_whsprod = {id_whsprod}")

        sql += " ORDER BY datecreate DESC, CASE typeprint WHEN 'N' THEN 1 WHEN 'L' THEN 2 WHEN 'F' THEN 3 ELSE 4 END"

        try:
            logger.info(f"Executando SELECT: {sql}")
            cursor.execute(sql)
            rows = cursor.fetchall()
            logger.info(f"SELECT executado com sucesso. Linhas retornadas: {len(rows)}")
        except Exception as e:
            logger.error("Erro ao executar SELECT")
            logger.exception(e)
            return {"status": "erro", "mensagem": f"Erro no SELECT: {str(e)}"}

        try:
            colunas = [desc[0] for desc in cursor.description]
            resultado = [dict(zip(colunas, row)) for row in rows]
            logger.debug(f"Colunas retornadas: {colunas}")
            logger.info("Transformação dos resultados em dicionário concluída")
        except Exception as e:
            logger.error("Erro ao processar resultados do SELECT")
            logger.exception(e)
            return {"status": "erro", "mensagem": f"Erro ao processar resultados: {str(e)}"}

        logger.info(">>> Finalizando rotina consulta_etiquetas com sucesso")
        return {"status": "ok", "resultado": resultado}

    except Exception as e:
        logger.error("Erro inesperado na rotina consulta_etiquetas")
        logger.exception(e)
        return {"status": "erro", "mensagem": str(e)}

    finally:
        logger.debug("Fechando cursor e conexão")
        cursor.close()
        conn.close()
