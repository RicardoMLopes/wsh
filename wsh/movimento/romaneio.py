from fastapi import Depends, APIRouter, HTTPException
from pydantic import BaseModel
from typing import List
from connection.db_connection import SessionLocal
from sqlalchemy.orm import Session
from datetime import datetime
import logging, time


moviment_rp = APIRouter()

class PutawayItem(BaseModel):
    pn: str
    description: str
    referencia: str
    qtd: float
    waybill: str
    processlines: str


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ==================================================
#   ROTA PARA IMPORTAR OS DADOS ROMANEIO
#===================================================
logger = logging.getLogger("romaneio")
logging.basicConfig(level=logging.INFO)

@moviment_rp.post("/romaneio")
def putaway(items: List[PutawayItem], db: Session = Depends(get_db)):
    conn = db.connection().connection
    cursor = conn.cursor()

    total = 0
    inseridos = 0
    atualizados = 0
    ignorados = 0

    for item in items:
        total += 1
        logger.info(f"Recebido item: {item}")

        # Verifica se já existe
        cursor.execute("""
            SELECT * FROM whsproductsputaway
            WHERE Reference=%s AND Waybill=%s AND PN=%s AND situationregistration <> 'E'
        """, (item.referencia, item.waybill, item.pn))
        row = cursor.fetchone()
        logger.info(f"Resultado SELECT: {row}")

        if row:
            # Pega RevisedQty e situationregistration
            try:
                idx_qty = [desc[0] for desc in cursor.description].index("RevisedQty")
                revised_qty = row[idx_qty]
            except ValueError:
                revised_qty = None

            try:
                idx_sit = [desc[0] for desc in cursor.description].index("situationregistration")
                situation = row[idx_sit]
            except ValueError:
                situation = None

            logger.info(f"RevisedQty={revised_qty}, situation={situation}")

            # Só atualiza se ainda está como inserido
            if revised_qty == 0 and situation == 'I':
                logger.info("Executando UPDATE...")
                cursor.execute("""
                    UPDATE whsproductsputaway
                    SET Qty=%s, Description=%s, processlines=%s, situationregistration='A', dateregistration=%s
                    WHERE Reference=%s AND Waybill=%s AND PN=%s
                """, (item.qtd, item.description, item.processlines, datetime.now(),
                      item.referencia, item.waybill, item.pn))
                atualizados += 1
            else:
                ignorados += 1
        else:
            # Faz INSERT
            logger.info("Nenhum registro encontrado, executando INSERT...")
            cursor.execute("SELECT MAX(Id) FROM whsproductsputaway")
            max_id = cursor.fetchone()[0] or 0
            new_id = max_id + 1
            logger.info(f"Novo Id calculado: {new_id}")

            cursor.execute("""
                INSERT INTO whsproductsputaway
                (Id, User_id, PN, Description, Reference, Qty, Waybill, processlines, datecreate, situationregistration, dateregistration)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (new_id, 0, item.pn, item.description, item.referencia, item.qtd,
                  item.waybill, item.processlines, datetime.now(), 'I', datetime.now()))
            inseridos += 1

    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Operação concluída com sucesso.")

    return {
        "status": "ok",
        "total": total,
        "inseridos": inseridos,
        "atualizados": atualizados,
        "ignorados": ignorados
    }

@moviment_rp.post("/atualiza-posicao")
def atualiza_posicao(db: Session = Depends(get_db)):
    conn = db.connection().connection
    cursor = conn.cursor()

    # Atualiza Position
    cursor.execute("""
        UPDATE whsproductsputaway
        INNER JOIN whsproducts ON whsproductsputaway.PN = whsproducts.PN
        SET whsproductsputaway.Position = whsproducts.Position
        WHERE (whsproductsputaway.Position = '' OR whsproductsputaway.Position IS NULL)
          AND (whsproducts.Position <> '' AND whsproducts.Position IS NOT NULL)
    """)
    pos_atualizados = cursor.rowcount

    # Atualiza Siccode
    cursor.execute("""
        UPDATE whsproductsputaway
        INNER JOIN whsproducts ON whsproductsputaway.PN = whsproducts.PN
        SET whsproductsputaway.siccode = whsproducts.siccode
        WHERE (whsproductsputaway.siccode = '' OR whsproductsputaway.siccode IS NULL)
          AND (whsproducts.siccode <> '' AND whsproducts.siccode IS NOT NULL)
    """)
    sic_atualizados = cursor.rowcount

    conn.commit()
    cursor.close()
    conn.close()

    return {
        "status": "ok",
        "posicoes_atualizadas": pos_atualizados,
        "siccodes_atualizados": sic_atualizados
    }


@moviment_rp.get("/tarefas")
def listar_tarefas(
    referencia: str = None,
    waybill: str = None,
    pn: str = None,
    operador: str = None,
    emergencial: bool = False,
    grn1: str = None,
    grn3: str = None,
    processdate: str = None,
    aaf: str = None,
    rnc: str = None,
    grn: str = None,
    db: Session = Depends(get_db)
):
    conn = db.connection().connection
    cursor = conn.cursor()

    sql = """
        SELECT Reference, Waybill, operator_id,
               MAX(grn1) AS _grn1,
               COUNT(reference) AS _linhas,
               MAX(processlines) AS _processlines,
               MAX(processdate) AS _processdate,
               MAX(grn) AS _grn,
               MAX(grn3) AS _grn3,
               MAX(aaf) AS _aaf,
               MAX(rnc) AS _rnc,
               MIN(datecreate) AS _dCreat,
               MAX(DateProcessStart) AS _DateProcessStart,
               MAX(DateProcessEnd) AS _DateProcessEnd,
               MIN(Criticality) AS _Criticality
        FROM whsproductsputaway
        WHERE situationregistration <> 'E'
    """

    params = []

    if referencia:
        sql += " AND Reference=%s"
        params.append(referencia)
    if waybill:
        sql += " AND Waybill=%s"
        params.append(waybill)
    if pn:
        sql += " AND PN=%s"
        params.append(pn)
    if operador:
        sql += " AND operator_id LIKE %s"
        params.append(f"%{operador}%")
    if emergencial:
        sql += " AND LEFT(Criticality,1)='E'"
    if grn1:
        sql += " AND GRN1=%s"
        params.append(grn1)
    if grn3:
        sql += " AND GRN3=%s"
        params.append(grn3)
    if processdate:
        sql += " AND processdate=%s"
        params.append(processdate)
    if aaf:
        sql += " AND AAF=%s"
        params.append(aaf)
    if rnc:
        sql += " AND RNC=%s"
        params.append(rnc)
    if grn:
        sql += " AND GRN=%s"
        params.append(grn)

    sql += " GROUP BY Reference, Waybill, operator_id, IFNULL(grn1,1), grn1"
    sql += " ORDER BY IFNULL(grn1,1), grn1, MIN(datecreate), Reference"

    cursor.execute(sql, tuple(params))
    rows = cursor.fetchall()

    # monta lista de dicionários
    colunas = [desc[0] for desc in cursor.description]
    resultado = [dict(zip(colunas, row)) for row in rows]

    cursor.close()
    conn.close()

    return resultado

#======================================================================================
#   IMPORTAÇÃO aurora071
#---------------------------------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

import logging, time

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def executar_sql_em_lotes(cursor, conn, sql_template, resultados, chave, lote=10000):
    total_afetados = 0
    lote_num = 1
    while True:
        sql = sql_template.format(lote=lote)
        try:
            logging.info("Iniciando [%s] lote %d...", chave, lote_num)
            start = time.time()
            afetados = cursor.execute(sql)
            conn.commit()
            duration = time.time() - start
            logging.info("Finalizado [%s] lote %d: %s registros afetados em %.2f segundos",
                         chave, lote_num, afetados, duration)
            total_afetados += afetados
            if afetados == 0:
                break
            lote_num += 1
        except Exception as e:
            conn.rollback()
            resultados[chave] = f"erro: {str(e)}"
            logging.error("Erro ao executar [%s] lote %d: %s", chave, lote_num, e)
            break
    resultados[chave] = total_afetados

@moviment_rp.post("/aurora071/process")
def processar_aurora071(
    update_geral: bool = False,
    grn_log: bool = False,
    db: Session = Depends(get_db)
):
    conn = db.connection().connection
    cursor = conn.cursor()
    resultados = {}

    try:
        # 🔹 Atualização GRN1
        condicao = "" if update_geral else "AND (p.grn1='' OR p.grn1 IS NULL)"
        sql_template = f"""
            UPDATE whsproductsputaway p
            JOIN (
                SELECT p.Id
                FROM whsproductsputaway p
                INNER JOIN whsaurora071 a
                ON p.reference = SUBSTRING(a.FileRef,1,6)
                AND p.PN = a.Item
                WHERE (p.grn1 IS NULL OR p.grn1 <> a.TXIssuedate)
                {condicao}
                LIMIT {{lote}}
            ) AS t ON p.Id = t.Id
            INNER JOIN whsaurora071 a
            ON p.reference = SUBSTRING(a.FileRef,1,6)
            AND p.PN = a.Item
            SET p.grn1 = a.TXIssuedate
        """
        executar_sql_em_lotes(cursor, conn, sql_template, resultados, "grn1")

        # 🔹 Atualização GRN3
        condicao = "" if update_geral else "AND (p.grn3='' OR p.grn3 IS NULL)"
        sql_template = f"""
            UPDATE whsproductsputaway p
            JOIN (
                SELECT p.Id
                FROM whsproductsputaway p
                INNER JOIN whsaurora071 a
                ON p.reference = SUBSTRING(a.FileRef,1,6)
                AND p.PN = a.Item
                WHERE (p.grn3 IS NULL OR p.grn3 <> a.Receiptdate)
                {condicao}
                LIMIT {{lote}}
            ) AS t ON p.Id = t.Id
            INNER JOIN whsaurora071 a
            ON p.reference = SUBSTRING(a.FileRef,1,6)
            AND p.PN = a.Item
            SET p.grn3 = a.Receiptdate
        """
        executar_sql_em_lotes(cursor, conn, sql_template, resultados, "grn3")

        # 🔹 Atualização GRN (usando StockGoodsInwards)
        condicao = "" if update_geral else "AND (p.GRN='' OR p.GRN IS NULL)"
        sql_template = f"""
            UPDATE whsproductsputaway p
            JOIN (
                SELECT p.Id
                FROM whsproductsputaway p
                INNER JOIN whsaurora071 a
                ON p.reference = SUBSTRING(a.FileRef,1,6)
                AND p.PN = a.Item
                WHERE (p.GRN IS NULL OR p.GRN <> a.StockGoodsInwards)
                {condicao}
                LIMIT {{lote}}
            ) AS t ON p.Id = t.Id
            INNER JOIN whsaurora071 a
            ON p.reference = SUBSTRING(a.FileRef,1,6)
            AND p.PN = a.Item
            SET p.GRN = a.StockGoodsInwards
        """
        executar_sql_em_lotes(cursor, conn, sql_template, resultados, "grn")

        # 🔹 Atualizações de log
        if grn_log:
            for campo, coluna in [("grn1", "TXIssuedate"), ("grn3", "Receiptdate"), ("GRN", "StockGoodsInwards")]:
                condicao = "" if update_geral else f"AND (l.{campo}='' OR l.{campo} IS NULL)"
                sql_template = f"""
                    UPDATE whsproductsputawaylog l
                    JOIN (
                        SELECT l.Id
                        FROM whsproductsputawaylog l
                        INNER JOIN whsaurora071 a
                        ON l.reference = SUBSTRING(a.FileRef,1,6)
                        AND l.PN = a.Item
                        WHERE (l.{campo} IS NULL OR l.{campo} <> a.{coluna})
                        {condicao}
                        LIMIT {{lote}}
                    ) AS t ON l.Id = t.Id
                    INNER JOIN whsaurora071 a
                    ON l.reference = SUBSTRING(a.FileRef,1,6)
                    AND l.PN = a.Item
                    SET l.{campo} = a.{coluna}
                """
                executar_sql_em_lotes(cursor, conn, sql_template, resultados, f"log_{campo}")

    finally:
        cursor.close()
        conn.close()

    return {"status": "ok", "resultados": resultados}

#========================================================================================================
#
#--------------------------------------------------------------------------------------------------------
def executar_sql(cursor, conn, sql, resultados, chave):
    try:
        logging.info("Iniciando [%s]...", chave)
        start = time.time()
        cursor.execute(sql)
        conn.commit()
        duration = time.time() - start
        resultados[chave] = cursor.rowcount
        logging.info("Finalizado [%s]: %s registros afetados em %.2f segundos",
                     chave, cursor.rowcount, duration)
    except Exception as e:
        conn.rollback()
        resultados[chave] = f"erro: {str(e)}"
        logging.error("Erro ao executar [%s]: %s", chave, e)

@moviment_rp.post("/auroraAAF/process")
def processar_auroraAAF(
    update_geral: bool = False,
    aaf_log: bool = False,
    aaf_tela: bool = False,
    linhas: list[dict] = None,   # se vier da tela, cada linha tem {reference, aaf, criticality}
    db: Session = Depends(get_db)
):
    conn = db.connection().connection
    cursor = conn.cursor()
    resultados = {}

    try:
        if aaf_tela and linhas:
            # 🔹 Atualização linha a linha (modo tela)
            for idx, linha in enumerate(linhas, start=1):
                if linha.get("aaf"):
                    condicao = "" if update_geral else " AND (whsproductsputaway.aaf='' OR whsproductsputaway.aaf IS NULL)"
                    sql = f"""
                        UPDATE whsproductsputaway
                        SET aaf = '{linha["aaf"]}',
                            Criticality = '{linha.get("criticality","")}'
                        WHERE reference = '{linha["reference"]}'
                        {condicao}
                    """
                    executar_sql(cursor, conn, sql, resultados, f"aaf_tela_{idx}")

                    if aaf_log:
                        condicao = "" if update_geral else " AND (whsproductsputawaylog.aaf='' OR whsproductsputawaylog.aaf IS NULL)"
                        sql = f"""
                            UPDATE whsproductsputawaylog
                            SET aaf = '{linha["aaf"]}',
                                Criticality = '{linha.get("criticality","")}'
                            WHERE reference = '{linha["reference"]}'
                            {condicao}
                        """
                        executar_sql(cursor, conn, sql, resultados, f"log_aaf_tela_{idx}")

        else:
            # 🔹 Atualização em massa (JOIN com whsauroraAAF)
            condicao = "" if update_geral else " WHERE (whsproductsputaway.aaf='' OR whsproductsputaway.aaf IS NULL)"
            sql = f"""
                UPDATE whsproductsputaway
                INNER JOIN whsauroraAAF
                ON whsproductsputaway.reference = whsauroraAAF.reference
                SET whsproductsputaway.aaf = whsauroraAAF.aaf,
                    whsproductsputaway.Criticality = whsauroraAAF.Criticality
                {condicao}
            """
            executar_sql(cursor, conn, sql, resultados, "aaf_mass")

            if aaf_log:
                condicao = "" if update_geral else " WHERE (whsproductsputawaylog.aaf='' OR whsproductsputawaylog.aaf IS NULL)"
                sql = f"""
                    UPDATE whsproductsputawaylog
                    INNER JOIN whsauroraAAF
                    ON whsproductsputawaylog.reference = whsauroraAAF.reference
                    SET whsproductsputawaylog.aaf = whsauroraAAF.aaf,
                        whsproductsputawaylog.Criticality = whsauroraAAF.Criticality
                    {condicao}
                """
                executar_sql(cursor, conn, sql, resultados, "log_aaf_mass")

    finally:
        cursor.close()
        conn.close()

    return {"status": "ok", "resultados": resultados}

@moviment_rp.post("/tarefas/atribuir-operador")
def atribuir_operador(
    reference: str,
    waybill: str,
    operador: str,
    db: Session = Depends(get_db)
):
    conn = db.connection().connection
    cursor = conn.cursor()
    resultados = {}

    try:
        # Verifica se existe registro válido
        sql_select = f"""
            SELECT COUNT(*) 
            FROM whsproductsputaway
            WHERE Reference = '{reference}'
              AND Waybill = '{waybill}'
              AND situationregistration <> 'E'
        """
        cursor.execute(sql_select)
        count = cursor.fetchone()[0]

        if count > 0:
            sql_update = f"""
                UPDATE whsproductsputaway
                SET operator_id = '{operador}'
                WHERE Reference = '{reference}'
                  AND Waybill = '{waybill}'
            """
            cursor.execute(sql_update)
            conn.commit()
            resultados = {"atualizados": cursor.rowcount}
        else:
            resultados = {"atualizados": 0, "mensagem": "Nenhum registro encontrado"}
    finally:
        cursor.close()
        conn.close()

    return {"status": "ok", "resultados": resultados}

