from fastapi import Depends, APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
from typing import List
from connection.db_connection import SessionLocal
from sqlalchemy.orm import Session
from datetime import datetime
import logging, time
from sqlalchemy import text
from datetime import date

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

            # logger.info(f"RevisedQty={revised_qty}, situation={situation}")

            # Só atualiza se ainda está como inserido
            if revised_qty == 0 and situation == 'I':
                # logger.info("Executando UPDATE...")
                cursor.execute("""
                    UPDATE whsproductsputaway
                    SET Qty=%s, Description=%s, processlines=%s, inputtype='import', situationregistration='A', dateregistration=%s
                    WHERE Reference=%s AND Waybill=%s AND PN=%s
                """, (item.qtd, item.description, item.processlines, datetime.now(),
                      item.referencia, item.waybill, item.pn))
                atualizados += 1
            else:
                ignorados += 1
        else:
            # Faz INSERT
            # logger.info("Nenhum registro encontrado, executando INSERT...")
            cursor.execute("SELECT MAX(Id) FROM whsproductsputaway")
            max_id = cursor.fetchone()[0] or 0
            new_id = max_id + 1
            # logger.info(f"Novo Id calculado: {new_id}")

            cursor.execute("""
                INSERT INTO whsproductsputaway
                (Id, User_id, PN, Description, Reference, Qty, Waybill, processlines, datecreate, inputtype, situationregistration, dateregistration)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (new_id, 0, item.pn, item.description, item.referencia, item.qtd,
                  item.waybill, item.processlines, datetime.now(), 'import', 'I', datetime.now()))
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

#-------------=====================------------------------------------------
#                  MOVIMENTO TAREFA
#============================================================================
def normaliza_data(valor):
    if valor is None:
        return None
    if isinstance(valor, str) and valor.upper() == "NULL":
        return None
    return valor


@moviment_rp.get("/tarefas")
def listar_tarefas(
    referencia: str = None,
    waybill: str = None,
    pn: str = None,
    operador: str = None,
    emergencial: bool = False,

    # novos filtros
    data_tipo: str = None,   # datecreate | start | end | aaf | grn | grn3
    data_ini: str = None,    # YYYY-MM-DD
    data_fim: str = None,    # YYYY-MM-DD
    ordenacao: int = 0,      # 0 | 1 | 2

    grn1: str = None,
    grn3: str = None,
    processdate: str = None,
    aaf: str = None,
    rnc: str = None,
    grn: str = None,

    db: Session = Depends(get_db)
):
    # logger.info("===== INICIO /tarefas =====")
    # logger.info(
    #     f"Parâmetros recebidos: referencia={referencia}, waybill={waybill}, pn={pn}, "
    #     f"operador={operador}, emergencial={emergencial}, "
    #     f"data_tipo={data_tipo}, data_ini={data_ini}, data_fim={data_fim}, "
    #     f"ordenacao={ordenacao}"
    # )

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
               MIN(DateProcessStart) AS _DateProcessStart,
               MAX(DateProcessEnd) AS _DateProcessEnd,
               MIN(Criticality) AS _Criticality
        FROM whsproductsputaway
        WHERE situationregistration <> 'E'
    """

    params = []

    # filtros básicos
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

    data_ini = normaliza_data(data_ini)
    data_fim = normaliza_data(data_fim)


    # filtro de período (igual Delphi)
    if data_tipo and data_ini and data_fim:
        if data_tipo == "datecreate":
            sql += " AND datecreate BETWEEN %s AND %s"
        elif data_tipo == "start":
            sql += " AND DateProcessStart BETWEEN %s AND %s"
        elif data_tipo == "end":
            sql += " AND DateProcessEnd BETWEEN %s AND %s"
        elif data_tipo == "aaf":
            sql += " AND aaf BETWEEN %s AND %s"
        elif data_tipo == "grn":
            sql += " AND grn BETWEEN %s AND %s"
        elif data_tipo == "grn3":
            sql += " AND grn3 BETWEEN %s AND %s"

        params.extend([data_ini, data_fim])

    # group by
    sql += " GROUP BY Reference, Waybill, operator_id"
    # , IFNULL(grn1, 1), grn1 | Tirou porque não esta trazendo, foi implementado no access 09/01/2026

    # ordenação
    if ordenacao == 0:
        sql += " ORDER BY _DateProcessStart"
    elif ordenacao == 1:
        sql += " ORDER BY _DateProcessStart IS NULL, _dCreat ASC"
    elif ordenacao == 2:
        sql += " ORDER BY _DateProcessEnd IS NULL, _dCreat ASC"

    # ===================== LOGS IMPORTANTES =====================
    # logger.info("SQL FINAL (com placeholders):")
    # logger.info(sql)
    #
    # logger.info("PARAMETROS:")
    # logger.info(params)

    # SQL apenas para visualização (NÃO usar em produção)
    sql_debug = sql
    for p in params:
        sql_debug = sql_debug.replace("%s", f"'{p}'", 1)

    # logger.info("SQL DEBUG (para copiar e colar no banco):")
    logger.info(sql_debug)
    # ============================================================

    cursor.execute(sql, tuple(params))
    rows = cursor.fetchall()

 #  logger.info(f"Total de registros retornados: {len(rows)}")

    colunas = [desc[0] for desc in cursor.description]
    resultado = [dict(zip(colunas, row)) for row in rows]

    cursor.close()
    conn.close()

 #   logger.info("===== FIM /tarefas =====")
    return resultado

#======================================================================================
#   IMPORTAÇÃO aurora071
#--------------------------------------------------------------------------------------

def executar_sql_em_lotes(cursor, conn, sql_template, resultados, chave, lote=5000, lote_min=500):
    total_afetados = 0
    lote_num = 1

    while True:
        sql = sql_template.format(lote=lote)
        try:
            start = time.time()
            afetados = cursor.execute(sql)
            conn.commit()
            duration = time.time() - start

            total_afetados += afetados
            print(f"Lote {lote_num} com {lote} registros: {afetados} afetados em {duration:.2f}s")

            if afetados == 0:
                break
            lote_num += 1

        except Exception as e:
            conn.rollback()
            print(f"Erro no lote {lote_num} com {lote} registros: {e}")

            # Se ainda dá para reduzir o lote, tenta novamente
            if lote > lote_min:
                lote = max(lote // 2, lote_min)
                print(f"Reduzindo lote para {lote} e tentando novamente...")
                continue
            else:
                resultados[chave] = f"erro: {str(e)}"
                break

    resultados[chave] = total_afetados

#=============================================================================================
#                 MOVIMENTO aurora071
#=============================================================================================
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
        logger.info("➡️ Iniciando atualização GRN1...")
        condicao = "" if update_geral else "AND (p.grn1='' OR p.grn1 IS NULL)"
        sql_template = f"""
            UPDATE whsproductsputaway p
            JOIN (
                SELECT p.Id
                FROM whsproductsputaway p
                INNER JOIN whsaurora071 a
                ON p.reference = a.FileRefPrefix
                AND p.PN = a.Item
                WHERE (p.grn1 IS NULL OR p.grn1 <> a.TXIssuedate)
                {condicao}
                LIMIT {{lote}}
            ) AS t ON p.Id = t.Id
            INNER JOIN whsaurora071 a
            ON p.reference = a.FileRefPrefix
            AND p.PN = a.Item
            SET p.grn1 = a.TXIssuedate
        """
        executar_sql_em_lotes(cursor, conn, sql_template, resultados, "grn1")
        logger.info("✅ Atualização GRN1 concluída.")

        # 🔹 Atualização GRN3
        logger.info("➡️ Iniciando atualização GRN3...")
        condicao = "" if update_geral else "AND (p.grn3='' OR p.grn3 IS NULL)"
        sql_template = f"""
            UPDATE whsproductsputaway p
            JOIN (
                SELECT p.Id
                FROM whsproductsputaway p
                INNER JOIN whsaurora071 a
                ON p.reference = a.FileRefPrefix
                AND p.PN = a.Item
                WHERE (StockGoodsInwards = 'S') AND (p.grn3 IS NULL OR p.grn3 <> a.Receiptdate)
                {condicao}
                LIMIT {{lote}}
            ) AS t ON p.Id = t.Id
            INNER JOIN whsaurora071 a
            ON p.reference = a.FileRefPrefix
            AND p.PN = a.Item
            SET p.grn3 = a.Receiptdate
        """
        executar_sql_em_lotes(cursor, conn, sql_template, resultados, "grn3")
        logger.info("✅ Atualização GRN3 concluída.")

        # 🔹 Atualização GRN
        logger.info("➡️ Iniciando atualização GRN...")
        condicao = "" if update_geral else "AND (p.GRN='' OR p.GRN IS NULL)"
        sql_template = f"""
            UPDATE whsproductsputaway p
                JOIN (
                    SELECT DISTINCT p.Id
                    FROM whsproductsputaway p
                    INNER JOIN whsaurora071 a
                      ON p.reference = a.FileRefPrefix
                     AND p.PN = a.Item
                     AND a.StockGoodsInwards in('G', 'S')
                    WHERE (p.GRN IS NULL OR p.GRN <> a.GRNNo)
                    {condicao}
                    LIMIT {{lote}}
                ) AS t ON p.Id = t.Id
                INNER JOIN whsaurora071 a
                  ON p.reference = a.FileRefPrefix
                 AND p.PN = a.Item
                 AND a.StockGoodsInwards in('G', 'S')
                SET p.GRN = a.GRNNo
        """
        executar_sql_em_lotes(cursor, conn, sql_template, resultados, "grn")
        logger.info("✅ Atualização GRN concluída.")

        # 🔹 Atualizações de log
        if grn_log:
            for campo, coluna in [("grn1", "TXIssuedate"), ("grn3", "Receiptdate"), ("GRN", "GRNNo")]:
                logger.info(f"➡️ Iniciando atualização de log {campo}...")
                condicao = "" if update_geral else f"AND (l.{campo}='' OR l.{campo} IS NULL)"
                sql_template = f"""
                    UPDATE whsproductsputawaylog l
                    JOIN (
                        SELECT l.Id
                        FROM whsproductsputawaylog l
                        INNER JOIN whsaurora071 a
                        ON p.reference = a.FileRefPrefix
                        AND l.PN = a.Item
                        WHERE (l.{campo} IS NULL OR l.{campo} <> a.{coluna})
                        {condicao}
                        LIMIT {{lote}}
                    ) AS t ON l.Id = t.Id
                    INNER JOIN whsaurora071 a
                    ON p.reference = a.FileRefPrefix
                    AND l.PN = a.Item
                    SET l.{campo} = a.{coluna}
                """
                executar_sql_em_lotes(cursor, conn, sql_template, resultados, f"log_{campo}")
                logger.info(f"✅ Atualização de log {campo} concluída.")

        logger.info("➡️ Limpando tabela whsaurora071...")
        cursor.execute("TRUNCATE TABLE whsaurora071;")
        conn.commit()
        logger.info("✅ Tabela whsaurora071 limpa.")

    finally:
        cursor.close()
        conn.close()

    return {"status": "ok", "resultados": resultados}

#========================================================================================================
#         MOVIMENTO auroraAAF
#--------------------------------------------------------------------------------------------------------

# 🔹 Função para converter datas para formato MySQL
def converter_data(valor: str) -> Optional[str]:
    if not valor or valor.strip() == "":
        return None
    for fmt in ["%Y-%m-%d %H:%M:%S", "%d-%m-%Y %H:%M:%S"]:
        try:
            dt = datetime.strptime(valor.strip(), fmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S")  # formato aceito pelo MySQL
        except ValueError:
            continue
    return None

def executar_sql(cursor, conn, sql, resultados, chave, params=None):
    try:
        start = time.time()
        logging.info(f"Executando SQL [{chave}]: {sql} | Params: {params}")
        cursor.execute(sql, params) if params is not None else cursor.execute(sql)
        conn.commit()
        duration = time.time() - start
        afetados = cursor.rowcount

        resultados[chave] = {"afetados": afetados,"sql": sql,"params": params,"tempo_execucao": f"{duration:.4f}s"}

        logging.info("Execução [%s] afetou %s registros em %s",chave, afetados, f"{duration:.4f}s" )

        return afetados

    except Exception as e:
        conn.rollback()
        resultados[chave] = {"erro": str(e),"sql": sql,"params": params}
        logging.error("Erro ao executar [%s]: %s | Params: %r", chave, e, params)
        raise

@moviment_rp.post("/auroraAAF/process")
def processar_auroraAAF(
    update_geral: bool = False,
    aaf_log: bool = False,
    aaf_tela: bool = False,
    linhas: list[dict] = None,
    db: Session = Depends(get_db)
):
    conn = db.connection().connection
    cursor = conn.cursor()
    resultados = {}
    total_afetados = 0
    linhas_fisicas_afetadas = 0
    conferencia = []

    # 🔹 CONTROLE REAL DE AFETADOS
    refs_atualizadas = set()

    try:
        cursor.execute("TRUNCATE TABLE whsauroraaaf")
        conn.commit()

        # ==================================================
        # 1️⃣ INSERÇÃO NA whsauroraaaf (IGUAL DELPHI)
        # ==================================================
        if linhas:
            for linha in linhas:
                # Regra idêntica ao Delphi
                if linha.get("flag_yes") != "YES" and linha.get("ImportRefCode") != "GRN No":

                    cursor.execute("""
                        INSERT INTO whsauroraaaf (
                            reference,
                            Waybill,
                            aaf,
                            Criticality,
                            grn1,
                            ImportRefCode,
                            situationregistration,
                            dateregistration
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                    """, (
                        linha.get("reference"),
                        linha.get("Waybill"),
                        linha.get("aaf"),
                        linha.get("Criticality"),
                        linha.get("grn1"),
                        linha.get("ImportRefCode"),
                        "I"
                    ))

            conn.commit()

        # ==================================================
        # 2️⃣ ATUALIZAÇÃO whsproductsputaway
        # ==================================================
        if aaf_tela and linhas:
            for idx, linha in enumerate(linhas, start=1):
                sql = """
                    UPDATE whsproductsputaway
                    SET aaf = %s,
                    dateatualizeaaf = NOW()                        
                    WHERE Reference = %s
                """
                # , Criticality = %s
                params = (
                    linha.get("aaf"),
                    # linha.get("criticality"),
                    linha.get("reference")
                )

                if not update_geral:
                    sql += " AND aaf IS NULL"

                # executar_sql(cursor, conn, sql, resultados, f"aaf_tela_{idx}", params)
                afetados = executar_sql( cursor, conn, sql, resultados, f"aaf_tela_{idx}", params )

                linhas_fisicas_afetadas += afetados

                # 🔹 CONTA SOMENTE UMA VEZ POR REFERENCE
                if afetados > 0:
                    refs_atualizadas.add(linha.get("reference"))

                if aaf_log:
                    sql = """
                        UPDATE whsproductsputawaylog
                        SET aaf = %s,
                            Criticality = %s
                        WHERE Reference = %s
                    """
                    if not update_geral:
                        sql += " AND aaf IS NULL"

                    executar_sql(cursor, conn, sql, resultados, f"log_aaf_tela_{idx}", params)
                    total_afetados += resultados[f"log_aaf_tela_{idx}"]["afetados"]

        # ==================================================
        # 3️⃣ CONFERÊNCIA FINAL (BASEADA NA whsauroraaaf)
        # ==================================================
        cursor.execute("""
            SELECT
                a.reference,
                a.aaf              AS aaf_importado,
                p.aaf              AS aaf_aplicado,
                a.ImportRefCode    AS criticality_importado,
                p.Criticality      AS criticality_aplicado,
                p.dateatualizeaaf
            FROM whsauroraaaf a
            LEFT JOIN whsproductsputaway p
                ON p.reference = a.reference
            WHERE a.situationregistration = 'I'
            ORDER BY a.reference
        """)

        hoje = date.today()

        for ref, aaf_imp, aaf_apl, crit_imp, crit_apl, dt_atualiza in cursor.fetchall():
            divergencias = []
            status = "OK"


            # 🔹 PRIORIDADE 1 — ATUALIZADO
            if dt_atualiza and dt_atualiza.date() == hoje:
                status = "ATUALIZADO"
                observacao = "AAF atualizado hoje"

            else:
                # 🔹 DIVERGÊNCIAS
                if aaf_imp != aaf_apl:
                    divergencias.append("AAF diferente")

                # Se quiser reativar depois
                # if crit_imp != crit_apl:
                #     divergencias.append("Criticality diferente")

                if divergencias:
                    status = "DIVERGENTE"
                    observacao = "; ".join(divergencias)
                else:
                    observacao = ""

            conferencia.append({
                "reference": ref,
                "aaf_importado": aaf_imp,
                "aaf_aplicado": aaf_apl,
                "criticality_importado": crit_imp,
                "criticality_aplicado": crit_apl,
                "status": status,
                "observacao": observacao
            })

    finally:
        cursor.close()
        conn.close()

    # ==================================================
    # 4️⃣ RETORNO PARA O DELPHI
    # ==================================================
    return {
        "status": "ok",
        "total_afetados": len(refs_atualizadas),
        "linhas_fisicas_afetadas": linhas_fisicas_afetadas,
        "detalhes": resultados,
        "conferencia": conferencia
    }



#=====================================================================================================================
#                            TAREFAS Atribuir Operador
#---------------------------------------------------------------------------------------------------------------------
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

#================================================================================================
#                    GRAVAR O MOVIMENTO
#------------------------------------------------------------------------------------------------

class MovimentoPutaway(BaseModel):
    id: int = Field(0)
    pn: str
    reference: str
    waybill: str
    descricao: Optional[str] = ''
    posicao: Optional[str] = ''
    classe: Optional[str] = ''
    quantidade_revisada: float = 0.0
    volume: int = 0
    operador_id: Optional[str] = ''
    usuario_id: Optional[str] = ''
    avaria: Optional[bool] = False
    multipla_etiqueta: Optional[bool] = False

    # Campos opcionais que vinham da UI / Cells no Delphi (para preencher o LOG)
    lbl_qtd_total: Optional[float] = None
    grn1: Optional[str] = None
    processlines: Optional[int] = None
    processdate: Optional[str] = None  # string no formato esperado (se enviado)
    grn3: Optional[str] = None
    aaf: Optional[str] = None
    rnc: Optional[str] = None
    # campo que pode conter a data preenchida para DateProcessStart (equivalente a Cells[10,linha])
    cells_dateprocessstart: Optional[str] = None


@moviment_rp.post("/movimento/putaway")
def movimento_putaway(mov: MovimentoPutaway, db: Session = Depends(get_db)):

    conn = db.connection().connection
    cursor = conn.cursor()

    try:
        # 🚨 TRANSACTION START
        conn.begin()

        # ----------------------------
        # Normaliza entrada
        # ----------------------------
        tx_codigo_id = int(mov.id or 0)
        quant_revisada_val = float(mov.quantidade_revisada or 0.0)
        volume_val = int(mov.volume or 0)
        usuario_token = (mov.usuario_id or '').strip()
        operador_id = (mov.operador_id or '').strip()

        TipoEtiqueta = 'N'
        StandardQty = 0.0
        LPSQty = 0.0
        UndeclaredSQty = 0.0
        MultiplaEtiqueta = False
        MaxVolume = volume_val
        GravaIniProcesso = False

        IdUser_new = usuario_token + "," if usuario_token else ""

        # ----------------------------
        # 🔒 SELECT COM LOCK (FOR UPDATE)
        # ----------------------------
        select_sql = """
            SELECT
              MAX(User_Id),
              SUM(COALESCE(RevisedQty,0)),
              SUM(COALESCE(Qty,0)),
              MIN(DateProcessStart),
              SUM(COALESCE(RevisedVolume,0))
            FROM whsproductsputaway
            WHERE Reference = %s
              AND Waybill = %s
              AND PN = %s
              AND COALESCE(situationregistration,'') <> 'E'
            FOR UPDATE
        """
        cursor.execute(select_sql, (mov.reference, mov.waybill, mov.pn))
        row = cursor.fetchone()

        # ----------------------------
        # UPDATE
        # ----------------------------
        if row:
            IdUser_db = row[0] or ''
            QtdBanco = float(row[1] or 0.0)
            QtdProcesso = float(row[2] or 0.0)
            DtProc = row[3]
            VolumeDB = int(row[4] or 0)

            DifQtd = QtdBanco + quant_revisada_val - QtdProcesso
            GravaIniProcesso = (DtProc is None)

            if VolumeDB > MaxVolume:
                MaxVolume = VolumeDB

            if QtdProcesso <= 0:
                UndeclaredSQty = quant_revisada_val
                TipoEtiqueta = 'F'
            else:
                if DifQtd <= 0:
                    StandardQty = quant_revisada_val
                else:
                    if DifQtd >= quant_revisada_val:
                        LPSQty = DifQtd
                        TipoEtiqueta = 'L'
                    else:
                        LPSQty = DifQtd
                        StandardQty = quant_revisada_val - DifQtd
                        MultiplaEtiqueta = True

            token_user = usuario_token + ","
            if usuario_token and token_user not in IdUser_db:
                base = IdUser_db.rstrip(',')
                IdUser_new = f"{base},{usuario_token}," if base else f"{usuario_token},"
            else:
                IdUser_new = IdUser_db

            update_parts = [
                "Print='F'",
                "typeprint='N'",
                "RevisedQty = COALESCE(RevisedQty,0) + %s",
                "User_Id = %s",
                "StandardQty = COALESCE(StandardQty,0) + %s",
                "LPSQty = COALESCE(LPSQty,0) + %s",
                "UndeclaredSQty = COALESCE(UndeclaredSQty,0) + %s",
                "RevisedVolume = %s",
                "Description = %s",
                "Position = %s",
                "siccode = %s",
                "situationregistration='A'",
                "dateregistration=CURRENT_TIMESTAMP"
            ]

            params = [
                quant_revisada_val,
                IdUser_new,
                StandardQty,
                LPSQty,
                UndeclaredSQty,
                MaxVolume,
                mov.descricao,
                mov.posicao,
                mov.classe
            ]

            if mov.avaria:
                update_parts.append("breakdownQty = COALESCE(breakdownQty,0) + %s")
                params.append(quant_revisada_val)

            if GravaIniProcesso:
                update_parts.append("DateProcessStart=CURRENT_TIMESTAMP")

            update_sql = f"""
                UPDATE whsproductsputaway
                SET {", ".join(update_parts)}
                WHERE Reference=%s AND Waybill=%s AND PN=%s
            """
            params.extend([mov.reference, mov.waybill, mov.pn])

            cursor.execute(update_sql, tuple(params))

            cursor.execute(
                "SELECT ID FROM whsproductsputaway WHERE Reference=%s AND Waybill=%s AND PN=%s",
                (mov.reference, mov.waybill, mov.pn)
            )
            mov.id = cursor.fetchone()[0]

        # ----------------------------
        # INSERT (registro novo)
        # ----------------------------
        else:
            TipoEtiqueta = "F"
            UndeclaredSQty = quant_revisada_val

            insert_sql = """
                INSERT INTO whsproductsputaway
                (PN, Description, Position, Qty, datecreate, operator_id,
                 Print, typeprint, RevisedQty, User_Id,
                 StandardQty, LPSQty, UndeclaredSQty, breakdownQty,
                 RevisedVolume, Reference, Waybill, DateProcessStart,
                 situationregistration, dateregistration)
                VALUES
                (%s,%s,%s,0,CURRENT_TIMESTAMP,%s,
                 'F','F',%s,%s,
                 %s,%s,%s,%s,
                 %s,%s,%s,CURRENT_TIMESTAMP,
                 'I',CURRENT_TIMESTAMP)
            """

            cursor.execute(insert_sql, (
                mov.pn, mov.descricao, mov.posicao, operador_id,
                quant_revisada_val, IdUser_new,
                0.0, 0.0, UndeclaredSQty,
                quant_revisada_val if mov.avaria else 0.0,
                MaxVolume, mov.reference, mov.waybill
            ))

            mov.id = cursor.lastrowid

        # ----------------------------
        # LOG (mesma transação)
        # ----------------------------
        I = 1 if MultiplaEtiqueta else 0
        F = 2 if MultiplaEtiqueta else 0

        for cont in range(I, F + 1):
            typeprint_val = TipoEtiqueta if cont == 0 else ("N" if cont == 1 else "L")
            revisedqty_for_log = quant_revisada_val if cont == 0 else (StandardQty if cont == 1 else LPSQty)

            cursor.execute("""
                INSERT INTO whsproductsputawaylog
                (Id_whsprod, Reference, Waybill, PN, Description, Position, siccode,
                 Qty, operator_id, datecreate, Print, typeprint, User_Id,
                 RevisedQty, breakdownQty, RevisedVolume, releasedQty,
                 situationregistration, dateregistration)
                VALUES
                (%s,%s,%s,%s,%s,%s,%s,
                 %s,%s,CURRENT_TIMESTAMP,'F',%s,%s,
                 %s,%s,%s,%s,'I',CURRENT_TIMESTAMP)
            """, (
                mov.id, mov.reference, mov.waybill, mov.pn,
                mov.descricao, mov.posicao, mov.classe,
                quant_revisada_val, operador_id,
                typeprint_val,
                int(usuario_token) if usuario_token.isdigit() else usuario_token,
                revisedqty_for_log,
                quant_revisada_val if mov.avaria else 0.0,
                MaxVolume,
                quant_revisada_val
            ))

        # ✅ COMMIT FINAL
        conn.commit()
        return {"status": "ok", "id": mov.id}

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        cursor.close()
        conn.close()


#====================================================
#            Aurora071
#----------------------------------------------------
class Aurora071Item(BaseModel):
    DocType: str
    FileRef: str
    Item: str
    StockGoodsInwards: Optional[str]
    Receiptdate: Optional[str]
    TXIssuedate: Optional[str]
    GRNNo: Optional[str]
    PMP: Optional[str]



def br_to_us(date_str: str) -> str:
    """Converte 'DD/MM/YYYY' para 'YYYY-MM-DD'."""
    try:
        return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
    except Exception:
        return date_str  # se já estiver no formato certo, retorna como está

# ============ ROTA ===================================
@moviment_rp.post("/whsaurora071/import")
def import_whsaurora071(
    itens: list[Aurora071Item],
    db: Session = Depends(get_db)
):
    try:
        # Limpa a tabela antes da importação
        db.execute(text("TRUNCATE TABLE whsaurora071"))

        # SQL ajustado com INSERT IGNORE
        sql = text("""
            INSERT IGNORE INTO whsaurora071
            (DocType, FileRef, Item, StockGoodsInwards,
             Receiptdate, TXIssuedate, GRNNo, PMP,
             situationregistration, dateregistration)
            VALUES
            (:DocType, :FileRef, :Item, :StockGoodsInwards,
             :Receiptdate, :TXIssuedate, :GRNNo, :PMP,
             'I', NOW())
        """)

        chaves = set()
        registros_salvos = 0

        for item in itens:
            # Chave composta com todos os campos recebidos
            chave = (
                item.DocType,
                item.FileRef,
                item.Item,
                item.StockGoodsInwards,
                item.Receiptdate,
                item.TXIssuedate,
                item.GRNNo,
                item.PMP
            )

            if chave in chaves:
                # Ignora duplicados silenciosamente
                continue
            chaves.add(chave)

            db.execute(sql, item.model_dump())
            registros_salvos += 1

        db.commit()

        return {
            "status": "ok",
            "records": registros_salvos
        }

    except Exception as e:
        db.rollback()
        import traceback
        print("Erro na importação:", traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
