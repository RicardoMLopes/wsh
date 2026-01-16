from fastapi import Depends, APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
from typing import List
from connection.db_connection import SessionLocal
from sqlalchemy.orm import Session
from datetime import datetime
import logging, time
from sqlalchemy import text

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
        resultados[chave] = {
            "afetados": afetados,
            "sql": sql,
            "params": params,
            "tempo_execucao": f"{duration:.4f}s"
        }
        logging.info("Execução [%s] afetou %s registros em %s",
                     chave, afetados, f"{duration:.4f}s")
    except Exception as e:
        conn.rollback()
        resultados[chave] = {"erro": str(e), "sql": sql, "params": params}
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
    conferencia = []

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
                    SET aaf = %s                        
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

                executar_sql(cursor, conn, sql, resultados, f"aaf_tela_{idx}", params)
                total_afetados += resultados[f"aaf_tela_{idx}"]["afetados"]

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
                p.Criticality      AS criticality_aplicado
            FROM whsauroraaaf a
            LEFT JOIN whsproductsputaway p
                ON p.reference = a.reference
            WHERE a.situationregistration = 'I'
            ORDER BY a.reference
        """)

        for ref, aaf_imp, aaf_apl, crit_imp, crit_apl in cursor.fetchall():
            divergencias = []

            if aaf_imp != aaf_apl:
                divergencias.append("AAF diferente")

            # if crit_imp != crit_apl:
            #     divergencias.append("Criticality diferente")

            status = "OK" if not divergencias else "DIVERGENTE"

            conferencia.append({
                "reference": ref,
                "aaf_importado": aaf_imp,
                "aaf_aplicado": aaf_apl,
                "criticality_importado": crit_imp,
                "criticality_aplicado": crit_apl,
                "status": status,
                "observacao": "; ".join(divergencias)
            })

    finally:
        cursor.close()
        conn.close()

    # ==================================================
    # 4️⃣ RETORNO PARA O DELPHI
    # ==================================================
    return {
        "status": "ok",
        "total_afetados": total_afetados,
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
    """
    Rota mov. putaway traduzida do Delphi.
    - Mantém comportamento do Delphi (calculo etiquetas, acumulo User_Id no registro principal)
    - Usa autoincrement (cursor.lastrowid) para INSERT principal
    - Grava LOG sempre (após UPDATE ou INSERT)
    - whsproductsputawaylog.User_Id recebe apenas usuario atual (numérico)
    """
    # logger.info(">>> [INICIO] movimento_putaway (rotina completa estilo Delphi)")
    # logger.info(f"[REQUEST] Dados recebidos: {mov.dict()}")

    # conexão crua (pymysql expected)
    conn = db.connection().connection
    cursor = conn.cursor()

    try:
        # ----------------------------
        # Normaliza entrada
        # ----------------------------
        tx_codigo_id = int(mov.id or 0)
        quant_revisada_val = float(mov.quantidade_revisada or 0.0)
        volume_val = int(mov.volume or 0)
        usuario_token = (mov.usuario_id or '').strip()   # ex: '19'
        operador_id = (mov.operador_id or '').strip()    # ex: '22'

        # logger.info(f"[STEP] Normalizando valores de entrada")
        # logger.info(f"[DATA] ID={tx_codigo_id} QtdRevisada={quant_revisada_val} Vol={volume_val} Usuario={usuario_token}")

        # Variáveis Delphi-style
        TipoEtiqueta = 'N'
        StandardQty = 0.0
        LPSQty = 0.0
        UndeclaredSQty = 0.0
        MultiplaEtiqueta = False
        MaxVolume = volume_val
        GravaIniProcesso = False

        # IdUser_new será calculado para o registro principal (string com vírgula)
        IdUser_new = usuario_token + "," if usuario_token else ""

        # ----------------------------
        # SELECT inicial (equivalente Delphi)
        # ----------------------------
        # logger.info("[SQL] Executando SELECT inicial estilo Delphi")
        select_sql = """
            SELECT
              MAX(User_Id) AS User,
              SUM(COALESCE(RevisedQty,0)) AS QtyLanc,
              SUM(COALESCE(Qty,0)) AS QtyProc,
              MIN(DateProcessStart) AS DtProc,
              SUM(COALESCE(RevisedVolume,0)) AS Volume
            FROM whsproductsputaway
            WHERE ID = %s
              AND Reference = %s
              AND Waybill = %s
              AND PN = %s
              AND COALESCE(situationregistration,'') <> 'E'
        """
        cursor.execute(select_sql, (tx_codigo_id, mov.reference, mov.waybill, mov.pn))
        row = cursor.fetchone()
        # logger.info(f"[SQL-RESULT] Resultado SELECT: {row}")

        # ----------------------------
        # Se registro existe: UPDATE
        # ----------------------------
        if row and tx_codigo_id != 0:
            # logger.info("[FLOW] Fluxo UPDATE → registro existente encontrado")
            IdUser_db = row[0] or ''                      # exemplo: '19,' ou '19,03,'
            QtdBanco = float(row[1] or 0.0)
            QtdProcesso = float(row[2] or 0.0)
            DtProc = row[3]
            VolumeDB = int(row[4] or 0)

            # logger.info(f"[DATA] Banco={QtdBanco} Processo={QtdProcesso} DtProc={DtProc} VolumeDB={VolumeDB}")

            # Cálculo Delphi
            DifQtd = QtdBanco + quant_revisada_val - QtdProcesso
            # logger.info(f"[DATA] DifQtd calculado: {DifQtd}")

            GravaIniProcesso = (DtProc is None)
            if GravaIniProcesso:
                logger.info("[FLOW] GravaIniProcesso = TRUE → será gravado DateProcessStart (UPDATE)")

            if VolumeDB > MaxVolume:
                MaxVolume = VolumeDB

            # TipoEtiqueta / Standard / LPS / Undeclared
            if QtdProcesso <= 0:
                UndeclaredSQty = quant_revisada_val
                TipoEtiqueta = 'F'
                # logger.info("[FLOW] TipoEtiqueta=F (Undeclared)")
            else:
                if DifQtd <= 0:
                    StandardQty = quant_revisada_val
                    TipoEtiqueta = 'N'
                    # logger.info("[FLOW] TipoEtiqueta=N (Standard)")
                else:
                    if DifQtd >= quant_revisada_val:
                        LPSQty = DifQtd
                        TipoEtiqueta = 'L'
                        # logger.info("[FLOW] TipoEtiqueta=L (LPS)")
                    else:
                        LPSQty = DifQtd
                        StandardQty = quant_revisada_val - DifQtd
                        MultiplaEtiqueta = True
                        # logger.info("[FLOW] TipoEtiqueta=N+L (Múltiplas etiquetas)")

            logger.info(f"[DATA] Standard={StandardQty} LPS={LPSQty} Undeclared={UndeclaredSQty} Multiple={MultiplaEtiqueta}")

            # Concatenação User_Id (mantém histórico no registro principal)
            # Delphi: se pos(LBLIdUsuario.Caption+',', IdUser) = 0 then ...
            token_user = usuario_token + ","
            if usuario_token and (token_user not in IdUser_db):
                # remove trailing comma if present
                if IdUser_db.endswith(','):
                    base = IdUser_db[:-1]
                else:
                    base = IdUser_db
                if base.strip() == '' or base.strip() == '0':
                    IdUser_new = f"{usuario_token},"
                else:
                    IdUser_new = f"{base},{usuario_token},"
            else:
                IdUser_new = IdUser_db

            # logger.info(f"[DATA] User_Id final (UPDATE): {IdUser_new}")

            # Montagem UPDATE dinâmico (placeholders + params)
            update_parts = []
            update_params = []

            update_parts.append("Print = 'F'")
            update_parts.append("typeprint = 'N'")

            update_parts.append("RevisedQty = COALESCE(RevisedQty,0) + %s")
            update_params.append(quant_revisada_val)

            update_parts.append("User_Id = %s")
            update_params.append(IdUser_new)

            if GravaIniProcesso:
                update_parts.append("DateProcessStart = CURRENT_TIMESTAMP")

            update_parts.append("StandardQty = COALESCE(StandardQty,0) + %s")
            update_params.append(StandardQty)
            update_parts.append("LPSQty = COALESCE(LPSQty,0) + %s")
            update_params.append(LPSQty)
            update_parts.append("UndeclaredSQty = COALESCE(UndeclaredSQty,0) + %s")
            update_params.append(UndeclaredSQty)

            if mov.avaria:
                update_parts.append("breakdownQty = COALESCE(breakdownQty,0) + %s")
                update_params.append(quant_revisada_val)

            update_parts.append("RevisedVolume = %s")
            update_params.append(MaxVolume)

            update_parts.append("Description = %s")
            update_params.append(mov.descricao)

            update_parts.append("Position = %s")
            update_params.append(mov.posicao)

            update_parts.append("siccode = %s")
            update_params.append(mov.classe)

            update_parts.append("situationregistration = 'A'")
            update_parts.append("dateregistration = CURRENT_TIMESTAMP")

            update_sql = "UPDATE whsproductsputaway SET " + ", ".join(update_parts) + \
                         " WHERE ID = %s AND Reference = %s AND Waybill = %s AND PN = %s"

            update_params.extend([tx_codigo_id, mov.reference, mov.waybill, mov.pn])

            # logger.debug(f"[SQL-UPDATE] {update_sql}")
            # logger.debug(f"[SQL-PARAMS] {update_params}")

            cursor.execute(update_sql, tuple(update_params))
            conn.commit()

            # logger.info(f"[SQL] UPDATE executado com sucesso, linhas afetadas: {cursor.rowcount}")

            # tx_codigo_id/mov.id permanecem os mesmos (vinham no request)
            mov.id = tx_codigo_id

        # ----------------------------
        # INSERT (registro novo) — sem MAX(ID)
        # ----------------------------
        else:
            # logger.info("[FLOW] Fluxo INSERT → nenhum registro encontrado (novo)")

            TipoEtiqueta = "F"
            UndeclaredSQty = quant_revisada_val

            # se IdUser_new vazio, preenche com usuario atual
            if IdUser_new == "":
                IdUser_new = usuario_token + "," if usuario_token else ""

            # logger.info("[SQL] Montando INSERT principal (sem Id, autoincrement)")

            # incluímos breakdownQty sempre como campo; valor será 0.0 se não houver avaria
            insert_sql = """
                INSERT INTO whsproductsputaway
                (PN, Description, Position, Qty, datecreate, operator_id,
                 Print, typeprint, RevisedQty, User_Id,
                 StandardQty, LPSQty, UndeclaredSQty, breakdownQty,
                 RevisedVolume, Reference, Waybill, DateProcessStart,
                 situationregistration, dateregistration)
                VALUES
                (%s, %s, %s, 0, CURRENT_TIMESTAMP, %s,
                 'F', 'F', %s, %s,
                 %s, %s, %s, %s,
                 %s, %s, %s, CURRENT_TIMESTAMP,
                 'I', CURRENT_TIMESTAMP)
            """

            breakdown_val = quant_revisada_val if mov.avaria else 0.0

            insert_params = [
                mov.pn,               # PN
                mov.descricao,        # Description
                mov.posicao,          # Position
                operador_id,          # operator_id
                quant_revisada_val,   # RevisedQty
                IdUser_new,           # User_Id (string com vírgula)
                StandardQty,          # StandardQty
                LPSQty,               # LPSQty
                UndeclaredSQty,       # UndeclaredSQty
                breakdown_val,        # breakdownQty
                MaxVolume,            # RevisedVolume
                mov.reference,        # Reference
                mov.waybill,          # Waybill
                # DateProcessStart is CURRENT_TIMESTAMP in SQL
            ]

            # logger.debug(f"[SQL-INSERT] {insert_sql}")
            # logger.debug(f"[SQL-PARAMS] {insert_params}")

            cursor.execute(insert_sql, tuple(insert_params))
            # recuperar id gerado pelo autoincrement (MySQL / pymysql)
            new_id = cursor.lastrowid
            conn.commit()

            tx_codigo_id = new_id
            mov.id = new_id

            # logger.info(f"[DATA] Novo ID gerado: {new_id}")
            # logger.info("[SQL] INSERT principal executado com sucesso")

        # ----------------------------
        # GRAVAR LOG — SEMPRE (após UPDATE OU INSERT)
        # ----------------------------
        logger.info("[FLOW] Gravando LOG (sempre executado após UPDATE/INSERT)")

        # Delphi: I=0,F=0; se MultiplaEtiqueta then I=1;F=2
        I = 1 if MultiplaEtiqueta else 0
        F = 2 if MultiplaEtiqueta else 0

        for cont in range(I, F + 1):
            # logger.info(f"[STEP] Inserindo LOG (cont={cont})")

            # Tipo print e revisedqty por iteração (Delphi)
            if cont == 0:
                typeprint_val = TipoEtiqueta
                revisedqty_for_log = quant_revisada_val
            elif cont == 1:
                typeprint_val = "N"
                revisedqty_for_log = StandardQty
            elif cont == 2:
                typeprint_val = "L"
                revisedqty_for_log = LPSQty

            # logger.info(f"[DATA] typeprint={typeprint_val} revised={revisedqty_for_log}")

            # Campos opcionais (mapear do model)
            qty_for_log = mov.lbl_qtd_total if mov.lbl_qtd_total is not None else mov.quantidade_revisada
            grn1_val = mov.grn1 if getattr(mov, "grn1", None) else None
            processlines_val = mov.processlines if getattr(mov, "processlines", None) is not None else None
            processdate_val = mov.processdate if getattr(mov, "processdate", None) else None
            grn3_val = mov.grn3 if getattr(mov, "grn3", None) else None
            aaf_val = mov.aaf if getattr(mov, "aaf", None) else None
            rnc_val = mov.rnc if getattr(mov, "rnc", None) else None

            # releasedQty segue Delphi => QuantRevisada
            released_qty_val = quant_revisada_val

            # breakdownQty conforme avaria
            breakdown_qty_val = quant_revisada_val if mov.avaria else 0.0

            # DateProcessStart: usa cells_dateprocessstart quando informado, senão NULL
            dateprocessstart_val = None
            if mov.cells_dateprocessstart:
                # assumimos string passada no formato aceito pelo driver (ex: 'YYYY-MM-DD HH:MM:SS')
                dateprocessstart_val = mov.cells_dateprocessstart

            # Preparar SQL do LOG — usamos CURRENT_TIMESTAMP para datecreate e dateregistration,
            # Print='F' e situationregistration='I' como no Delphi.
            insert_log_sql = """
                INSERT INTO whsproductsputawaylog
                    (Id_whsprod, Reference, Waybill, PN, Description, Position, siccode,
                     Qty, grn1, operator_id, processlines, processdate, grn3, aaf, rnc,
                     datecreate, Print, typeprint, User_Id, RevisedQty,
                     breakdownQty, RevisedVolume, releasedQty, DateProcessStart,
                     StandardQty, LPSQty, UndeclaredSQty, situationregistration, dateregistration)
                    VALUES
                    (%s, %s, %s, %s, %s, %s, %s,
                     %s, %s, %s, %s, %s, %s, %s, %s,
                     CURRENT_TIMESTAMP, 'F', %s, %s, %s,
                     %s, %s, %s, %s,
                     %s, %s, %s, 'I', CURRENT_TIMESTAMP)

            """

            # Para User_Id no LOG, deve ser SOMENTE o usuário atual (numérico) — sem vírgula.
            # Tentamos converter usuario_token para int; se falhar, deixamos como string (driver tentará converter).
            user_id_log = None
            if usuario_token:
                try:
                    user_id_log = int(usuario_token)
                except Exception:
                    user_id_log = usuario_token

            params_log = [
                mov.id, mov.reference, mov.waybill, mov.pn, mov.descricao, mov.posicao, mov.classe,
                qty_for_log, grn1_val, operador_id or None, processlines_val,
                processdate_val, grn3_val, aaf_val, rnc_val,
                typeprint_val, user_id_log, revisedqty_for_log, breakdown_qty_val, MaxVolume,
                released_qty_val, dateprocessstart_val, StandardQty, LPSQty, UndeclaredSQty
            ]

            # DEBUG: conferir placeholders vs params
            expected = insert_log_sql.count("%s")
            received = len(params_log)
            # logger.debug(f"[DEBUG] insert_log_sql placeholders={expected}, params_log length={received}")
            if expected != received:
                # logger.error("[ERRO] placeholders != params_log length! SQL ou params incorretos.")
                # logger.error(f"SQL: {insert_log_sql}")
                # logger.error(f"PARAMS: {params_log}")
                raise Exception("Inconsistência entre placeholders e parâmetros no INSERT LOG")

            # logger.debug(f"[SQL-INSERT-LOG] {insert_log_sql}")
            # logger.debug(f"[SQL-PARAMS] {params_log}")

            cursor.execute(insert_log_sql, tuple(params_log))
            conn.commit()

            # logger.info(f"[SQL] INSERT LOG cont={cont} executado")

        # fim do loop logs
        # logger.info(">>> [SUCESSO] movimento_putaway finalizado com sucesso")
        return {"status": "ok", "id": mov.id}

    except Exception as e:
        # logger.exception("[ERRO] Exceção geral na rotina movimento_putaway")
        try:
            conn.rollback()
        except:
            pass
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        logger.info("[FINALIZAÇÃO] Fechando cursor/conexão")
        try:
            cursor.close()
        except:
            pass
        try:
            conn.close()
        except:
            pass

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
