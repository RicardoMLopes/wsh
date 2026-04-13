from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import text
from connection.db_connection import SessionLocal
from sqlalchemy.orm import Session
from fastapi import Request

a020_a190_rp = APIRouter()

# ------------------------------
# MODELO DOS DADOS ENVIADOS
# ------------------------------
class Linha(BaseModel):
    flag: str
    campo01: str
    pn: str
    descricao: str
    qty: str
    referencia: str
    waybill: str
    processlines: str
    usarDescricaoPN: bool


class ImportacaoRequest(BaseModel):
    tipo: str
    linhas: list[Linha]


# ------------------------------------------------------
# CONEXÃO
# ------------------------------------------------------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ------------------------------------------------------
# CONVERSÃO NUMÉRICA ROBUSTA
# ------------------------------------------------------
def to_decimal(valor):
    try:
        if valor is None:
            return 0.0

        if isinstance(valor, (int, float)):
            return float(valor)

        valor = str(valor).strip()

        if valor == "":
            return 0.0

        # remove lixo invisível
        valor = valor.replace("\r", "").replace("\n", "")

        # remove milhar e ajusta decimal BR → US
        valor = valor.replace(".", "").replace(",", ".")

        return float(valor)

    except Exception:
        raise Exception(f"Valor inválido para número: {valor}")


# ------------------------------------------------------
# ROTA PRINCIPAL
# ------------------------------------------------------
# @a020_a190_rp.post("/importar/a020_a190")
# def importar_a020_a190(payload: ImportacaoRequest, db: Session = Depends(get_db)):
#
#     tipo = payload.tipo.upper()
#     inputtype = "national" if tipo == "A020" else "transfer"
#
#     try:
#         for idx, linha in enumerate(payload.linhas, start=1):
#
#             if linha.campo01.strip() == "" or linha.flag.strip() == "S":
#                 continue
#
#             # 🔹 CONVERSÕES CENTRALIZADAS
#             qty = to_decimal(linha.qty)
#             processlines = int(to_decimal(linha.processlines))
#
#             # 🔍 DEBUG (opcional)
#             # print(f"[DEBUG] Linha {idx} | qty original: {repr(linha.qty)} | convertido: {qty}")
#
#             consulta = text("""
#                 SELECT *
#                 FROM whsproductsputaway
#                 WHERE TRIM(Reference) = :reference
#                   AND Waybill = :waybill
#                   AND PN = :pn
#                   AND situationregistration <> 'E'
#                 LIMIT 1
#             """)
#
#             result = db.execute(consulta, {
#                 "reference": linha.referencia.strip(),
#                 "waybill": linha.waybill,
#                 "pn": linha.pn
#             }).mappings().first()
#
#             descricao = linha.descricao if not linha.usarDescricaoPN else linha.pn
#
#             # ----------------------------------------------------
#             # UPDATE
#             # ----------------------------------------------------
#             if result:
#
#                 revisedQty = result["RevisedQty"] if result["RevisedQty"] is not None else 0
#
#                 if revisedQty == 0:
#
#                     update_sql = text("""
#                         UPDATE whsproductsputaway
#                         SET Qty = :qty,
#                             Description = :descricao,
#                             processlines = :processlines,
#                             inputtype = :inputtype,
#                             situationregistration = 'A',
#                             dateregistration = NOW()
#                         WHERE TRIM(Reference) = :reference
#                           AND Waybill = :waybill
#                           AND PN = :pn
#                     """)
#
#                     db.execute(update_sql, {
#                         "qty": qty,
#                         "descricao": descricao,
#                         "processlines": processlines,
#                         "inputtype": inputtype,
#                         "reference": linha.referencia.strip(),
#                         "waybill": linha.waybill,
#                         "pn": linha.pn
#                     })
#
#             # ----------------------------------------------------
#             # INSERT
#             # ----------------------------------------------------
#             else:
#
#                 max_id = db.execute(
#                     text("SELECT COALESCE(MAX(ID), 0) FROM whsproductsputaway")
#                 ).scalar()
#
#                 max_id += 1
#
#                 insert_sql = text("""
#                     INSERT INTO whsproductsputaway
#                         (Id, User_id, PN, Description, Reference, Qty,
#                          Waybill, processlines, inputtype, datecreate,
#                          situationregistration, dateregistration)
#                     VALUES
#                         (:id, 0, :pn, :descricao, :reference, :qty,
#                          :waybill, :processlines, :inputtype, NOW(),
#                          'I', NOW())
#                 """)
#
#                 db.execute(insert_sql, {
#                     "id": max_id,
#                     "pn": linha.pn,
#                     "descricao": descricao,
#                     "reference": linha.referencia.strip(),
#                     "qty": qty,
#                     "waybill": linha.waybill,
#                     "processlines": processlines,
#                     "inputtype": inputtype
#                 })
#
#         # ✅ COMMIT ÚNICO (muito importante)
#         db.commit()
#
#     except Exception as e:
#         db.rollback()
#
#         raise Exception(
#             f"Erro na linha {idx} | "
#             f"PN: {getattr(linha, 'pn', None)} | "
#             f"Reference: {getattr(linha, 'referencia', None)} | "
#             f"Erro: {str(e)}"
#         )
#
#     return {"status": "OK", "mensagem": "Rotina executada com sucesso!"}


@a020_a190_rp.post("/importar/a020_a190")
def importar_a020_a190(payload: ImportacaoRequest, request: Request, db: Session = Depends(get_db)):

    tipo = payload.tipo.upper()
    inputtype = "national" if tipo == "A020" else "transfer"

    try:
        # ----------------------------------------------------
        # PRÉ-FILTRO (remove linhas inúteis antes de tudo)
        # ----------------------------------------------------
        linhas_validas = [
            l for l in payload.linhas
            if not (l.campo01.strip() == "" or l.flag.strip() == "S")
        ]

        if not linhas_validas:
            return {"status": "OK", "mensagem": "Nenhuma linha válida para processar."}

        # ----------------------------------------------------
        # COLETA CHAVES
        # ----------------------------------------------------
        refs = list(set(l.referencia.strip() for l in linhas_validas))
        waybills = list(set(l.waybill for l in linhas_validas))
        pns = list(set(l.pn for l in linhas_validas))

        # ----------------------------------------------------
        # BUSCA ÚNICA
        # ----------------------------------------------------
        consulta = text("""
            SELECT *
            FROM whsproductsputaway
            WHERE reference IN :refs
              AND Waybill IN :waybills
              AND PN IN :pns
              AND situationregistration <> 'E'
        """)

        rows = db.execute(consulta, {
            "refs": tuple(refs),
            "waybills": tuple(waybills),
            "pns": tuple(pns)
        }).mappings().all()

        # ----------------------------------------------------
        # MAPA EM MEMÓRIA
        # ----------------------------------------------------
        mapa = {
            (r["reference"].strip(), r["Waybill"], r["PN"]): r
            for r in rows
        }

        # ----------------------------------------------------
        # PEGA MAX ID UMA VEZ
        # ----------------------------------------------------
        max_id = db.execute(
            text("SELECT COALESCE(MAX(ID), 0) FROM whsproductsputaway")
        ).scalar()

        # ----------------------------------------------------
        # LISTAS DE BATCH
        # ----------------------------------------------------
        updates = []
        inserts = []

        # ----------------------------------------------------
        # LOOP PRINCIPAL (SEM ACESSO AO BANCO)
        # ----------------------------------------------------
        for idx, linha in enumerate(linhas_validas, start=1):

            qty = to_decimal(linha.qty)
            processlines = int(to_decimal(linha.processlines))

            reference = linha.referencia.strip()
            key = (reference, linha.waybill, linha.pn)

            descricao = linha.descricao if not linha.usarDescricaoPN else linha.pn

            result = mapa.get(key)

            if result:
                revisedQty = result["RevisedQty"] or 0

                if revisedQty == 0:
                    updates.append({
                        "qty": qty,
                        "descricao": descricao,
                        "processlines": processlines,
                        "inputtype": inputtype,
                        "reference": reference,
                        "waybill": linha.waybill,
                        "pn": linha.pn
                    })

            else:
                max_id += 1

                inserts.append({
                    "id": max_id,
                    "pn": linha.pn,
                    "descricao": descricao,
                    "reference": reference,
                    "qty": qty,
                    "waybill": linha.waybill,
                    "processlines": processlines,
                    "inputtype": inputtype
                })

        # ----------------------------------------------------
        # EXECUÇÃO EM LOTE
        # ----------------------------------------------------

        if updates:
            db.execute(text("""
                UPDATE whsproductsputaway
                SET Qty = :qty,
                    Description = :descricao,
                    processlines = :processlines,
                    inputtype = :inputtype,
                    situationregistration = 'A',
                    dateregistration = NOW()
                WHERE reference = :reference
                  AND Waybill = :waybill
                  AND PN = :pn
            """), updates)

        if inserts:
            db.execute(text("""
                INSERT INTO whsproductsputaway
                    (Id, User_id, PN, Description, reference, Qty,
                     Waybill, processlines, inputtype, datecreate,
                     situationregistration, dateregistration)
                VALUES
                    (:id, 0, :pn, :descricao, :reference, :qty,
                     :waybill, :processlines, :inputtype, NOW(),
                     'I', NOW())
            """), inserts)

        db.commit()

        request.state.movlog = {
            "inserts": len(inserts),
            "updates": len(updates),
            "total": len(linhas_validas)
        }

    except Exception as e:
        db.rollback()

        raise Exception(
            f"Erro na linha {idx} | "
            f"PN: {getattr(linha, 'pn', None)} | "
            f"Reference: {getattr(linha, 'referencia', None)} | "
            f"Erro: {str(e)}"
        )

    return {
        "status": "OK",
        "mensagem": f"Processado com sucesso! Inserts: {len(inserts)} | Updates: {len(updates)}"
    }