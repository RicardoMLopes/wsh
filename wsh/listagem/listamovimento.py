from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from sqlalchemy.orm import Session
import logging
from pydantic import BaseModel
from connection.db_connection import SessionLocal
from typing import List, Optional
from datetime import datetime, date
import json

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



CAMPOS_DATA_SEM_HORA = {"grn1", "grn3"}
CAMPOS_DATA_COM_HORA = {
    "datecreate",
    "aaf",
    "DateProcessStart",
    "DateProcessEnd",
    "dateregistration"
}
def normaliza_data(valor):
    if valor is None:
        return None
    if isinstance(valor, str) and valor.upper() == "NULL":
        return None
    return valor

def formatar_datas(row_dict: dict):
    for campo in CAMPOS_DATA_SEM_HORA | CAMPOS_DATA_COM_HORA:
        valor = row_dict.get(campo)

        if not valor:
            continue

        try:
            if isinstance(valor, (datetime, date)):
                dt = valor
            else:
                dt = datetime.fromisoformat(str(valor))

            if campo in CAMPOS_DATA_SEM_HORA:
                row_dict[campo] = dt.strftime("%d/%m/%Y")
            else:
                row_dict[campo] = dt.strftime("%d/%m/%Y %H:%M:%S")

        except Exception:
            pass  # mantém o valor original se não conseguir converter

    return row_dict

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
    data_tipo: str = "",
    ordem: int = 0,
    db: Session = Depends(get_db)
):

    logger.info("📌 Iniciando rota /listageral (STREAMING)")

    tabela = "whsproductsputaway" if tipo == "G" else "whsproductsputawaylog"
    sql = f"SELECT * FROM {tabela} WHERE Id > 0"
    params = {}

    if controle:
        sql += " AND Reference = :controle"
        params["controle"] = controle

    if waybill:
        sql += " AND Waybill = :waybill"
        params["waybill"] = waybill

    if codigoitem:
        sql += " AND PN = :pn"
        params["pn"] = codigoitem

    if situacao == 0:
        sql += " AND situationregistration <> 'E'"
    elif situacao == 1:
        sql += " AND situationregistration = 'E'"

    if status == 1:
        sql += " AND ((Qty = RevisedQty) AND (breakdownQty = 0))"
    elif status == 2:
        sql += " AND LPSQty > 0"
    elif status == 3:
        sql += " AND UndeclaredSQty > 0"
    elif status == 4:
        sql += " AND breakdownQty > 0"
    elif status == 5:
        sql += " AND ((Qty > RevisedQty) AND (breakdownQty = 0))"

    if processend == 1:
        sql += " AND DateProcessEnd IS NOT NULL"

    if operador:
        if tipo == "G":
            sql += " AND operator_id LIKE :operador"
            params["operador"] = f"%{operador}%"
        else:
            sql += " AND User_Id = :operador"
            params["operador"] = operador

    dataini = normaliza_data(dataini)
    datafim = normaliza_data(datafim)

    if filtro_data == 1 and dataini and datafim:
        # filtro de período (igual Delphi)
        if data_tipo and dataini and datafim:
            if data_tipo == "datecreate":
                sql += " AND datecreate BETWEEN :dataini AND :datafim"
            elif data_tipo == "start":
                sql += " AND DateProcessStart BETWEEN :dataini AND :datafim"
            elif data_tipo == "end":
                sql += " AND DateProcessEnd BETWEEN :dataini AND :datafim"
            elif data_tipo == "aaf":
                sql += " AND aaf BETWEEN :dataini AND :datafim"
            elif data_tipo == "grn":
                sql += " AND grn BETWEEN :dataini AND :datafim"
            elif data_tipo == "grn3":
                sql += " AND grn3 BETWEEN :dataini AND :datafim"
        params["dataini"] = dataini
        params["datafim"] = datafim

    order_fields = [
        "", "Id", "User_Id", "PN", "Description", "Qty", "RevisedQty",
        "Position", "dateregistration", "siccode", "Reference", "Waybill",
        "operator_id", "processlines", "AAF", "grn1", "grn",
        "GRN3", "RNC", "processdate"
    ]

    if ordem > 0:
        sql += f" ORDER BY {order_fields[ordem]}"

    logger.info(f"🟦 SQL Streaming:\n{sql}")

    from datetime import datetime, date

    CAMPOS_DATA_SEM_HORA = {"grn1", "grn3"}
    CAMPOS_DATA_COM_HORA = {
        "datecreate",
        "aaf",
        "dateregistration",
        "DateProcessStart",
        "DateProcessEnd"
    }

    def formatar_datas(row_dict: dict):
        for campo in CAMPOS_DATA_SEM_HORA | CAMPOS_DATA_COM_HORA:
            valor = row_dict.get(campo)

            if not valor:
                continue

            try:
                if isinstance(valor, (datetime, date)):
                    dt = valor
                else:
                    dt = datetime.fromisoformat(str(valor))

                if campo in CAMPOS_DATA_SEM_HORA:
                    row_dict[campo] = dt.strftime("%d/%m/%Y")
                else:
                    row_dict[campo] = dt.strftime("%d/%m/%Y %H:%M:%S")

            except Exception:
                pass

        return row_dict

    def stream():
        yield b'{"success": true, "data": ['

        first = True
        result = db.execute(text(sql), params)

        for row in result:
            if not first:
                yield b","
            first = False

            row_dict = dict(row._mapping)
            row_dict = formatar_datas(row_dict)

            yield json.dumps(row_dict, default=str).encode("utf-8")

        yield b"]}"

    return StreamingResponse(
        stream(),
        media_type="application/json"
    )


# =========================================================================
#
# =========================================================================

class GravaGRNItem(BaseModel):
    reference: str
    waybill: str
    pn: str

class GravaGRNRequest(BaseModel):
    grn1: Optional[str] = None
    grn3: Optional[str] = None
    processdate: Optional[str] = None
    aaf: Optional[str] = None
    rnc: Optional[str] = None
    grn: Optional[str] = None
    itens: List[GravaGRNItem]

def converter_data(valor: str) -> Optional[str]:
    """
    Converte data de 'DD/MM/YYYY' para 'YYYY-MM-DD'.
    Retorna None se o valor for vazio ou inválido.
    """
    if not valor or valor.strip() in ["", "  /  /    "]:
        return None
    try:
        return datetime.strptime(valor.strip(), "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None

@listagem_rp.post("/grava-grn")
def grava_grn(dados: GravaGRNRequest, db: Session = Depends(get_db)):
    try:
        if not dados.itens:
            raise HTTPException(status_code=400, detail="Nenhum item informado")

        campos = []
        params = {}

        if dados.grn1:
            data_convertida = converter_data(dados.grn1)
            if data_convertida:
                campos.append("GRN1 = :grn1")
                params["grn1"] = data_convertida

        if dados.grn3:
            data_convertida = converter_data(dados.grn3)
            if data_convertida:
                campos.append("GRN3 = :grn3")
                params["grn3"] = data_convertida

        # 🔹 CONVERSÃO DE DATA
        if dados.processdate:
            data_convertida = converter_data(dados.processdate)
            if data_convertida:
                campos.append("processdate = :processdate")
                params["processdate"] = data_convertida

        if dados.aaf:
            data_convertida = converter_data(dados.aaf)
            if data_convertida:
                campos.append("AAF = :aaf")
                params["aaf"] = data_convertida

        if dados.rnc:
            campos.append("RNC = :rnc")
            params["rnc"] = dados.rnc

        if dados.grn:
            campos.append("GRN = :grn")
            params["grn"] = dados.grn

        if not campos:
            raise HTTPException(status_code=400, detail="Nenhum campo para atualizar")

        # 🔹 SQL COMPATÍVEL COM MYSQL
        sql = f"""
            UPDATE whsproductsputaway
            SET {", ".join(campos)}
            WHERE `Reference` = :reference
              AND `Waybill` = :waybill
              AND `PN` = :pn
        """

        total = 0

        for item in dados.itens:
            exec_params = params.copy()
            exec_params.update({
                "reference": item.reference,
                "waybill": item.waybill,
                "pn": item.pn
            })

            result = db.execute(text(sql), exec_params)
            total += result.rowcount

        db.commit()

        return {
            "success": True,
            "registros_atualizados": total
        }

    except HTTPException:
        db.rollback()
        raise

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))