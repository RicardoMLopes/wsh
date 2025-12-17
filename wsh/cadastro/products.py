import logging
from fastapi import Depends, APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime
from connection.db_connection import SessionLocal

products_rp = APIRouter()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("products_logger")

# ----------------------------
# Pydantic Schema
# ----------------------------
class ProdutoSchema(BaseModel):
    PN: str
    Description: str
    Position: str
    PositionAux: str
    SiCcode: str

class ProdutosRequest(BaseModel):
    produtos: List[ProdutoSchema]

# ----------------------------
# Dependência do banco
# ----------------------------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ----------------------------
# Rota principal (staging + bulk merge)
# ----------------------------
@products_rp.post("/products", status_code=200)
def receber_produtos(request: ProdutosRequest, db: Session = Depends(get_db)):
    produtos = request.produtos
    total = len(produtos)
    logger.info(f"Recebendo {total} produtos (bulk staging + estatísticas + UPDATE/INSERT)")

    CHUNK_SIZE = 5000

    try:
        conn = db.connection().connection
        cursor = conn.cursor()

        # 1) Inserir todos na staging
        insert_staging_sql = """
            INSERT INTO staging_products (PN, Description, Position, PositionAux, SiCcode)
            VALUES (%s, %s, %s, %s, %s)
        """
        staging_rows = [
            (item.PN, item.Description, item.Position, item.PositionAux, item.SiCcode)
            for item in produtos
            if item.PN and item.PN.strip()
        ]

        pos = 0
        total_staging = len(staging_rows)
        while pos < total_staging:
            chunk = staging_rows[pos: pos + CHUNK_SIZE]
            cursor.executemany(insert_staging_sql, chunk)
            pos += CHUNK_SIZE
            logger.info(f"  Inseridos na staging: {min(pos, total_staging)}/{total_staging}")
        conn.commit()

        # 2) Criar staging única (deduplicação por PN)
        cursor.execute("DROP TEMPORARY TABLE IF EXISTS staging_unique")
        cursor.execute("""
            CREATE TEMPORARY TABLE staging_unique AS
            SELECT PN,
                   MAX(Description)   AS Description,
                   MAX(Position)      AS Position,
                   MAX(PositionAux)   AS PositionAux,
                   MAX(SiCcode)       AS SiCcode
            FROM staging_products
            GROUP BY PN
        """)
        conn.commit()

        # 3) Estatísticas
        logger.info("Calculando estatísticas...")

        inserts_sql = """
            SELECT COUNT(*)
            FROM staging_unique s
            LEFT JOIN whsproducts w ON w.PN = s.PN
            WHERE w.PN IS NULL
        """

        updates_sql = """
            SELECT COUNT(*)
            FROM staging_unique s
            JOIN whsproducts w ON w.PN = s.PN
            WHERE w.situationregistration <> 'E'
              AND (
                    COALESCE(TRIM(UPPER(w.Description)),'') <> COALESCE(TRIM(UPPER(s.Description)),'') OR
                    COALESCE(TRIM(UPPER(w.Position)),'') <> COALESCE(TRIM(UPPER(s.Position)),'') OR
                    COALESCE(TRIM(UPPER(w.PositionAux)),'') <> COALESCE(TRIM(UPPER(s.PositionAux)),'') OR
                    COALESCE(TRIM(UPPER(w.SiCcode)),'') <> COALESCE(TRIM(UPPER(s.SiCcode)),''))
        """

        ignorados_sql = """
            SELECT COUNT(*)
            FROM staging_unique s
            JOIN whsproducts w ON w.PN = s.PN
            WHERE w.situationregistration <> 'E'
              AND (
                    COALESCE(TRIM(UPPER(w.Description)),'') = COALESCE(TRIM(UPPER(s.Description)),'') AND
                    COALESCE(TRIM(UPPER(w.Position)),'') = COALESCE(TRIM(UPPER(s.Position)),'') AND
                    COALESCE(TRIM(UPPER(w.PositionAux)),'') = COALESCE(TRIM(UPPER(s.PositionAux)),'') AND
                    COALESCE(TRIM(UPPER(w.SiCcode)),'') = COALESCE(TRIM(UPPER(s.SiCcode)),''))
        """

        cursor.execute(inserts_sql)
        inseridos = cursor.fetchone()[0]

        cursor.execute(updates_sql)
        atualizados = cursor.fetchone()[0]

        cursor.execute(ignorados_sql)
        ignorados = cursor.fetchone()[0]

        # 4a) UPDATE — somente PNs existentes
        logger.info("Atualizando produtos existentes...")
        update_sql = """
            UPDATE whsproducts w
            JOIN staging_unique s ON s.PN = w.PN
            SET
                w.Description = s.Description,
                w.Position = s.Position,
                w.PositionAux = s.PositionAux,
                w.SiCcode = s.SiCcode,
                w.situationregistration = 'A',
                w.dateregistration = NOW()
            WHERE w.situationregistration <> 'E'
        """
        cursor.execute(update_sql)
        conn.commit()

        # 4b) INSERT — somente PNs inexistentes
        logger.info("Inserindo novos produtos...")
        insert_sql = """
            INSERT INTO whsproducts
                (PN, Description, Position, PositionAux, SiCcode, situationregistration, dateregistration)
            SELECT
                s.PN,
                s.Description,
                s.Position,
                s.PositionAux,
                s.SiCcode,
                'I',
                NOW()
            FROM staging_unique s
            LEFT JOIN whsproducts w ON w.PN = s.PN
            WHERE w.PN IS NULL
        """
        cursor.execute(insert_sql)
        conn.commit()

        # 5) Limpar staging
        cursor.execute("TRUNCATE TABLE staging_products")
        conn.commit()

        result = {
            "status": "success",
            "total_recebido": total,
            "inseridos": inseridos,
            "atualizados": atualizados,
            "ignorados": ignorados
        }

        logger.info(f"Processamento concluído: {result}")
        return result

    except Exception as e:
        logger.error(f"Erro durante processamento: {e}", exc_info=True)
        try:
            conn.rollback()
        except Exception:
            pass
        try:
            cursor.execute("TRUNCATE TABLE staging_products")
            conn.commit()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))


@products_rp.post("/update_positions")
def update_positions_run(atualizar: bool = False):
    db = SessionLocal()
    try:
        if atualizar:
            sql_insert = """
                INSERT INTO whspositionputaway (Position)
                SELECT DISTINCT Position 
                FROM whsproducts
                WHERE Position NOT IN (SELECT Position FROM whspositionputaway)
                  AND (whsproducts.Position = '' OR whsproducts.Position IS NULL)
            """
            db.execute(text(sql_insert))
            db.commit()

        return {"status": "ok", "msg": "Atualização concluída"}

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        db.close()



@products_rp.get("/positions")
def get_positions(
    filtro: Optional[str] = None,
    tipo: Optional[int] = 0
):
    db = SessionLocal()
    try:
        base_sql = "SELECT * FROM whspositionputaway"
        params = {}

        if filtro:
            if tipo == 0:  # >=
                base_sql += " WHERE Position >= :filtro"
                params["filtro"] = filtro
            elif tipo == 1:  # LIKE usando *
                base_sql += " WHERE Position LIKE :filtro"
                params["filtro"] = filtro.replace("*", "%") + "%"

        result = db.execute(text(base_sql), params).fetchall()
        data = [dict(r._mapping) for r in result]

        return {"status": "ok", "data": data}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        db.close()
