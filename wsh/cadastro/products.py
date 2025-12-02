import logging
from fastapi import Depends, APIRouter, HTTPException
from pydantic import BaseModel
from typing import List
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
    logger.info(f"Recebendo {total} produtos (bulk staging + estatísticas + MERGE)")

    CHUNK_SIZE = 5000

    try:
        conn = db.connection().connection
        cursor = conn.cursor()

        # 1) Inserir todos na staging
        insert_staging_sql = """
            INSERT INTO staging_products (PN, Description, Position, PositionAux, SiCcode)
            VALUES (%s, %s, %s, %s, %s)
        """
        staging_rows = [(item.PN, item.Description, item.Position, item.PositionAux, item.SiCcode) for item in produtos]

        pos = 0
        total_staging = len(staging_rows)
        while pos < total_staging:
            chunk = staging_rows[pos: pos + CHUNK_SIZE]
            cursor.executemany(insert_staging_sql, chunk)
            pos += CHUNK_SIZE
            logger.info(f"  Inseridos na staging: {min(pos, total_staging)}/{total_staging}")
        conn.commit()

        # 2) Criar staging única para evitar duplicatas
        cursor.execute("DROP TEMPORARY TABLE IF EXISTS staging_unique")
        cursor.execute("""
            CREATE TEMPORARY TABLE staging_unique AS
            SELECT DISTINCT PN, Description, Position, PositionAux, SiCcode
            FROM staging_products
        """)
        conn.commit()

        # 3) Calcular estatísticas com normalização
        logger.info("Calculando estatísticas...")

        inserts_sql = """
            SELECT COUNT(*) FROM staging_unique s
            LEFT JOIN (SELECT PN FROM whsproducts GROUP BY PN) w ON w.PN = s.PN
            WHERE w.PN IS NULL
        """
        updates_sql = """
            SELECT COUNT(*) FROM staging_unique s
            JOIN (
                SELECT PN,
                       MAX(Description) AS Description,
                       MAX(Position) AS Position,
                       MAX(PositionAux) AS PositionAux,
                       MAX(SiCcode) AS SiCcode
                FROM whsproducts
                GROUP BY PN
            ) w ON w.PN = s.PN
            WHERE (COALESCE(TRIM(UPPER(w.Description)),'') <> COALESCE(TRIM(UPPER(s.Description)),'')
                OR COALESCE(TRIM(UPPER(w.Position)),'') <> COALESCE(TRIM(UPPER(s.Position)),'')
                OR COALESCE(TRIM(UPPER(w.PositionAux)),'') <> COALESCE(TRIM(UPPER(s.PositionAux)),'')
                OR COALESCE(TRIM(UPPER(w.SiCcode)),'') <> COALESCE(TRIM(UPPER(s.SiCcode)),''))
        """
        ignorados_sql = """
            SELECT COUNT(*) FROM staging_unique s
            JOIN (
                SELECT PN,
                       MAX(Description) AS Description,
                       MAX(Position) AS Position,
                       MAX(PositionAux) AS PositionAux,
                       MAX(SiCcode) AS SiCcode
                FROM whsproducts
                GROUP BY PN
            ) w ON w.PN = s.PN
            WHERE (COALESCE(TRIM(UPPER(w.Description)),'') = COALESCE(TRIM(UPPER(s.Description)),'')
                AND COALESCE(TRIM(UPPER(w.Position)),'') = COALESCE(TRIM(UPPER(s.Position)),'')
                AND COALESCE(TRIM(UPPER(w.PositionAux)),'') = COALESCE(TRIM(UPPER(s.PositionAux)),'')
                AND COALESCE(TRIM(UPPER(w.SiCcode)),'') = COALESCE(TRIM(UPPER(s.SiCcode)),''))
        """

        cursor.execute(inserts_sql)
        inseridos = cursor.fetchone()[0]

        cursor.execute(updates_sql)
        atualizados = cursor.fetchone()[0]

        cursor.execute(ignorados_sql)
        ignorados = cursor.fetchone()[0]

        if inseridos + atualizados + ignorados != total:
            logger.warning(f"Totais não batem: {inseridos}+{atualizados}+{ignorados} != {total}")

        # 4) MERGE em lote
        logger.info("Executando MERGE...")
        merge_sql = """
            INSERT INTO whsproducts (PN, Description, Position, PositionAux, SiCcode, situationregistration, dateregistration)
            SELECT PN, Description, Position, PositionAux, SiCcode, 'I', NOW()
            FROM staging_unique
            ON DUPLICATE KEY UPDATE
                Description = VALUES(Description),
                Position = VALUES(Position),
                PositionAux = VALUES(PositionAux),
                SiCcode = VALUES(SiCcode),
                situationregistration = 'A',
                dateregistration = VALUES(dateregistration)
        """
        cursor.execute(merge_sql)
        conn.commit()

        # 5) Limpar staging
        cursor.execute("TRUNCATE TABLE staging_products")
        conn.commit()

        # 6) Retorno final
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