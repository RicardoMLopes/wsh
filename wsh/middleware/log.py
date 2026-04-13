from starlette.middleware.base import BaseHTTPMiddleware
from fastapi import Request
import time
from sqlalchemy import text
from connection.db_connection import SessionLocal

# ------------------------------------------------------
# CONEXÃO
# ------------------------------------------------------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class MovLogMiddleware(BaseHTTPMiddleware):

    async def dispatch(self, request: Request, call_next):

        start_time = time.time()

        # inicia estrutura padrão
        request.state.movlog = {
            "inserts": 0,
            "updates": 0,
            "total": 0
        }

        response = await call_next(request)

        duration = int((time.time() - start_time) * 1000)

        db = next(get_db())

        try:
            data = request.state.movlog

            db.execute(
                text("""
                    INSERT INTO movlog 
                        (rota, registros_inseridos, registros_atualizados, total_processado, data_hora)
                    VALUES 
                        (:rota, :ins, :upd, :total, NOW())
                """),
                {
                    "rota": request.url.path,
                    "ins": data.get("inserts", 0),
                    "upd": data.get("updates", 0),
                    "total": data.get("total", 0),
                }
            )

            db.commit()

        except:
            db.rollback()

        return response