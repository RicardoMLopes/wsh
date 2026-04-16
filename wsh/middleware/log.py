from starlette.middleware.base import BaseHTTPMiddleware
from fastapi import Request
import time
from sqlalchemy import text
from connection.db_connection import SessionLocal
import logging



logger = logging.getLogger("movlog")

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
        response = None

        logger.info(f"[MOVLOG] INICIO {request.method} {request.url.path}")

        # ==================================================
        # contexto padrão
        # ==================================================
        request.state.movlog = {
            "inserts": 0,
            "updates": 0,
            "total": 0,
            "usuario": None,
            "descricao": None,
            "status": "SUCCESS"
        }

        try:
            logger.info("[MOVLOG] -> call_next")
            response = await call_next(request)
            logger.info(f"[MOVLOG] <- call_next OK ({response.status_code})")

        except Exception as e:
            logger.exception("[MOVLOG] ERRO na rota")

            request.state.movlog["status"] = "ERROR"
            request.state.movlog["descricao"] = str(e)

            raise

        finally:
            logger.info("[MOVLOG] finally iniciado")

            duration = int((time.time() - start_time) * 1000)

            data = getattr(request.state, "movlog", {})

            inserts = data.get("inserts", 0)
            updates = data.get("updates", 0)
            total = data.get("total", 0)

            usuario = data.get("usuario")
            descricao = data.get("descricao")
            status = data.get("status", "SUCCESS")

            logger.info(
                f"[MOVLOG] dados -> inserts={inserts}, updates={updates}, total={total}, status={status}"
            )

            # ==================================================
            # regra de gravação
            # ==================================================
            if (inserts + updates) == 0 and status != "ERROR" and total == 0:
                logger.info("[MOVLOG] ignorado (sem impacto)")
                return response

            # ==================================================
            # 🔥 conexão no seu padrão ORIGINAL
            # ==================================================
            db_gen = get_db()
            db = next(db_gen)

            try:
                logger.info("[MOVLOG] executando INSERT movlog")

                db.execute(
                    text("""
                        INSERT INTO movlog 
                            (rota,
                             registros_inseridos,
                             registros_atualizados,
                             total_processado,
                             usuario,
                             descricao,
                             status,
                             tempo_ms,
                             data_hora)
                        VALUES 
                            (:rota,
                             :ins,
                             :upd,
                             :total,
                             :usuario,
                             :descricao,
                             :status,
                             :tempo,
                             NOW())
                    """),
                    {
                        "rota": request.url.path,
                        "ins": inserts,
                        "upd": updates,
                        "total": total,
                        "usuario": usuario,
                        "descricao": descricao,
                        "status": status,
                        "tempo": duration
                    }
                )

                db.commit()
                logger.info("[MOVLOG] INSERT OK")

            except Exception as e:
                logger.exception(f"[MOVLOG] ERRO ao gravar movlog: {e}")
                db.rollback()

            finally:
                # 🔥 fechamento correto no padrão yield
                try:
                    next(db_gen)
                except StopIteration:
                    pass

        logger.info("[MOVLOG] FIM request")

        return response