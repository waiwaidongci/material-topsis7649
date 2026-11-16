import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from sqlalchemy import text
from redis import Redis
from src.config.database import Base,engine
from src.config.settings import SEED_DEMO,REDIS_URL
from src import models
from src.routes import auth,studies,samples,criteria,runs,notes,audit
from src.middlewares.audit import AuditMiddleware
from src.middlewares.errors import ErrorMiddleware
from src.middlewares.rate_limit import RateLimitMiddleware
def ensure_publication_columns():
    """Idempotent upgrade for existing databases: fencing and publication journal columns."""
    with engine.begin() as conn:
        if engine.dialect.name=='sqlite':
            cols={r[1] for r in conn.execute(text("PRAGMA table_info(decision_runs)"))}
            if 'epoch' not in cols:conn.execute(text("ALTER TABLE decision_runs ADD COLUMN epoch INTEGER NOT NULL DEFAULT 0"))
            if 'claim_id' not in cols:conn.execute(text("ALTER TABLE decision_runs ADD COLUMN claim_id VARCHAR(64) NOT NULL DEFAULT ''"))
            if 'heartbeat_at' not in cols:conn.execute(text("ALTER TABLE decision_runs ADD COLUMN heartbeat_at DATETIME"))
            if 'publication' not in cols:
                conn.execute(text("ALTER TABLE decision_runs ADD COLUMN publication JSON"))
                conn.execute(text("UPDATE decision_runs SET publication='{}' WHERE publication IS NULL"))
            return
        cols={r[0] for r in conn.execute(text("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='decision_runs'"))}
        if 'epoch' not in cols:conn.execute(text("ALTER TABLE decision_runs ADD COLUMN epoch INTEGER NOT NULL DEFAULT 0"))
        if 'claim_id' not in cols:conn.execute(text("ALTER TABLE decision_runs ADD COLUMN claim_id VARCHAR(64) NOT NULL DEFAULT ''"))
        if 'heartbeat_at' not in cols:conn.execute(text("ALTER TABLE decision_runs ADD COLUMN heartbeat_at DATETIME NULL"))
        if 'publication' not in cols:
            conn.execute(text("ALTER TABLE decision_runs ADD COLUMN publication JSON NULL"))
            conn.execute(text("UPDATE decision_runs SET publication=JSON_OBJECT() WHERE publication IS NULL"))
            conn.execute(text("ALTER TABLE decision_runs MODIFY publication JSON NOT NULL"))
@asynccontextmanager
async def lifespan(app):
    Base.metadata.create_all(engine)
    try:ensure_publication_columns()
    except Exception:logging.exception('publication column check failed; app remains available')
    if SEED_DEMO:
        try:
            from scripts.seed_demo import seed
            seed()
        except Exception:logging.exception('seed failed; app remains available')
    yield
app=FastAPI(title='芦砚声学材料权衡册',lifespan=lifespan)
app.add_middleware(AuditMiddleware);app.add_middleware(RateLimitMiddleware);app.add_middleware(ErrorMiddleware)
for mod in (auth,studies,samples,criteria,runs,notes,audit):app.include_router(mod.router)
@app.get('/health')
def health():
    with engine.connect() as conn:conn.execute(text('SELECT 1'))
    Redis.from_url(REDIS_URL).ping();return {'status':'ok'}
