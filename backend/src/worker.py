import logging,time
from pathlib import Path
from sqlalchemy import select
from redis import Redis
from src.config.database import Session
from src.config.settings import REDIS_URL
from src.models import DecisionRun
from src.services.run_service import execute_run,reconcile
redis=Redis.from_url(REDIS_URL)
logging.basicConfig(level=logging.INFO)
def main():
    while True:
        Path('/tmp/heartbeat').touch()
        try:
            with Session() as db:
                if not reconcile(db):
                    item=db.scalar(select(DecisionRun).where(DecisionRun.status=='queued').order_by(DecisionRun.id))
                    if item:execute_run(db,item.id)
                    else:redis.brpop('material:runs',timeout=2)
        except Exception:
            logging.exception('worker pass failed');time.sleep(2)
        time.sleep(.1)
if __name__=='__main__':main()
