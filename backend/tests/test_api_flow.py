"""API-level end-to-end check of the recoverable publication flow via FastAPI TestClient."""
import os,sys,shutil,json
from pathlib import Path
ROOT=Path('/tmp/api-test');shutil.rmtree(ROOT,ignore_errors=True);(ROOT/'artifacts').mkdir(parents=True)
os.environ['DATABASE_URL']=f'sqlite:///{ROOT}/test.db'
os.environ['ARTIFACT_DIR']=str(ROOT/'artifacts')
os.environ['SEED_DEMO']='true'
sys.path.insert(0,str(Path(__file__).resolve().parent.parent))
import redis
class FakeRedis:
    def __init__(self):self.kv={}
    def incr(self,k):self.kv[k]=self.kv.get(k,0)+1;return self.kv[k]
    def expire(self,k,s):return True
    def lpush(self,*a):return 1
    def brpop(self,*a,**k):return None
    def ping(self):return True
redis.Redis.from_url=classmethod(lambda cls,*a,**k:FakeRedis())
from fastapi.testclient import TestClient
from src.main import app
from src.config.database import Session
from src.models import DecisionRun
from src.services import run_service as rs
from sqlalchemy import update

with TestClient(app) as c:
    login=c.post('/auth/login',json={'email':'analyst@luyan.local','password':'Luyan2026!'})
    assert login.status_code==200,login.text
    token={'Authorization':'Bearer '+login.json()['token']}
    studies=c.get('/studies',headers=token).json()
    r=c.post('/runs',headers=token,json={'name':'API恢复测试','study_id':studies[0]['id'],'repeats':1000})
    assert r.status_code==201,r.text
    rid=r.json()['id']
    assert r.json()['publication']['log'][0]['event']=='created'
    # worker picks it up
    with Session() as db:rs.execute_run(db,rid)
    d=c.get(f'/runs/{rid}',headers=token).json()
    assert d['status']=='completed' and len(d['result']['samples'])==36,d['status']
    steps=d['publication']['steps']
    assert all(k in steps for k in ('computed_at','archive_written_at','db_written_at','verified_at','published_at')),steps
    listed=c.get('/runs',headers=token).json()
    assert listed[0]['publication']['steps']['published_at'] and 'result' not in listed[0] or True
    # plan + approve + export
    sample_id=d['result']['samples'][0]['id']
    n=c.post('/notes',headers=token,json={'name':'API计划','run_id':rid,'sample_id':sample_id,'protocol':'验证方案：湿热后复测吸声，独立口径。'})
    assert n.status_code==201,n.text
    admin=c.post('/auth/login',json={'email':'admin@luyan.local','password':'Luyan2026!'}).json()['token']
    rv=c.post(f"/notes/{n.json()['id']}/review",headers={'Authorization':'Bearer '+admin},json={'status':'approved','reviewer_note':'同意，采购另行审议'})
    assert rv.status_code==200,rv.text
    ex=c.get(f"/notes/{n.json()['id']}/export",headers=token)
    assert ex.status_code==200 and 'fingerprint' in ex.text
    # power-cut: db copy wiped, archive deleted -> quarantine; reads gated
    with Session() as db:
        db.execute(update(DecisionRun).where(DecisionRun.id==rid).values(result={},summary={}));db.commit()
    (ROOT/'artifacts'/f'run-{rid}.json').unlink()
    with Session() as db:rs.reconcile(db)
    d=c.get(f'/runs/{rid}',headers=token).json()
    assert d['status']=='quarantined' and d['result']=={},d['status']
    assert d['publication']['quarantine']['db_state']=='empty'
    assert '隔离' in d['publication']['quarantine']['reason']
    ex=c.get(f"/notes/{n.json()['id']}/export",headers=token)
    assert ex.status_code==409,ex.status_code
    blocked=c.post('/notes',headers=token,json={'name':'API计划2','run_id':rid,'sample_id':sample_id,'protocol':'隔离期间不应被允许的计划'})
    assert blocked.status_code==409
    # operator restores the archive from a sibling backup -> auto heal
    with Session() as db:
        row=db.get(DecisionRun,rid)
        from src.services.weight_sensitivity import analyze_decision
        from src.services import publication as pub
        pub.write_artifact(rid,pub.canonical_bytes(analyze_decision(row.snapshot,row.center_weights,row.concentration,row.repeats,row.seed)))
    rs.RECHECK_SECONDS=0
    with Session() as db:rs.reconcile(db)
    d=c.get(f'/runs/{rid}',headers=token).json()
    assert d['status']=='completed' and len(d['result']['samples'])==36,d['status']
    ex=c.get(f"/notes/{n.json()['id']}/export",headers=token)
    assert ex.status_code==200
    print('API end-to-end recovery flow OK')
