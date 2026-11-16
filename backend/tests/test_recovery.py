"""Integration tests for recoverable dual-write publication.

Runs the real service layer against SQLite and a temp artifact dir:
every interruption point (compute done, single-side write, publish confirm),
fencing of late/concurrent executors, quarantine of unusable copies, and
automatic healing. Run from backend/:  python3.11 tests/test_recovery.py
"""
import hashlib,json,os,sys,shutil
from pathlib import Path

ROOT=Path('/tmp/recovery-test')
shutil.rmtree(ROOT,ignore_errors=True)
(ROOT/'artifacts').mkdir(parents=True)
os.environ['DATABASE_URL']=f'sqlite:///{ROOT}/test.db'
os.environ['ARTIFACT_DIR']=str(ROOT/'artifacts')
os.environ.setdefault('REDIS_URL','redis://localhost:6379/0')
sys.path.insert(0,str(Path(__file__).resolve().parent.parent))

from sqlalchemy import update
from src.config.database import Base,engine,Session
from src.models import DecisionRun,CandidateStudy,MaterialSample,Criterion,TrialNote
from src.schemas.run import RunInput
from src.services import run_service as rs
from src.services import publication as pub
from src.services import note_service
from src.services.weight_sensitivity import analyze_decision
from src.controllers import run_controller
from fastapi import HTTPException

FAILURES=[]
def check(name,cond,detail=''):
    print(('PASS' if cond else 'FAIL'),name,'' if cond else f'-- {detail}')
    if not cond:FAILURES.append(name)

STUDY_ID=0
def reset():
    global STUDY_ID
    with Session() as db:
        for t in (TrialNote,DecisionRun,Criterion,CandidateStudy,MaterialSample):db.query(t).delete()
        db.commit()
    for f in (ROOT/'artifacts').glob('*'):f.unlink()
    with Session() as db:
        samples=[MaterialSample(code=f'LY{i:02d}',name=f'试样{i}',family='芦芯',absorption=.5+i*.1,impact=3+i,carbon=10-i,price=100+i*7,thickness=20+i,batch='B1') for i in range(1,5)]
        db.add_all(samples);db.flush()
        study=CandidateStudy(code='T1',name='测试研究',context='ctx',source_file='t.csv',material_ids=[s.id for s in samples])
        db.add(study);db.flush()
        db.add_all([Criterion(study_id=study.id,code='absorption',name='吸声',unit='αw',direction='benefit',center_weight=.6),Criterion(study_id=study.id,code='price',name='单价',unit='元',direction='cost',center_weight=.4)])
        db.commit();STUDY_ID=study.id

def new_run(name='r1',repeats=1000):
    with Session() as db:return rs.create_run(db,RunInput(name=name,study_id=STUDY_ID,repeats=repeats)).id

def row(id):
    db=Session();r=db.get(DecisionRun,id);db.expunge(r);db.close();return r

def set_run(id,**fields):
    with Session() as db:db.execute(update(DecisionRun).where(DecisionRun.id==id).values(**fields));db.commit()

def reconcile():
    with Session() as db:rs.reconcile(db)

def forbid_compute(*a,**k):raise AssertionError('已核实的计算不应重算')

Base.metadata.create_all(engine)
reset()

# --- pure publication primitives ---
probe={'b':1,'a':[.1,2]}
check('canonical serialization is deterministic',pub.canonical_bytes(probe)==b'{"a":[0.1,2],"b":1}')
sha=pub.write_artifact(999,pub.canonical_bytes(probe))
state,parsed,file_sha=pub.read_artifact(999)
check('artifact round-trip',state=='ok' and parsed==probe and file_sha==sha)
check('missing artifact state',pub.read_artifact(998)[0]=='missing')
(ROOT/'artifacts'/'run-998.json').write_text('{broken')
check('corrupt artifact state',pub.read_artifact(998)[0]=='corrupt')

# --- happy path: full publish protocol ---
rid=new_run()
with Session() as db:rs.execute_run(db,rid)
r=row(rid)
check('happy path completes',r.status=='completed',r.status)
steps=(r.publication or {}).get('steps') or {}
check('all checkpoints recorded',all(k in steps for k in ('computed_at','archive_written_at','archive_sha256','db_written_at','verified_at','published_at')),str(sorted(steps)))
check('artifact exists and matches db copy',pub.inspect_copies(row(rid))['consistent'])
check('fingerprint matches frozen inputs',r.result['fingerprint']==pub.expected_fingerprint(r))
with Session() as db:out=run_controller.detail(db,rid)
check('published result served to pages',len(out.result.get('samples',[]))==4)

# fingerprint regression: identical to the pre-refactor formula
canonical={'version':'vector-topsis-dirichlet-v1','snapshot':r.snapshot,'center_weights':r.center_weights,'concentration':r.concentration,'repeats':r.repeats,'seed':r.seed}
legacy=hashlib.sha256(json.dumps(canonical,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
check('fingerprint formula unchanged',legacy==r.result['fingerprint'])

# determinism: same inputs -> identical bytes
again=analyze_decision(r.snapshot,r.center_weights,r.concentration,r.repeats,r.seed)
check('recomputation is byte-identical',pub.canonical_bytes(again)==pub.canonical_bytes(r.result))

# --- interruption right after compute: publishing, nothing ever written ---
reset();rid=new_run()
set_run(rid,status='publishing',claim_id='dead',heartbeat_at=None,publication={'log':[{'at':'2026-11-16T09:00:00','event':'computed','detail':'计算完成'}]})
reconcile()
r=row(rid)
check('crash after compute recovers via recompute',r.status=='completed' and pub.inspect_copies(r)['consistent'],r.status)
check('recompute logged',any(e['event']=='recompute' for e in r.publication['log']))

# --- interruption after archive write only: repair db from file, no recompute ---
reset();rid=new_run()
with Session() as db:rs.execute_run(db,rid)
saved=row(rid).result
set_run(rid,status='publishing',result={},summary={},claim_id='dead',heartbeat_at=None)
real=rs.analyze_decision;rs.analyze_decision=forbid_compute
reconcile()
rs.analyze_decision=real
r=row(rid)
check('single-side (file) repairs db without recompute',r.status=='completed' and r.result==saved,r.status)
check('repair from file logged',any(e['event']=='repaired_from_file' for e in r.publication['log']))

# --- interruption after db write only: rewrite file, no recompute ---
set_run(rid,status='publishing',claim_id='dead',heartbeat_at=None)
(ROOT/'artifacts'/f'run-{rid}.json').unlink()
rs.analyze_decision=forbid_compute
reconcile()
rs.analyze_decision=real
r=row(rid)
check('single-side (db) rewrites file without recompute',r.status=='completed' and pub.inspect_copies(r)['consistent'],r.status)
check('repair from db logged',any(e['event']=='repaired_from_db' for e in r.publication['log']))

# --- interruption at publish confirm: both copies good, status not yet completed ---
set_run(rid,status='publishing',claim_id='dead',heartbeat_at=None)
rs.analyze_decision=forbid_compute
reconcile()
rs.analyze_decision=real
r=row(rid)
check('crash at publish confirm still publishes',r.status=='completed' and pub.inspect_copies(r)['consistent'],r.status)

# --- fencing: a late old executor cannot publish ---
with Session() as db:claim_row,claim_a=rs.claim_run(db,rid,('completed',))
with Session() as db:row_b,claim_b=rs.claim_run(db,rid,('completed',))
check('second claim fences the first',bool(claim_a) and bool(claim_b) and claim_a!=claim_b and row_b.epoch==claim_row.epoch+1)
try:
    with Session() as db:rs.guarded(db,rid,claim_a,error='late write')
    check('late executor write rejected',False)
except rs.ClaimLost:check('late executor write rejected',True)
check('late write did not land',row(rid).error!='late write')

# --- fresh claim is not displaced while alive; stale claim is taken over ---
set_run(rid,status='running',claim_id='alive',heartbeat_at=rs._now(),result={})
reconcile()
check('live executor not displaced',row(rid).status=='running')
set_run(rid,heartbeat_at=None)
reconcile()
check('stale executor taken over and completed',row(rid).status=='completed')

# --- both copies unusable on a completed run: quarantine, never serve empty ---
reset();rid=new_run()
with Session() as db:rs.execute_run(db,rid)
with Session() as db:
    note=TrialNote(name='计划1',run_id=rid,sample_id=1,protocol='湿热复测方案与独立口径说明',status='approved',reviewer_note='同意',reviewed_by='admin@luyan.local')
    db.add(note);db.commit();note_id=note.id
set_run(rid,result={},summary={})
(ROOT/'artifacts'/f'run-{rid}.json').unlink()
reconcile()
r=row(rid)
q=(r.publication or {}).get('quarantine') or {}
check('both copies lost -> quarantined',r.status=='quarantined',r.status)
check('quarantine records both states',q.get('db_state')=='empty' and q.get('file_state')=='missing',str(q))
check('quarantine explains itself',bool(q.get('reason')) and '隔离' in q.get('reason',''))
with Session() as db:out=run_controller.detail(db,rid)
check('quarantined result not served as empty completion',out.result=={})
try:
    with Session() as db:note_service.export_note(db,note_id)
    check('export blocked while quarantined',False)
except HTTPException as e:check('export blocked while quarantined',e.status_code==409)
try:
    with Session() as db:note_service.create_note(db,type('N',(),{'run_id':rid,'sample_id':1,'name':'x','protocol':'p'})())
    check('new plan blocked while quarantined',False)
except HTTPException as e:check('new plan blocked while quarantined',e.status_code==409)

# --- quarantine heals automatically once a copy returns ---
restored=analyze_decision(r.snapshot,r.center_weights,r.concentration,r.repeats,r.seed)
pub.write_artifact(rid,pub.canonical_bytes(restored))
reconcile()
check('quarantine recheck is throttled',row(rid).status=='quarantined')
rs.RECHECK_SECONDS=0
reconcile()
r=row(rid)
check('quarantine heals when a copy returns',r.status=='completed' and pub.inspect_copies(r)['consistent'],r.status)
check('healing keeps quarantine history',any(e['event']=='quarantined' for e in r.publication['log']) and any(e['event']=='recovered' for e in r.publication['log']))
with Session() as db:csv_out=note_service.export_note(db,note_id)
check('approved plan export works again after healing','fingerprint' in csv_out)

# --- corrupt file repaired from db; corrupt db repaired from file ---
(ROOT/'artifacts'/f'run-{rid}.json').write_text('{"not":"the result"}')
reconcile()
r=row(rid)
check('corrupt file repaired from db',r.status=='completed' and pub.inspect_copies(r)['consistent'],r.status)
tampered=json.loads(json.dumps(r.result));tampered['samples'][0]['score']=.012345
set_run(rid,result=tampered)
reconcile()
r=row(rid)
check('corrupt db repaired from file',r.status=='completed' and r.result['samples'][0]['score']!=.012345,r.status)

# --- both sides valid but different, no recorded hash: quarantine as undecidable ---
reset();rid=new_run()
with Session() as db:rs.execute_run(db,rid)
r=row(rid)
other=json.loads(json.dumps(r.result));other['draws'][0]['scores'][0]+=.25
(ROOT/'artifacts'/f'run-{rid}.json').write_text(json.dumps(other,ensure_ascii=False,allow_nan=False))
set_run(rid,publication={})
reconcile()
r=row(rid)
q=(r.publication or {}).get('quarantine') or {}
check('undecidable mismatch quarantined',r.status=='quarantined' and q.get('db_state')=='mismatch',f"{r.status} {q.get('db_state')}")

# --- recorded archive hash arbitrates a mismatch toward the file ---
file_sha=hashlib.sha256(json.dumps(other,ensure_ascii=False,allow_nan=False).encode()).hexdigest()
set_run(rid,publication={'steps':{'archive_sha256':file_sha}})
reconcile()
r=row(rid)
check('recorded hash arbitrates mismatch',r.status=='completed' and r.result['draws'][0]['scores'][0]==other['draws'][0]['scores'][0],r.status)

# --- legacy completed run: verification backfilled, stays readable ---
reset();rid=new_run()
with Session() as db:rs.execute_run(db,rid)
set_run(rid,publication={})
reconcile()
r=row(rid)
check('legacy run backfilled and still completed',r.status=='completed' and bool((r.publication.get('steps') or {}).get('verified_at')),r.status)
with Session() as db:out=run_controller.detail(db,rid)
check('legacy result still served',len(out.result.get('samples',[]))==4)

# --- unverified result is never served before publication ---
reset();rid=new_run()
set_run(rid,status='publishing',result={'samples':[1]},claim_id='dead',heartbeat_at=None)
with Session() as db:out=run_controller.detail(db,rid)
check('unpublished result hidden from pages',out.result=={})
reconcile()
r=row(rid)
check('corrupt never-written copies trigger recompute',r.status=='completed' and pub.inspect_copies(r)['consistent'],r.status)

# --- publish-phase IO trouble pauses recoverably instead of failing terminally ---
reset();rid=new_run()
os.chmod(ROOT/'artifacts',0o500)
with Session() as db:rs.execute_run(db,rid)
r=row(rid)
check('publish io trouble stays recoverable',r.status=='publishing' and '发布中断' in r.error,r.status+' '+r.error)
check('publish interruption journaled',any(e['event']=='publish_interrupted' for e in (r.publication or {}).get('log',[])))
os.chmod(ROOT/'artifacts',0o700)
set_run(rid,heartbeat_at=None)
reconcile()
r=row(rid)
check('publish resumes after fs recovers',r.status=='completed' and pub.inspect_copies(r)['consistent'],r.status)

print()
if FAILURES:print(f'{len(FAILURES)} FAILURES: {FAILURES}');sys.exit(1)
print('all recovery tests passed')
