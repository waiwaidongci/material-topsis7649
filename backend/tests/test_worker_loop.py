"""Smoke test: the real worker process picks up a queued run and publishes it,
tolerating a missing Redis. Run from backend/: python3.11 tests/test_worker_loop.py
"""
import os,sys,shutil,subprocess,time
from pathlib import Path
ROOT=Path('/tmp/worker-test');shutil.rmtree(ROOT,ignore_errors=True);(ROOT/'artifacts').mkdir(parents=True)
os.environ['DATABASE_URL']=f'sqlite:///{ROOT}/test.db'
os.environ['ARTIFACT_DIR']=str(ROOT/'artifacts')
sys.path.insert(0,str(Path(__file__).resolve().parent.parent))
from src.config.database import Base,engine,Session
from src.models import DecisionRun,CandidateStudy,MaterialSample,Criterion
from src.schemas.run import RunInput
from src.services import run_service as rs
from src.services import publication as pub

Base.metadata.create_all(engine)
with Session() as db:
    samples=[MaterialSample(code=f'LY{i:02d}',name=f'试样{i}',family='芦芯',absorption=.5+i*.1,impact=3+i,carbon=10-i,price=100+i*7,thickness=20+i,batch='B1') for i in range(1,5)]
    db.add_all(samples);db.flush()
    study=CandidateStudy(code='T1',name='测试研究',context='ctx',source_file='t.csv',material_ids=[s.id for s in samples])
    db.add(study);db.flush()
    db.add_all([Criterion(study_id=study.id,code='absorption',name='吸声',unit='αw',direction='benefit',center_weight=.6),Criterion(study_id=study.id,code='price',name='单价',unit='元',direction='cost',center_weight=.4)])
    db.commit();sid=study.id
with Session() as db:rid=rs.create_run(db,RunInput(name='worker冒烟',study_id=sid,repeats=1000)).id

env={**os.environ,'PYTHONPATH':str(Path(__file__).resolve().parent.parent)}
proc=subprocess.Popen([sys.executable,'-m','src.worker'],env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
try:
    deadline=time.time()+90;status=''
    while time.time()<deadline:
        with Session() as db:
            row=db.get(DecisionRun,rid)
            if row:status=row.status
        if status=='completed':break
        time.sleep(1)
    assert status=='completed',f'worker did not complete run, status={status}'
    with Session() as db:
        row=db.get(DecisionRun,rid)
        assert pub.inspect_copies(row)['consistent'],'copies inconsistent after worker publish'
        steps=(row.publication or {}).get('steps') or {}
        assert all(k in steps for k in ('computed_at','archive_written_at','db_written_at','verified_at','published_at')),steps
    print('worker loop smoke test OK')
finally:
    proc.kill();proc.wait()
