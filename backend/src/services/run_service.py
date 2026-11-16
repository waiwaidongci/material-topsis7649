import logging,os,uuid
from datetime import datetime
from pathlib import Path
from sqlalchemy import select,func,update
from sqlalchemy.orm import defer
from fastapi import HTTPException
from redis import Redis
from src.config.settings import REDIS_URL
from src.models import DecisionRun,MaterialSample
from src.services.study_service import get_study
from src.services.criterion_service import list_criteria
from src.services.weight_sensitivity import analyze_decision
from src.services import publication as pub
redis=Redis.from_url(REDIS_URL)
STALE_SECONDS=int(os.getenv('RUN_CLAIM_STALE_SECONDS','30'))
RECHECK_SECONDS=int(os.getenv('RUN_RECHECK_SECONDS','30'))
ACTIVE_STATES=('queued','running','publishing')

class ClaimLost(Exception):pass

def _now():return datetime.utcnow()
def _iso():return _now().isoformat(timespec='seconds')
def _stale(row):return row.heartbeat_at is None or (_now()-row.heartbeat_at).total_seconds()>STALE_SECONDS

def list_runs(db):return db.scalars(select(DecisionRun).options(defer(DecisionRun.result),defer(DecisionRun.snapshot)).order_by(DecisionRun.id.desc())).all()
def get_run(db,id):
    item=db.get(DecisionRun,id)
    if not item:raise HTTPException(404,'权衡任务不存在')
    return item

def create_run(db,data):
    study=get_study(db,data.study_id)
    if db.scalar(select(DecisionRun).where(DecisionRun.name==data.name)):raise HTTPException(409,'任务名称已存在')
    if db.scalar(select(func.count()).select_from(DecisionRun).where(DecisionRun.status.in_(ACTIVE_STATES)))>=3:raise HTTPException(429,'已有3项活动任务，请等待完成')
    criteria=[{k:getattr(c,k) for k in ('id','code','name','unit','direction','center_weight')} for c in list_criteria(db,study.id)]
    weights=data.center_weights or {c['code']:c['center_weight'] for c in criteria}
    if set(weights)!={c['code'] for c in criteria}:raise HTTPException(422,'中心权重须逐一覆盖本研究指标')
    fields=('id','code','name','family','absorption','impact','carbon','price','thickness','batch')
    samples=[{k:getattr(s,k) for k in fields} for s in db.scalars(select(MaterialSample).where(MaterialSample.id.in_(study.material_ids)).order_by(MaterialSample.code)).all()]
    snapshot={'study':{'id':study.id,'code':study.code,'name':study.name,'source_file':study.source_file,'material_ids':study.material_ids},'samples':samples,'criteria':criteria}
    publication={'log':[{'at':_iso(),'event':'created','detail':'任务已排队，等待执行者接手'}]}
    item=DecisionRun(**{**data.model_dump(),'center_weights':weights},snapshot=snapshot,summary={'samples':len(samples),'criteria':len(criteria)},status='queued',publication=publication);db.add(item);db.commit()
    try:redis.lpush('material:runs',str(item.id))
    except Exception:pass
    return item

# --- fencing: only the current claim holder may advance or publish a run ---

def claim_run(db,id,from_states,status=None):
    """Atomically take over a run by bumping its epoch; returns (row, claim_id) or (None, '').

    A running/publishing run with a live heartbeat is left alone, so a
    concurrent or late executor can never displace healthy work.
    """
    row=db.get(DecisionRun,id)
    if not row or row.status not in from_states:return None,''
    if row.status in ('running','publishing') and row.claim_id and not _stale(row):return None,''
    claim=uuid.uuid4().hex
    values={'epoch':row.epoch+1,'claim_id':claim,'heartbeat_at':_now()}
    if status:values['status']=status
    res=db.execute(update(DecisionRun).where(DecisionRun.id==id,DecisionRun.epoch==row.epoch).values(**values))
    db.commit()
    if res.rowcount!=1:db.rollback();return None,''
    row=db.get(DecisionRun,id);db.refresh(row)
    return row,claim

def guarded(db,id,claim,**fields):
    """Commit a checkpoint only if we still hold the claim; otherwise the run was taken over."""
    res=db.execute(update(DecisionRun).where(DecisionRun.id==id,DecisionRun.claim_id==claim).values(**fields))
    db.commit()
    if res.rowcount!=1:raise ClaimLost(f'任务 {id} 已由其他执行者接手')

def _heartbeat(db,id,claim):
    Path('/tmp/heartbeat').touch()
    guarded(db,id,claim,heartbeat_at=_now())

# --- publication journal ---

def _log(publication,event,detail=''):
    publication={**publication} if isinstance(publication,dict) else {}
    entries=list(publication.get('log') or [])
    entries.append({'at':_iso(),'event':event,'detail':detail})
    publication['log']=entries[-30:]
    return publication

def _step(publication,key,value=None):
    publication={**publication} if isinstance(publication,dict) else {}
    steps=dict(publication.get('steps') or {})
    steps[key]=value if value is not None else _iso()
    publication['steps']=steps
    return publication

def _summary_of(result):
    return {'samples':len(result['samples']),'criteria':len(result['criteria']),'all_indistinguishable':result['all_indistinguishable'],'fingerprint':result['fingerprint']}

# --- dual-copy publish protocol; every guarded call is a recoverable checkpoint ---

def _publish_copies(db,id,claim,result,publication):
    sha=pub.write_artifact(id,pub.canonical_bytes(result))
    publication=_log(_step(_step(publication,'archive_written_at'),'archive_sha256',sha),'archive_written',f'留档 run-{id}.json 已落盘 sha256 {sha[:12]}…')
    guarded(db,id,claim,publication=publication)
    publication=_log(_step(publication,'db_written_at'),'db_written','结果写入数据库副本')
    guarded(db,id,claim,result=result,summary=_summary_of(result),publication=publication)
    return publication

def _publish(db,id,claim,publication):
    publication=_log(_step(publication,'published_at'),'published','双写核验一致，结果发布供页面与计划导出读取')
    guarded(db,id,claim,status='completed',error='',publication=publication)

def _quarantine(db,id,claim,publication,report):
    reason=pub.quarantine_reason(report)
    quarantine={'at':_iso(),'reason':reason,'db_state':report['db_state'],'file_state':report['file_state'],'last_check_at':_iso()}
    publication=_log({**publication,'quarantine':quarantine},'quarantined',reason)
    guarded(db,id,claim,status='quarantined',error=reason[:600],publication=publication)
    return publication

def _repair(db,id,claim,publication,report,source):
    """Rewrite the lost side from the surviving copy; never recomputes verified data."""
    result=report['db_result'] if source=='db' else report['file_result']
    if source=='db':
        sha=pub.write_artifact(id,pub.canonical_bytes(result))
        publication=_log(_step(publication,'archive_sha256',sha),'repaired_from_db',f'留档文件{pub.STATE_LABELS[report["file_state"]]}，已从数据库副本重写 sha256 {sha[:12]}…')
        guarded(db,id,claim,publication=publication)
    else:
        publication=_log(publication,'repaired_from_file',f'数据库副本{pub.STATE_LABELS[report["db_state"]]}，已从留档文件恢复')
        guarded(db,id,claim,result=result,summary=_summary_of(result),publication=publication)
    return publication

def _ensure_consistent(db,id,claim,row,publication):
    """Verify both copies, repairing from the survivor; quarantine when neither is usable.

    Returns (publication, True) once the two copies verify consistent,
    otherwise quarantines the run and returns (publication, False).
    """
    db.refresh(row)
    report=pub.inspect_copies(row)
    if report['consistent']:
        return _log(_step(publication,'verified_at'),'verified','数据库与留档文件内容核验一致'),True
    source=pub.repair_source(report,publication)
    if source:
        publication=_repair(db,id,claim,publication,report,source)
        db.refresh(row)
        report=pub.inspect_copies(row)
        if report['consistent']:
            return _log(_step(publication,'verified_at'),'verified','修复后数据库与留档文件内容核验一致'),True
    return _quarantine(db,id,claim,publication,report),False

# --- execution and recovery ---

def _fail(db,id,claim,exc):
    try:guarded(db,id,claim,status='failed',error=str(exc)[:600])
    except ClaimLost:db.rollback()

def _pause_publish(db,id,claim,publication,exc):
    """Publish-phase IO trouble is recoverable: stay in publishing, note it, let the reconciler resume."""
    try:guarded(db,id,claim,error=('发布中断，等待自动恢复：'+str(exc))[:600],publication=_log(publication,'publish_interrupted',str(exc)[:300]))
    except ClaimLost:db.rollback()

def _publish_flow(db,row,claim,result,publication):
    publication=_log(_step(publication,'computed_at'),'computed','计算完成，进入双写核验发布')
    guarded(db,row.id,claim,status='publishing',publication=publication)
    publication=_publish_copies(db,row.id,claim,result,publication)
    publication,ok=_ensure_consistent(db,row.id,claim,row,publication)
    if ok:_publish(db,row.id,claim,publication)

def execute_run(db,id):
    row,claim=claim_run(db,id,('queued','running'),status='running')
    if not row:return
    publication=_log(dict(row.publication or {}),'claimed',f'执行者 {claim[:8]} 接手（纪元 {row.epoch}）')
    try:
        guarded(db,id,claim,publication=publication)
        result=analyze_decision(row.snapshot,row.center_weights,row.concentration,row.repeats,row.seed,lambda:_heartbeat(db,id,claim))
    except ClaimLost:db.rollback();return
    except Exception as exc:
        db.rollback();_fail(db,id,claim,exc);return
    try:_publish_flow(db,row,claim,result,publication)
    except ClaimLost:db.rollback()
    except Exception as exc:db.rollback();_pause_publish(db,id,claim,publication,exc)

def _recover_publishing(db,id):
    """Resume an interrupted publish: verify copies, repair one side from the
    other, recompute only when nothing was ever written, else quarantine."""
    row,claim=claim_run(db,id,('publishing',))
    if not row:return
    publication=_log(dict(row.publication or {}),'recovery_claimed',f'发布流程中断，执行者 {claim[:8]} 接手恢复（纪元 {row.epoch}）')
    try:
        guarded(db,id,claim,publication=publication,heartbeat_at=_now())
        report=pub.inspect_copies(row)
        steps=publication.get('steps') or {}
        ever_wrote=bool(steps.get('archive_written_at') or steps.get('db_written_at'))
        if report['consistent'] or pub.repair_source(report,publication) or ever_wrote:
            publication,ok=_ensure_consistent(db,id,claim,row,publication)
            if ok:_publish(db,id,claim,publication)
            return
        publication=_log(publication,'recompute','计算完成后未写入任何副本即中断，按冻结快照复算')
        guarded(db,id,claim,status='running',publication=publication)
        try:
            result=analyze_decision(row.snapshot,row.center_weights,row.concentration,row.repeats,row.seed,lambda:_heartbeat(db,id,claim))
        except ClaimLost:db.rollback();return
        except Exception as exc:
            db.rollback();_fail(db,id,claim,exc);return
        try:_publish_flow(db,row,claim,result,publication)
        except ClaimLost:db.rollback()
        except Exception as exc:db.rollback();_pause_publish(db,id,claim,publication,exc)
    except ClaimLost:db.rollback()

def _recover_completed(db,id):
    """Backfill or re-check verification for published runs; repair a lost copy
    from the survivor, quarantine when both are unusable."""
    row,claim=claim_run(db,id,('completed',))
    if not row:return
    publication=dict(row.publication or {})
    first=not (publication.get('steps') or {}).get('verified_at')
    try:
        report=pub.inspect_copies(row)
        if report['consistent']:
            publication=_step(publication,'last_verify_at')
            if first:publication=_log(_step(publication,'verified_at'),'verified','既有结果补录核验：数据库与留档文件一致')
            if not (publication.get('steps') or {}).get('published_at'):publication=_step(publication,'published_at')
            guarded(db,id,claim,publication=publication)
            return
        if first:publication=_log(publication,'recovery_claimed','既有结果缺少核验记录，开始补录')
        publication,ok=_ensure_consistent(db,id,claim,row,publication)
        if ok:
            publication=_log(publication,'recovered','已完成结果副本修复并重新核验一致')
            _publish(db,id,claim,publication)
    except ClaimLost:db.rollback()

def _recover_quarantined(db,id):
    """Re-examine a quarantined run; heal automatically once a usable copy returns."""
    row,claim=claim_run(db,id,('quarantined',))
    if not row:return
    publication=dict(row.publication or {})
    try:
        report=pub.inspect_copies(row)
        if report['consistent'] or pub.repair_source(report,publication):
            publication=_log(publication,'recovery_claimed','隔离复查发现可用副本，开始恢复')
            publication,ok=_ensure_consistent(db,id,claim,row,publication)
            if ok:
                publication=_log(publication,'recovered','隔离恢复：副本核验一致，重新发布')
                _publish(db,id,claim,publication)
            return
        quarantine={**(publication.get('quarantine') or {}),'last_check_at':_iso()}
        guarded(db,id,claim,publication={**publication,'quarantine':quarantine})
    except ClaimLost:db.rollback()

def _recheck_due(row,publication,checked_key):
    if not isinstance(publication,dict):return True
    at=(publication.get('steps') or {}).get(checked_key) or (publication.get('quarantine') or {}).get(checked_key)
    if not at:return True
    try:return (_now()-datetime.fromisoformat(at)).total_seconds()>RECHECK_SECONDS
    except ValueError:return True

def reconcile(db):
    """One recovery pass over runs that may need it; returns True when it worked.

    Covers every interruption point: mid-compute (running), mid-publish with
    zero or one copy written (publishing), published runs whose copies drift
    (completed), and quarantined runs awaiting a restored copy.
    """
    worked=False
    rows=db.scalars(select(DecisionRun).where(DecisionRun.status.in_(('running','publishing','completed','quarantined'))).order_by(DecisionRun.id)).all()
    for row in rows:
        try:
            if row.status=='running':
                if row.claim_id and not _stale(row):continue
                execute_run(db,row.id);worked=True
            elif row.status=='publishing':
                if row.claim_id and not _stale(row):continue
                _recover_publishing(db,row.id);worked=True
            elif row.status=='completed':
                publication=row.publication or {}
                verified=bool((publication.get('steps') or {}).get('verified_at'))
                if not verified or _recheck_due(row,publication,'last_verify_at'):_recover_completed(db,row.id);worked=True
            elif row.status=='quarantined':
                if _recheck_due(row,row.publication or {},'last_check_at'):_recover_quarantined(db,row.id);worked=True
        except Exception:
            db.rollback();logging.exception('reconcile pass failed for run %s',row.id)
    return worked
