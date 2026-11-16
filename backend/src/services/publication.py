"""Recoverable dual-copy publication for decision results.

A run result lives in two places: the decision_runs.result JSON column and the
archive file /artifacts/run-<id>.json. A power cut may leave only one copy.
Results are served to pages and plan exports only after both copies verify
consistent; one missing or corrupt copy is repaired from the survivor; when
both are unusable the run is quarantined with the observed states so an empty
result is never treated as complete.
"""
import hashlib,json,os,tempfile
from pathlib import Path

REQUIRED_KEYS=('samples','criteria','columns','center_weights','alpha','concentration','repeats','seed','ideal_best','ideal_worst','all_indistinguishable','draws','fingerprint','snapshot')
STATE_LABELS={'ok':'正常','empty':'结果为空','missing':'留档缺失','corrupt':'内容损坏','mismatch':'两侧不一致'}

def artifact_dir():return Path(os.getenv('ARTIFACT_DIR','/artifacts'))
def artifact_path(run_id):return artifact_dir()/f'run-{run_id}.json'

def canonical_bytes(result):
    """Deterministic serialization; archive files are stored in exactly this form."""
    return json.dumps(result,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()

def content_hash(result):return hashlib.sha256(canonical_bytes(result)).hexdigest()

def fingerprint_inputs(snapshot,center_weights,concentration,repeats,seed):
    canonical={'version':'vector-topsis-dirichlet-v1','snapshot':snapshot,'center_weights':center_weights,'concentration':concentration,'repeats':repeats,'seed':seed}
    return hashlib.sha256(json.dumps(canonical,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()

def expected_fingerprint(run):return fingerprint_inputs(run.snapshot,run.center_weights,run.concentration,run.repeats,run.seed)

def validate_structure(result,run):
    """Return '' when a copy is internally consistent with the frozen inputs, else a short reason."""
    if not isinstance(result,dict) or not result:return '结果为空'
    missing=[k for k in REQUIRED_KEYS if k not in result]
    if missing:return '缺少字段:'+','.join(missing[:4])
    try:
        if result['fingerprint']!=expected_fingerprint(run):return '指纹与冻结输入不符'
        if result['snapshot']!=run.snapshot:return '内嵌快照与任务冻结内容不符'
        if result['repeats']!=run.repeats or result['seed']!=run.seed:return '情景数或种子与任务不符'
        samples,draws,criteria=result['samples'],result['draws'],result['criteria']
        if len(samples)!=len(run.snapshot['samples']):return '候选数量与快照不符'
        if len(draws)!=result['repeats']:return '情景留档数量不完整'
        if len(result['center_weights'])!=len(criteria):return '中心权重数量与指标不符'
        n_samples,n_criteria=len(samples),len(criteria)
        for d in draws:
            if len(d['weights'])!=n_criteria or len(d['scores'])!=n_samples or len(d['ranks'])!=n_samples or len(d['first_share'])!=n_samples:return '情景留档结构不完整'
    except (KeyError,TypeError,IndexError):return '结果结构损坏'
    return ''

def write_artifact(run_id,data):
    """Atomic write: temp file, fsync, rename, directory fsync. Returns sha256 of the bytes."""
    artifact_dir().mkdir(parents=True,exist_ok=True)
    for stale in artifact_dir().glob(f'.run-{run_id}-*.tmp'):
        try:stale.unlink()
        except OSError:pass
    fd,tmp=tempfile.mkstemp(dir=artifact_dir(),prefix=f'.run-{run_id}-',suffix='.tmp')
    try:
        with os.fdopen(fd,'wb') as f:
            f.write(data);f.flush();os.fsync(f.fileno())
        os.replace(tmp,artifact_path(run_id))
    except BaseException:
        try:os.unlink(tmp)
        except OSError:pass
        raise
    try:
        dfd=os.open(str(artifact_dir()),os.O_RDONLY)
        try:os.fsync(dfd)
        finally:os.close(dfd)
    except OSError:pass
    return hashlib.sha256(data).hexdigest()

def read_artifact(run_id):
    """Return (state, parsed|None, sha256|None) with state in ok|missing|corrupt."""
    path=artifact_path(run_id)
    try:data=path.read_bytes()
    except OSError:return 'missing',None,None
    sha=hashlib.sha256(data).hexdigest()
    try:parsed=json.loads(data)
    except (ValueError,UnicodeDecodeError):return 'corrupt',None,sha
    if not isinstance(parsed,dict):return 'corrupt',None,sha
    return 'ok',parsed,sha

def inspect_copies(run):
    """Classify both copies of a run result.

    States: ok / empty (db only) / missing (file only) / corrupt / mismatch
    (both individually valid but different). Payloads and content hashes are
    included for copies that are individually valid so a survivor can repair
    the other side without recomputation.
    """
    report={'db_state':'empty','file_state':'missing','db_result':None,'file_result':None,'db_hash':None,'file_hash':None,'file_sha':None,'consistent':False}
    db_result=run.result if isinstance(run.result,dict) else {}
    if db_result:
        reason=validate_structure(db_result,run)
        if not reason:
            try:report.update(db_state='ok',db_result=db_result,db_hash=content_hash(db_result))
            except ValueError:report['db_state']='corrupt'
        else:report['db_state']='corrupt'
    file_state,file_result,file_sha=read_artifact(run.id)
    report['file_state'],report['file_sha']=file_state,file_sha
    if file_state=='ok':
        reason=validate_structure(file_result,run)
        if reason:report['file_state']='corrupt'
        else:
            try:report.update(file_result=file_result,file_hash=content_hash(file_result))
            except ValueError:report['file_state']='corrupt'
    if report['db_state']=='ok' and report['file_state']=='ok':
        if report['db_hash']==report['file_hash']:report['consistent']=True
        else:report['db_state']=report['file_state']='mismatch'
    return report

def repair_source(report,publication):
    """Pick the surviving trustworthy side: 'db', 'file', or None.

    When both sides are individually valid but differ, the sha256 recorded at
    archive-write time arbitrates; without a recorded hash the conflict is not
    decidable and the run must be quarantined.
    """
    db_ok=report['db_state']=='ok';file_ok=report['file_state']=='ok'
    if db_ok and not file_ok:return 'db'
    if file_ok and not db_ok:return 'file'
    if report['db_state']=='mismatch' and report['file_state']=='mismatch':
        recorded=(publication.get('steps') or {}).get('archive_sha256') if isinstance(publication,dict) else None
        if recorded:
            if report['file_sha']==recorded:return 'file'
            if report['db_hash']==recorded:return 'db'
    return None

def quarantine_reason(report):
    db_label=STATE_LABELS.get(report['db_state'],report['db_state'])
    file_label=STATE_LABELS.get(report['file_state'],report['file_state'])
    return f'两侧结果均不可核验：数据库副本{db_label}，留档文件{file_label}。已隔离，不把空结果当完成；恢复任一副本后自动重新核验发布。'
