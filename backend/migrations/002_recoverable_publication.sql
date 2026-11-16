-- 可恢复双写发布：执行者围栏与发布留痕列。后端启动时幂等执行同等检查，本文件供审阅与手工升级。
ALTER TABLE decision_runs ADD COLUMN epoch INTEGER NOT NULL DEFAULT 0;
ALTER TABLE decision_runs ADD COLUMN claim_id VARCHAR(64) NOT NULL DEFAULT '';
ALTER TABLE decision_runs ADD COLUMN heartbeat_at DATETIME NULL;
ALTER TABLE decision_runs ADD COLUMN publication JSON NULL;
UPDATE decision_runs SET publication=JSON_OBJECT() WHERE publication IS NULL;
ALTER TABLE decision_runs MODIFY publication JSON NOT NULL;
