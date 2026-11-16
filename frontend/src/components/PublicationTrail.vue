<script setup lang="ts">
import {computed} from 'vue';
import type {Publication} from '../types/domain';
import {date} from '../utils/format';
const props=defineProps<{publication?:Publication;status:string}>();
const STEPS:[string,string][]=[['computed_at','计算完成'],['archive_written_at','留档落盘'],['db_written_at','结果入库'],['verified_at','双写核验'],['published_at','发布']];
const eventLabels:Record<string,string>={created:'已排队',claimed:'执行者接手',computed:'计算完成',archive_written:'留档落盘',db_written:'结果入库',verified:'双写核验一致',published:'发布完成',repaired_from_db:'从数据库修复留档',repaired_from_file:'从留档修复数据库',quarantined:'结果隔离',recovered:'恢复并重新发布',recompute:'按冻结快照复算',recovery_claimed:'恢复接手',publish_interrupted:'发布中断待恢复'};
const stateLabels:Record<string,string>={ok:'正常',empty:'结果为空',missing:'留档缺失',corrupt:'内容损坏',mismatch:'两侧不一致'};
const steps=computed(()=>props.publication?.steps||{});
const log=computed(()=>[...(props.publication?.log||[])].reverse().slice(0,5));
const quarantine=computed(()=>props.publication?.quarantine||null);
</script>
<template><aside class="publication-trail"><p class="eyebrow">PUBLICATION TRAIL / 双写发布留痕</p><ol class="trail-steps"><li v-for="[key,label] in STEPS" :key="key" :class="{done:!!steps[key]}"><i/><span>{{label}}</span><small v-if="steps[key]">{{date(String(steps[key]))}}</small></li></ol><div v-if="quarantine" class="quarantine-box"><b>隔离依据</b><p>{{quarantine.reason}}</p><small>数据库副本：{{stateLabels[quarantine.db_state]||quarantine.db_state}} · 留档文件：{{stateLabels[quarantine.file_state]||quarantine.file_state}} · {{date(quarantine.at)}}起隔离；任一副本恢复后自动重新核验发布，期间不把空结果当完成。</small></div><ul v-if="log.length" class="trail-log"><li v-for="(e,i) in log" :key="i"><b>{{eventLabels[e.event]||e.event}}</b><span>{{e.detail}}</span><small>{{date(e.at)}}</small></li></ul></aside>
</template>
