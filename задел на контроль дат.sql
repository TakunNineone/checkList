create table rn as
(
with tt as
(
select t.version,t.rinok,t.entity,t.parentrole,table_label,br_label,t.axis,a.arcto root_rulenodes
from
(
select t.version,t.rinok,t.entity,t.parentrole,a.arcfrom table_label,a.arcto br_label,axis
from tableschemas t
join arcs a on a.version=t.version and a.rinok=t.rinok and a.entity=t.entity and a.parentrole=t.parentrole and a.arcto=t.label
and a.arcrole='http://xbrl.org/arcrole/2014/table-breakdown'
where rolefrom='breakdown'
and t.parentrole like 'http://www.cbr.ru/xbrl/nso/ins/rep/2023-03-31/tab/sr_0420154/sr_0420154_R1'
) t 
join arcs a on a.version=t.version and a.rinok=t.rinok and a.entity=t.entity and a.parentrole=t.parentrole and a.arcfrom=t.br_label
order by t.version,t.rinok,t.entity,t.parentrole,a.arcto
),
per as
(
	select distinct rn.version,rn.rinok,rn.entity,rn.parentrole,period_type,rp.start,rp.end 
	from rulenodes rn
	join rulenodes_p rp on rp.version=rn.version and rp.rinok=rn.rinok and rp.entity=rn.entity and rp.parentrole=rn.parentrole and rp.rulenode_id=rn.id
	where rn.parentrole in ('http://www.cbr.ru/xbrl/nso/ins/rep/2023-03-31/tab/sr_0420154/sr_0420154_R1')
),
rn as
(
select r.version,r.rinok,r.entity,r.parentrole,r.id,r.label,dimension,concept,period_type,tag,
case when tag is null and period_type='instant' then per_instant 
	when tag is null and period_type='duration' then per_start else period_start end period_start,
case when tag is null and period_type='instant' then null 
	when tag is null and period_type='duration' then per_end else period_end end period_end,
	arcto child
from
(
select r.version,r.rinok,r.entity,r.parentrole,r.id,r.label,re.dimension||'#'||re.member dimension,
rc.value concept,rs.rulenode_id rs_rulenode_id,rs.tag,coalesce(rs.period_type,e.periodtype) period_type,
case when rs.per_start is null then coalesce(rs.per_instant,rs.per_end) else  rs.per_start end period_start,rs.per_end period_end
from rulenodes r
left join (select rs.*,case when per_start is not null then 'duration' else 'instant' end period_type from rulesets rs) rs on 
	rs.version=r.version and rs.rinok=r.rinok and rs.entity=r.entity and rs.parentrole=r.parentrole and rs.tag=r.tagselector
left join rulenodes_e re on re.version=r.version and re.rinok=r.rinok and re.entity=r.entity and re.parentrole=r.parentrole
and re.rulenode_id=r.id
left join rulenodes_c rc on rc.version=r.version and rc.rinok=r.rinok and rc.entity=r.entity and rc.parentrole=r.parentrole
and rc.rulenode_id=r.id
left join elements e on e.qname=rc.value and e.version=rc.version
where r.parentrole in ('http://www.cbr.ru/xbrl/nso/ins/rep/2023-03-31/tab/sr_0420154/sr_0420154_R1')
order by concept
) r
left join (select distinct version,rinok,entity,parentrole,
max(case when p_type='duration' then per_start end) per_start,
max(case when p_type='duration' then per_end end) per_end,
max(case when p_type='instant' then '$par:refPeriodEnd' end) per_instant
from
(
select rs.*,case when per_start is not null then 'duration' else 'instant' end p_type 
from rulesets rs
) rs
group by version,rinok,entity,parentrole) rs on rs.version=r.version and rs.rinok=r.rinok and rs.entity=r.entity and rs.parentrole=r.parentrole
left join arcs a on a.version=r.version and a.rinok=r.rinok and a.entity=r.entity and a.parentrole=r.parentrole and a.arcfrom=r.label and a.arcrole='http://xbrl.org/arcrole/2014/definition-node-subtree'
order by r.version,r.rinok,r.entity,r.parentrole,r.label
),
an as
(
select an.version,an.rinok,an.entity,an.parentrole,string_agg(dimension,';') an_dim
from aspectnodes an
where an.parentrole in ('http://www.cbr.ru/xbrl/nso/ins/rep/2023-03-31/tab/sr_0420154/sr_0420154_R1')
group by an.version,an.rinok,an.entity,an.parentrole
order by 5
),
rc as
(
select distinct rn.version,rn.rinok,rn.entity,rn.parentrole,rn.label,concept,dimension,period_start,period_end,period_type,arcfrom father
from rn 
join arcs a on a.version=rn.version and a.rinok=rn.rinok and a.entity=rn.entity and a.parentrole=rn.parentrole and a.arcto=rn.label
where concept is not null 
)
,
re as
(
select distinct rn.version,rn.rinok,rn.entity,rn.parentrole,rn.label,concept,dimension,period_start,period_end,period_type,arcfrom father
from rn 
join arcs a on a.version=rn.version and a.rinok=rn.rinok and a.entity=rn.entity and a.parentrole=rn.parentrole and a.arcto=rn.label
where concept is null 
),
r_dim as
(
select tt.version,tt.rinok,tt.entity,tt.parentrole,dimension root_dim,label,father
from tt
left join (select re.version,re.rinok,re.entity,re.parentrole,coalesce(rr.father,re.father) father,coalesce(rr.label,re.father) label,re.dimension from re
left join re rr on rr.label=re.father and rr.version=re.version and rr.rinok=re.rinok and rr.entity=re.entity and rr.parentrole=re.parentrole) re1 
on re1.label=tt.root_rulenodes and re1.version=tt.version and re1.rinok=tt.rinok and re1.entity=tt.entity and re1.parentrole=tt.parentrole
where dimension is not null
),
re_t as
(
select * from re 
where re.label in ('ruleNode_90','ruleNode_50','ruleNode_43','ruleNode_44','ruleNode_45','ruleNode_46','ruleNode_47','ruleNode_48','ruleNode_49','ruleNode_52',
'ruleNode_51','ruleNode_42','ruleNode_40','ruleNode_41','ruleNode_85','ruleNode_87','ruleNode_53','ruleNode_54','ruleNode_55','ruleNode_56')
)
select * from rn

	)


-- select distinct rc.parentrole,concept,
-- coalesce(rc.dimension,'')||case when rc.dimension is null then '' else ';' end||
-- coalesce(dimension_1,'')||case when rc.dimension_1 is null then '' else ';' end|| 
-- coalesce(an_dim,'')||case when an_dim is null then '' else ';' end||
-- coalesce(root_dim,'') dimensions,

-- rc.period_type,
-- coalesce(period_start,start) period_start,
-- case when period_start is null then coalesce(period_end,tr.end) else period_end end period_end
-- from
-- (
-- select rc.version,rc.rinok,rc.entity,rc.parentrole,rc.label,concept,rc.dimension,dimension_1,
-- coalesce(period_start,period_start_1) period_start,
-- coalesce(period_end,period_end_1) period_end,
-- coalesce(period_type,period_type_1) period_type
-- from 
-- (
-- select rc.version,rc.rinok,rc.entity,rc.parentrole,coalesce(rr.label,rc.label) label,rc.concept,rc.dimension,rc.period_start,rc.period_end,rc.period_type,
-- coalesce(rc.father,rr.father) father
-- from rc
-- left join (select version,rinok,entity,parentrole,father,label from re where concept is null and dimension is null) rr on
-- rr.father=rc.label and rr.version=rc.version and rr.rinok=rc.rinok and rr.entity=rc.entity and rr.parentrole=rc.parentrole
-- ) rc
-- left join (select version,rinok,entity,parentrole,father,dimension dimension_1,period_start period_start_1,period_end period_end_1,period_type period_type_1,label from re) re1 
-- on re1.father=rc.label and re1.version=rc.version and re1.rinok=rc.rinok and re1.entity=rc.entity and re1.parentrole=rc.parentrole
-- ) rc 
-- left join an on an.version=rc.version and an.rinok=rc.rinok and an.entity=rc.entity and an.parentrole=rc.parentrole
-- left join r_dim on r_dim.version=rc.version and r_dim.rinok=rc.rinok and r_dim.entity=rc.entity and r_dim.parentrole=rc.parentrole
-- left join per tr on  tr.version=rc.version and tr.rinok=rc.rinok and tr.entity=rc.entity and tr.parentrole=rc.parentrole
-- and rc.period_type=tr.period_type and rc.period_start is null and rc.period_end is null
-- -- where concept='nfo-dic:StoimDanBukhUch'
-- order by 1,2,3 nulls last

