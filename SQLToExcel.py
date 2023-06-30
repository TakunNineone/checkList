import psycopg2,pandas as pd,gc,warnings

warnings.filterwarnings("ignore")
def do_sql(sql):
    connect = psycopg2.connect(user="postgres",
                               password="124kosm21",
                               host="127.0.0.1",
                               port="5432",
                               database="final_5_2")
    df = pd.read_sql_query(sql, connect)
    connect.close()
    gc.collect()
    return df

def save_to_excel(df,sql):
    with pd.ExcelWriter("date_control_sr_0420253_r_1_spr.xlsx") as writer:
        df.to_excel(writer, index=False, sheet_name='result')
        df_sql=pd.DataFrame({'sql':[sql]})
        df_sql.to_excel(writer,index=False,sheet_name='SQL')
sql2="""
select distinct e.version "Версия",e.rinok "Справочник",rinok_form "Рынок формы",table_entity,uri_razdel,e.entity "Файл",taxis "Открытая ось",e.id "Домен",pattern,minlength,
substitutiongroup,case when element_id is not null then 'да' else 'нет' end "Используется в definition"
from elements e
join
(
select id taxis,split_part(typeddomainref,'#',-1) typdename 
from elements where typeddomainref is not null  
and version='final_5_2_j'
) ee on ee.typdename=id
left join 
(
select distinct l.href_id element_id,tp.uri_razdel,tp.entity table_entity,tp.rinok rinok_form
from arcs a
join tableparts tp on tp.version=a.version and tp.rinok=a.rinok
join locators l on l.label=a.arcto and l.version=a.version and l.rinok=a.rinok and l.entity=a.entity and a.parentrole=l.parentrole 
where arctype='definition' and (tp.uri_table=a.parentrole or tp.uri_razdel=a.parentrole)
and a.version='final_5_2_j'
) df on df.element_id=taxis
where
(
pattern is null or minlength is null
or substitutiongroup is not null
) and e.version='final_5_2_j'
order by rinok_form,e.rinok,e.id,case when element_id is not null then 'да' else 'нет' end
"""
sql="""
with at as
(
select distinct r.version,r.rinok,r.entity,r.parentrole,r.label as rulenode,l.label,l.text,rc.value,
	r.entity||'#'||r.label,rt.definition uri_text
from rulenodes r
left join roletypes rt on rt.version=r.version and rt.rinok=r.rinok and rt.roleuri=r.parentrole and rt.version='final_5_2_j'
left join rulenodes_c rc on rc.version=r.version and rc.rinok=r.rinok and rc.entity=r.entity and rc.parentrole=r.parentrole 
	and rc.rulenode_id=r.id and rc.version='final_5_2_j'
left join
(
select l.href,l.version,l.rinok,lb.label,lb.lang,lb.text
from locators l
join arcs a on a.version=l.version and a.rinok=l.rinok and a.entity=l.entity and a.arcfrom=l.label and a.version='final_5_2_j'
join labels lb on lb.version=a.version and lb.rinok=a.rinok and lb.entity=a.entity and lb.label=a.arcto and lb.version='final_5_2_j'
where l.locfrom='lab' and lb.role='http://www.xbrl.org/2008/role/label'
) l on l.version=r.version and l.rinok=r.rinok and l.href=r.entity||'#'||r.label
where rc.value is not null and l.version='final_5_2_j'
),
ap as
(
select distinct a.version,a.rinok,a.entity,a.parentrole,l.href_id,pl.text,pl.qname,pl.role
from arcs a
join locators l on l.label=a.arcto and l.version=a.version and l.rinok=a.rinok and l.entity=a.entity and a.parentrole=l.parentrole and l.version='final_5_2_j'
join elements_labels pl on pl.version=l.version and pl.id=l.href_id and pl.role=a.preferredlabel and pl.version='final_5_2_j'
where arctype ='presentation'
and pl.role not in ('http://www.xbrl.org/2003/role/periodEndLabel','http://www.xbrl.org/2003/role/periodStartLabel')
	and a.version='final_5_2_j'
order by href_id
),
ad as 
(
select distinct a.version,a.rinok,a.entity,a.parentrole,l.href_id,el.text,el.qname,el.abstract
from arcs a
join locators l on l.label=a.arcto and l.version=a.version and l.rinok=a.rinok and l.entity=a.entity and a.parentrole=l.parentrole and l.version='final_5_2_j'
join elements_labels el on el.version=l.version and el.id=l.href_id and el.role='http://www.xbrl.org/2003/role/label' and el.lang='ru' and el.version='final_5_2_j'
where arctype ='definition' and a.version='final_5_2_j'
order by href_id
)

select distinct at.version "Версия",at.entity "Файл",at.rinok "Рынок",at.parentrole "URI в table",
uri_text "Раздел",at.rulenode "ID ruleNode",at.value "Показатель в ruleNode",
ap.qname "Показатель в presentation",ad.qname "Показатель в definition", 
at.text "Лайбл рулнода",ap.text "Лейбл presentation",ad.text "Лейбл в definition"
from at
left join ap on ap.qname=value and (ap.parentrole similar to at.parentrole||'\D%' or ap.parentrole=at.parentrole)
left join ad on ad.qname=value and (ad.parentrole similar to at.parentrole||'\D%' or ad.parentrole=at.parentrole)  
where at.version='final_5_2_j' 
order by 1,2,3,4

"""
sql3="""
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
and t.parentrole like 'http://www.cbr.ru/xbrl/nso/npf/rep/2023-03-31/tab/sr_0420253_r_1_spr'
) t 
join arcs a on a.version=t.version and a.rinok=t.rinok and a.entity=t.entity and a.parentrole=t.parentrole and a.arcfrom=t.br_label
order by t.version,t.rinok,t.entity,t.parentrole,a.arcto
),
per as
(
	select rn.version,rn.rinok,rn.entity,rn.parentrole,period_type,rp.start,rp.end 
	from rulenodes rn
	join rulenodes_p rp on rp.version=rn.version and rp.rinok=rn.rinok and rp.entity=rn.entity and rp.parentrole=rn.parentrole and rp.rulenode_id=rn.id
	where rn.parentrole in ('http://www.cbr.ru/xbrl/nso/npf/rep/2023-03-31/tab/sr_0420253_r_1_spr')
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
left join elements e on e.qname=rc.value and e.version=rc.version and e.abstract!='true'
where r.parentrole in ('http://www.cbr.ru/xbrl/nso/npf/rep/2023-03-31/tab/sr_0420253_r_1_spr')
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
where an.parentrole in ('http://www.cbr.ru/xbrl/nso/npf/rep/2023-03-31/tab/sr_0420253_r_1_spr')
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
select tt.version,tt.rinok,tt.entity,tt.parentrole,dimension root_dim
from tt
left join (select version,rinok,entity,parentrole,father,dimension,label from re) re1 
on re1.label=tt.root_rulenodes and re1.version=tt.version and re1.rinok=tt.rinok and re1.entity=tt.entity and re1.parentrole=tt.parentrole
where dimension is not null
)


select rc.parentrole,concept,
case when rc.dimension is null then '' else rc.dimension end||case when rc.dimension is null then '' else ';' end||case when dimension_1 is null then '' else dimension_1 end||
		   case when dimension_1 is null then '' else ';' end||case when an_dim is null then '' else an_dim end||case when an_dim is null then '' else ';' end||case when root_dim is null then '' else root_dim end dimensions,
rc.period_type,
coalesce(period_start,start) period_start,
case when period_start is null then coalesce(period_end,tr.end) else period_end end period_end
from
(
select rc.version,rc.rinok,rc.entity,rc.parentrole,rc.label,concept,rc.dimension,dimension_1,
coalesce(period_start,period_start_1) period_start,
coalesce(period_end,period_end_1) period_end,
coalesce(period_type,period_type_1) period_type
from 
(
select rc.version,rc.rinok,rc.entity,rc.parentrole,coalesce(rr.label,rc.label) label,rc.concept,rc.dimension,rc.period_start,rc.period_end,rc.period_type,
coalesce(rc.father,rr.father) father
from rc
left join (select version,rinok,entity,parentrole,father,label from re where concept is null and dimension is null) rr on
rr.father=rc.label and rr.version=rc.version and rr.rinok=rc.rinok and rr.entity=rc.entity and rr.parentrole=rc.parentrole
) rc
left join (select version,rinok,entity,parentrole,father,dimension dimension_1,period_start period_start_1,period_end period_end_1,period_type period_type_1,label from re) re1 
on re1.father=rc.label and re1.version=rc.version and re1.rinok=rc.rinok and re1.entity=rc.entity and re1.parentrole=rc.parentrole
) rc 
left join an on an.version=rc.version and an.rinok=rc.rinok and an.entity=rc.entity and an.parentrole=rc.parentrole
left join r_dim on r_dim.version=rc.version and r_dim.rinok=rc.rinok and r_dim.entity=rc.entity and r_dim.parentrole=rc.parentrole
left join per tr on  tr.version=rc.version and tr.rinok=rc.rinok and tr.entity=rc.entity and tr.parentrole=rc.parentrole
and rc.period_type=tr.period_type and rc.period_start is null and rc.period_end is null
order by 1,2,3 nulls last
"""

df=do_sql(sql3)
save_to_excel(df,sql3)
