import psycopg2,pandas as pd,gc,warnings

warnings.filterwarnings("ignore")
def do_sql(sql):
    connect = psycopg2.connect(user="postgres",
                               password="124kosm21",
                               host="127.0.0.1",
                               port="5432",
                               database="final_5_2_j")
    df = pd.read_sql_query(sql, connect)
    connect.close()
    gc.collect()
    return df

def save_to_excel(df,sql,name):
    with pd.ExcelWriter(f"{name}.xlsx") as writer:
        df.to_excel(writer, index=False, sheet_name='result')
        df_sql=pd.DataFrame({'sql':[sql]})
        df_sql.to_excel(writer,index=False,sheet_name='SQL')
sql_bfo="""
with 
df as 
(
select array_agg(e.qname||'#'||em.qname) dim_def
from locators l 
join arcs a on a.version=l.version and a.rinok=l.rinok and a.entity=l.entity and a.arcfrom=l.label and a.parentrole=l.parentrole and arcrole='http://xbrl.org/int/dim/arcrole/dimension-default'
join locators lm on  a.version=lm.version and a.rinok=lm.rinok and a.entity=lm.entity and a.arcto=lm.label and a.parentrole=lm.parentrole
join elements e on e.id=l.href_id and e.version=l.version
join elements em on em.id=lm.href_id and em.version=lm.version
where l.rinok='bfo'
group by l.version,l.rinok,l.entity,l.parentrole,a.arcrole
),
def as
(
select l.version,l.rinok,l.entity,l.parentrole,e.qname,l.label,arcfrom,arcto,arcrole,e.type,coalesce(e.abstract,'false') abstract,a.usable,targetrole,
	case when arcrole='http://xbrl.org/int/dim/arcrole/domain-member' and coalesce(e.type,'')!='nonnum:domainItemType' then 1
	when arcrole='http://xbrl.org/int/dim/arcrole/hypercube-dimension' then 2
	when arcrole='http://xbrl.org/int/dim/arcrole/dimension-domain' then 3
	when arcrole='http://xbrl.org/int/dim/arcrole/domain-member' then 4 
	when arcrole='http://xbrl.org/int/dim/arcrole/notAll' then 5 
	when arcrole='http://xbrl.org/int/dim/arcrole/all' then 0 else -1 end type_elem
from locators l
join elements e on e.id=href_id and e.version=l.version and e.rinok not in ('eps')
join arcs a on a.arcto=l.label and l.version=a.version and l.rinok=a.rinok and l.entity=a.entity and a.parentrole=l.parentrole
and a.arctype='definition' 
where l.rinok='bfo'
-- 		and l.parentrole in 
-- ('http://www.cbr.ru/xbrl/bfo/rep/2024-01-01/tab/FR_4_005_02b_01','http://www.cbr.ru/xbrl/bfo/dict/Exclusion_001_technical',
-- 'http://www.cbr.ru/xbrl/bfo/dict/Exclusion_100_technical','http://www.cbr.ru/xbrl/bfo/dict/Exclusion_101_technical')
	order by arcrole
),
dd as
(select version,rinok,entity,parentrole,string_to_array(unnest(cross_agregate(array_agg(dims))),'|') dims
from
(
select version,rinok,entity,parentrole,split_part(dims,'#',1) dim,string_agg(dims,'|') dims
from
(
select version,rinok,entity,parentrole,unnest(dimensions) dims
from 
(
 select dd.version,dd.rinok,dd.entity,dd.parentrole,
        array_remove(array_agg(dd.qname||case when dd.qname||'#'||coalesce(dd3.qname,dd2.qname) is not null then '#' else '' end||coalesce(coalesce(dd3.qname,dd2.qname),''))||
        array_agg(distinct case when coalesce(dd2.usable,'true') ='true' and dd3.qname is not null then dd.qname||'#'||dd2.qname end),null) dimensions
        from 
		(select version,rinok,entity,parentrole,qname,arcfrom,label,usable from def
        where type_elem=2) dd
        left join (select version,rinok,entity,parentrole,qname,arcfrom,label,usable from def
        where type_elem=3) dd2 on dd.version=dd2.version and dd2.rinok=dd.rinok and dd2.entity=dd.entity and dd2.parentrole=dd.parentrole and dd2.arcfrom=dd.label
        left join (select version,rinok,entity,parentrole,qname,arcfrom,label,usable from def
        where type_elem=4) dd3 on dd3.version=dd2.version and dd2.rinok=dd3.rinok and dd2.entity=dd3.entity and dd2.parentrole=dd3.parentrole and dd3.arcfrom=dd2.label
        group by dd.version,dd.rinok,dd.entity,dd.parentrole
)dd
)dd
group by version,rinok,entity,parentrole,split_part(dims,'#',1)
) dd group by version,rinok,entity,parentrole
)

select version,rinok,entrypoint,concept,dims,
array_length(array_agg(distinct parentrole),1) len,is_minus,
string_agg(distinct parentrole,';') roles
from
(
select version,rinok,entrypoint,concept,dims,parentrole,max(is_minus) is_minus
from
(
select dd.version,dd.rinok,entrypoint,concept,
case when dims is null then dims else delete_default_dims(dims,dim_def) end dims,parentrole,is_minus
from
(
select cc.version,cc.rinok,cc.entity,cc.parentrole,cc.qname concept,dims,dims_minus,
case when dims is not null then array_sravn(dims,dims_minus) else 0 end is_minus
from 
(
select version,rinok,entity,parentrole,qname,arcfrom,label,usable,targetrole 
from def
where type_elem=1 --and parentrole='http://www.cbr.ru/xbrl/bfo/rep/2023-03-31/tab/FR_4_008_02a_01'
and abstract='false'
) cc 
left join dd using (version,rinok,entity,parentrole)
left join 
(
select d1.version,d1.rinok,d1.entity,d1.parentrole,d1.arcfrom,dims dims_minus
from def d1 
join dd d2 on d1.version=d2.version and d1.rinok=d2.rinok and d2.parentrole=d1.targetrole
where d1.type_elem=5 --and d1.parentrole='http://www.cbr.ru/xbrl/bfo/rep/2023-03-31/tab/FR_4_008_02a_01'
) tr on tr.version=cc.version and cc.rinok=tr.rinok and cc.entity=tr.entity and cc.parentrole=tr.parentrole and tr.arcfrom=cc.label
) dd
left join 
(
select distinct tp.version,tp.entity,tp.rinok,targetnamespace entrypoint,tp.uri_razdel
from tableparts tp 
join tables t on t.version=tp.version and t.namespace=tp.uri_table
) tp on tp.version=dd.version and tp.rinok=dd.rinok and tp.uri_razdel=dd.parentrole	
join df on 1=1
) dd 
group by version,rinok,entrypoint,concept,dims,parentrole
	) dd
group by version,rinok,concept,dims,entrypoint,is_minus
order by version,rinok,entrypoint,concept

"""
sql_1="""
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
df=do_sql(sql_1)
save_to_excel(df,sql_1,'лэйблы дефинишн и презентейшн')


