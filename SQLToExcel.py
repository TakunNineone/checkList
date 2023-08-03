import psycopg2,pandas as pd,gc,warnings

warnings.filterwarnings("ignore")
def do_sql(sql):
    connect = psycopg2.connect(user="postgres",
                               password="124kosm21",
                               host="127.0.0.1",
                               port="5432",
                               database="bfo")
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

df=do_sql(sql_bfo)
save_to_excel(df,sql_bfo,'bfo_dubles_new')

