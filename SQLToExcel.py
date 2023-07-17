import psycopg2,pandas as pd,gc,warnings

warnings.filterwarnings("ignore")
def do_sql(sql):
    connect = psycopg2.connect(user="postgres",
                               password="124kosm21",
                               host="127.0.0.1",
                               port="5432",
                               database="final_3_2")
    df = pd.read_sql_query(sql, connect)
    connect.close()
    gc.collect()
    return df

def save_to_excel(df,sql,name):
    with pd.ExcelWriter(f"{name}.xlsx") as writer:
        df.to_excel(writer, index=False, sheet_name='result')
        df_sql=pd.DataFrame({'sql':[sql]})
        df_sql.to_excel(writer,index=False,sheet_name='SQL')
sql="""
with def as
(
select l.version,l.rinok,l.entity,l.parentrole,e.qname,l.label,arcfrom,arcto,arcrole,e.type,e.rinok e_rinok
from locators l
join elements e on e.id=href_id and e.version=l.version
join arcs a on a.arcto=l.label and l.version=a.version and l.rinok=a.rinok and l.entity=a.entity and a.parentrole=l.parentrole
where l.parentrole in (select uri_razdel from tableparts)
and a.arctype='definition'
order by parentrole
)

select version,e_rinok,
count(distinct case when arcrole='http://xbrl.org/int/dim/arcrole/domain-member' and (type not in ('nonnum:domainItemType') or type is null) and qname is not null  then qname end) conc,
count(distinct case when arcrole='http://xbrl.org/int/dim/arcrole/hypercube-dimension' and qname is not null  then qname end) dim,
count(distinct case when arcrole in ('http://xbrl.org/int/dim/arcrole/dimension-domain','http://xbrl.org/int/dim/arcrole/domain-member') and 
type in ('nonnum:domainItemType') and qname is not null  then qname end) mem
from def
group by grouping sets
	((version,e_rinok),version)
"""
sql2="""
select version,entrypoint,sum(vas) assertions
from
(
select va.version,va.rinok,va.entity,va.parentrole,targetnamespace entrypoint,count(distinct va.id) vas 
from va_assertions va
join tableparts tp on tp.version=va.version and tp.uri_razdel=va.parentrole
join tables t on t.version=tp.version and t.namespace=tp.uri_table
group by va.version,va.rinok,va.entity,va.parentrole,targetnamespace
	order by targetnamespace
) va
group by  version,entrypoint
order by 1,2
"""
sql3="""
with def as
(
select l.version,l.rinok,l.entity,l.parentrole,e.qname,l.label,arcfrom,arcto,arcrole,e.type,a.usable
from locators l
join elements e on e.id=href_id and e.version=l.version
join arcs a on a.arcto=l.label and l.version=a.version and l.rinok=a.rinok and l.entity=a.entity and a.parentrole=l.parentrole
where l.parentrole in (select uri_razdel from tableparts where entity='sr_0420154.xsd')
and a.arctype='definition'
order by parentrole
),
cc as 
(
select version,rinok,entity,parentrole,qname concept from def 
where arcrole='http://xbrl.org/int/dim/arcrole/domain-member' and (type not in ('nonnum:domainItemType') or type is null)
)
-- select cc.version,cc.rinok,cc.entity,cc.parentrole,concept,
-- string_agg(dd.qname||case when dd.qname||'#'||coalesce(dd3.qname,dd2.qname) is not null then '#' else '' end||coalesce(coalesce(dd3.qname,dd2.qname),''),';') dimensions,
-- string_agg(distinct case when coalesce(dd2.usable,'true') ='true' and dd3.qname is not null then dd.qname||'#'||dd2.qname end,';') dimensions_group,
-- rt.definition parentrole_text
select cc.version,cc.rinok,cc.entity,cc.parentrole,concept,dd.qname,dd2.qname,dd3.qname,coalesce(dd2.usable,'true') usable
from cc
left join roletypes rt on rt.roleuri=cc.parentrole
left join def dd on dd.version=cc.version and dd.entity=cc.entity and dd.rinok=cc.rinok and dd.parentrole=cc.parentrole and dd.arcrole='http://xbrl.org/int/dim/arcrole/hypercube-dimension'
left join def dd2 on dd.version=dd2.version and dd2.rinok=dd.rinok and dd2.entity=dd.entity and dd2.parentrole=dd.parentrole and dd2.arcfrom=dd.label 
and dd2.arcrole='http://xbrl.org/int/dim/arcrole/dimension-domain'
left join def dd3 on dd3.version=dd2.version and dd2.rinok=dd3.rinok and dd2.entity=dd3.entity and dd2.parentrole=dd3.parentrole and dd3.arcfrom=dd2.label 
and dd3.arcrole='http://xbrl.org/int/dim/arcrole/domain-member'
-- group by cc.version,cc.rinok,cc.entity,cc.parentrole,concept,rt.definition
-- order by parentrole,concept,array_length(array_agg(dd.qname||'#'||coalesce(coalesce(dd3.qname,dd2.qname),'')),1) desc
where dd3.qname is not null
"""

df=do_sql(sql)
save_to_excel(df,sql,'final_3_2_elements')
df=do_sql(sql2)
save_to_excel(df,sql2,'final_3_2_assertions')
