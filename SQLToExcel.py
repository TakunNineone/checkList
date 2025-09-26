import psycopg2,pandas as pd,gc,warnings,pickle

warnings.filterwarnings("ignore")
def do_sql(sql):
    connect = psycopg2.connect(user="postgres",
                               password="124kosm21",
                               host="127.0.0.1",
                               port="5432",
                               database="final_7_1_olya")
    df = pd.read_sql_query(sql, connect)
    connect.close()
    gc.collect()
    return df

def save_to_excel(df,sql,name):
    with pd.ExcelWriter(f"{name}.xlsx") as writer:
        df.to_excel(writer, index=False, sheet_name='result')
        df_sql=pd.DataFrame({'sql':[sql]})
        df_sql.to_excel(writer,index=False,sheet_name='SQL')
sql_1="""
with def_temp as not materialized
(
 select l.version,l.rinok,l.entity,l.parentrole,qname,l.label,arcfrom,arcto,arcrole,etype,coalesce(abstract,'false') abstract,a.usable,targetrole,
         case when arcrole='http://xbrl.org/int/dim/arcrole/domain-member' and coalesce(etype,'')!='nonnum:domainItemType' then 1
         when arcrole='http://xbrl.org/int/dim/arcrole/hypercube-dimension' then 2
         when arcrole='http://xbrl.org/int/dim/arcrole/dimension-domain' then 3
         when arcrole='http://xbrl.org/int/dim/arcrole/domain-member' then 4 
         when arcrole='http://xbrl.org/int/dim/arcrole/notAll' then 5 
         when arcrole='http://xbrl.org/int/dim/arcrole/all' then 0 else -1 end type_elem,
  typeddomainref
        from 
   (
   select l.*,e.type etype,abstract,qname,typeddomainref from (select * from locators where locfrom='definition' order by href_id) l join (select * from elements order by id) e on e.id=href_id
      order by l.rinok,l.entity,l.parentrole,l.label
   ) l 
join (select * from arcs where arctype='definition' order by rinok,entity,parentrole) a on l.rinok=a.rinok and l.entity=a.entity and a.parentrole=l.parentrole and a.arcto=l.label
	where l.rinok in ('kra','purcb')
order by arcrole
),
def as 
(
select distinct version,rinok,entity,parentrole,qname concept
from def_temp
where type_elem=1
and coalesce(abstract,'false')='false'
   
   order by rinok,entity,parentrole,qname
)
, 
rn as 
(
select distinct rn.version,rn.rinok,rn.parentrole,role_definition,rn.value
from rulenodes_c rn
join (select distinct rinok,role_table,role_definition from roles_table_definition order by 1,2) rt on rt.rinok=rn.rinok and rt.role_table=rn.parentrole
join elements e on e.qname=rn.value and e.version=rn.version
where coalesce(abstract,'false')='false'
   order by rinok,role_definition,value
) 

select distinct def.rinok,def.parentrole,rn2.parentrole
from def 
left join rn rn2 on rn2.rinok=def.rinok and def.parentrole=rn2.role_definition and rn2.value=def.concept
order by 1,rn2.parentrole nulls first
"""
df=do_sql(sql_1)
save_to_excel(df,sql_1,'kra_purcb_сопоставление_8')


