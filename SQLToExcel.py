import psycopg2,pandas as pd,gc,warnings

warnings.filterwarnings("ignore")
def do_sql(sql):
    connect = psycopg2.connect(user="postgres",
                               password="124kosm21",
                               host="127.0.0.1",
                               port="5432",
                               database="final_6_5")
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
with 
va as
(
select va.version,va.rinok,va.entity,va.parentrole,rt.definition,va.label va_label,va.id va_id,va.test,p.test precond,coalesce(m.text,m2.text) msg,lw.href_id sev
	from va_assertions va
	join roletypes rt on rt.version=va.version and rt.rinok=va.rinok and rt.roleuri=va.parentrole
	left join (select * from arcs where arcrole='http://xbrl.org/arcrole/2016/assertion-unsatisfied-severity') aw on aw.version=va.version and aw.rinok=va.rinok and aw.parentrole=va.parentrole and aw.entity=va.entity and aw.arcfrom=va.label
	left join locators lw on lw.version=va.version and lw.rinok=va.rinok and  lw.parentrole=aw.parentrole and lw.entity=va.entity and lw.label=aw.arcto
	left join locators l on l.version=va.version and l.rinok=va.rinok and l.entity=va.entity and va.id=l.href_id
	left join (select * from arcs where arcrole='http://xbrl.org/arcrole/2010/assertion-unsatisfied-message') la on la.version=l.version and la.rinok=l.rinok and la.entity=l.entity and la.arcfrom=l.label
	left join (select * from arcs where arcrole='http://xbrl.org/arcrole/2010/assertion-unsatisfied-message') la2 on la2.version=va.version and la2.rinok=va.rinok and la2.entity=va.entity and la2.arcfrom=va.label
	left join messages m on m.version=la.version and m.rinok=la.rinok and m.entity=la.entity and m.label=la.arcto
	left join messages m2 on m2.version=la2.version and m2.rinok=la2.rinok and m2.entity=la2.entity and m2.label=la2.arcto
	left join (select * from arcs where arcrole='http://xbrl.org/arcrole/2008/variable-set-precondition') a on a.version=va.version and a.rinok=va.rinok and a.entity=va.entity and a.parentrole=va.parentrole and va.label=a.arcfrom
	left join preconditions p on p.version=a.version and p.rinok=a.rinok and p.entity=a.entity and p.parentrole=a.parentrole and p.label=a.arcto
order by va.version,va.rinok,va.entity,va.parentrole
),
fv as
(
select fv.version,fv.rinok,fv.entity,fv.parentrole, label fv_label,arcfrom,id fv_id,name var_name
from va_factvars fv
join arcs a on a.version=fv.version and a.rinok=fv.rinok and a.entity=fv.entity and a.parentrole=fv.parentrole 
and a.arcto=fv.label
	order by fv.version,fv.rinok,fv.entity,fv.parentrole
),
cc as 
(
select c.version,c.rinok,c.entity,c.parentrole,value,arcfrom,arcto 
from va_concepts c
join arcs a on a.version=c.version and a.rinok=c.rinok and a.entity=c.entity and a.parentrole=c.parentrole and a.arcto=c.label
	order by c.version,c.rinok,c.entity,c.parentrole
),
fv_dim as
(
select fv.version,fv.rinok,fv.entity,fv.parentrole,fv.arcfrom,cc.value concept,fv_id,var_name
from fv
left join cc on cc.version=fv.version and cc.rinok=fv.rinok and cc.entity=fv.entity and cc.parentrole=fv.parentrole and cc.arcfrom=fv.fv_label
	order by fv.version,fv.rinok,fv.entity,fv.parentrole
)

select va.version,va.rinok,va.entity,va.parentrole,va_id,definition,test,string_agg(var_name||'~'||fv_id,'!') var,precond,sev,msg
from va
left join fv_dim fv on fv.version=va.version and fv.rinok=va.rinok and fv.entity=va.entity and fv.parentrole=va.parentrole
and fv.arcfrom=va.va_label
group by va.version,va.rinok,va.entity,va.parentrole,va_id,definition,test,precond,sev,msg
order by parentrole,va_id
"""
df=do_sql(sql_1)
save_to_excel(df,sql_1,'Приложение 3 final_6')


