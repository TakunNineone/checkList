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
with pp_t as
(
select distinct parentrole uri
from arcs a
where arctype='presentation'  --and a.version=HID
),
dd as (
select a.version,a.entity,a.rinok,a.parentrole,l.href_id
from arcs a
join locators l on l.label=a.arcto and l.version=a.version and l.rinok=a.rinok and l.entity=a.entity and a.parentrole=l.parentrole 
where arctype='definition' 
and a.parentrole in (select * from pp_t)
	order by a.version,a.entity,a.rinok,a.parentrole
),
pp as
(
select a.version,a.entity,a.rinok,a.parentrole,l.href_id 
from arcs a
join locators l on l.label=a.arcto and l.version=a.version and l.rinok=a.rinok and l.entity=a.entity and a.parentrole=l.parentrole 
where arctype='presentation'  
	order by a.version,a.entity,a.rinok,a.parentrole
) 

select version "Версия",max(rinok_def) "Рынок роли def",max(rinok_pres) "Рынок роли pres",parentrole "Роль",href_id "Элемент",max(def) "Файл def",max(pres) "Файл pres"
from
(
select version,dd.rinok rinok_def,null rinok_pres,parentrole,href_id,dd.entity def,null pres
from dd
left join pp using (version,rinok,parentrole,href_id)
where pp.entity is null

union all

select version,null,pp.rinok,parentrole,href_id,null,pp.entity 
from pp
left join dd using (version,rinok,parentrole,href_id)
where dd.entity is null
) dp 
group by version,parentrole,href_id

"""
df=do_sql(sql_1)
save_to_excel(df,sql_1,'61.')


