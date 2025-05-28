import psycopg2,pandas as pd,gc,warnings,pickle

warnings.filterwarnings("ignore")
def do_sql(sql):
    connect = psycopg2.connect(user="postgres",
                               password="124kosm21",
                               host="127.0.0.1",
                               port="5432",
                               database="final_7_0_no")
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
select qname,id,typeddomainref,entity "Используется в definition",rinok
from 
(
select qname,id,typeddomainref from elements where typeddomainref is not null
and typeddomainref not like '%TypedName'
) e
left join 
(
select distinct entity,l.href_id,rinok from locators l
where locfrom='definition' 
and fullpath like '%/tab/%'
) l on l.href_id=e.id
order by 4 nulls last,1,2,3
"""
df=do_sql(sql_1)
save_to_excel(df,sql_1,'неправильные typeddomainref')


