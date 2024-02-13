import psycopg2,pandas as pd,gc,warnings,pickle

warnings.filterwarnings("ignore")
def do_sql(sql):
    connect = psycopg2.connect(user="postgres",
                               password="124kosm21",
                               host="127.0.0.1",
                               port="5432",
                               database="final_6_git")
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
select distinct version "Версия",rinok "Рынок",entity "Файл",parentrole "Роль",id,value 
from va_concepts
where value not in (select qname from elements 
       )
union all
select distinct version "Версия",rinok "Рынок",entity "Файл",parentrole "Роль",id,dimension 
from va_edimensions
where dimension not in (select qname from elements 
        )
union all
select distinct version "Версия",rinok "Рынок",entity "Файл",parentrole "Роль",dimension_id,member 
from va_edmembers
where member not in (select qname from elements 
     )
"""
df=do_sql(sql_1)
save_to_excel(df,sql_1,'46_git')


