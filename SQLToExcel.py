import psycopg2,pandas as pd,gc,warnings,pickle

warnings.filterwarnings("ignore")
def do_sql(sql):
    connect = psycopg2.connect(user="postgres",
                               password="124kosm21",
                               host="127.0.0.1",
                               port="5432",
                               database="final_6")
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
select x.version "Версия",x.rinok,x.entity "Файл xsd",x.file "Файл в tab",lb.href "Файл в xsd",case when is_hidden='true' then 'да' else 'нет' end "Скрытый да/нет"
from xsdfiles x
left join linkbaserefs lb  on lb.version=x.version and lb.entity=x.entity and lb.href=x.file
where x.file not similar to '%.xsd'
and lb.href is null
"""
df=do_sql(sql_1)

save_to_excel(df,sql_1,'Мусорные файлы в папках tab')


