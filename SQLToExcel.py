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
with ll as
(
select version,rinok,entity,href,ll.order,rn::numeric[] as rn,
DENSE_RANK() OVER (
    PARTITION BY version,rinok,entity
    ORDER BY rn::numeric[],href) rn_num
from
(
select version,rinok,entity,href,tp.order,array_remove(string_to_array(NULLIF(regexp_replace(href, '\D','|','g'), '1'),'|'),'') rn
from linkbaserefs tp
where href like '%-rend.xml' and href not like '../%' and rinok!='bfo'
) ll
order by version,rinok,entity,rn::numeric[]
)

select version "Версия",rinok "Рынок",entity "Файл",href "REND",ll.order "Порядок в файле",rn_num "Порядок как должен быть",rn
from ll
where (version,rinok,entity) in (select version,rinok,entity from ll where ll.order-rn_num!=0)
"""
df=do_sql(sql_1)
save_to_excel(df,sql_1,'порядок разделов (где не сошлось)')


