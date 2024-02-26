import psycopg2,pandas as pd,gc,warnings,pickle

warnings.filterwarnings("ignore")
def do_sql(sql):
    connect = psycopg2.connect(user="postgres",
                               password="124kosm21",
                               host="127.0.0.1",
                               port="5432",
                               database="final_6_0")
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
select distinct e.version "Версия",e.rinok "Рынок",e.entity "Файл элемента с паттерном",dd.entity "Файл таблицы",uri_razdel "URI definition",
taxis "Ось (для typedName)",elem "Элемент с паттерном",substitutiongroup "Тип",e.minlength,e.pattern,round_skobka "Баланс круглых скобок",square_skobka "Баланс квадратных скобок"
from
(
select coalesce(ee.id,e.id) all_id,e.version,e.rinok,e.entity,ee.qname taxis,e.qname elem,substitutiongroup,e.minlength,e.pattern,round_skobka,square_skobka
from
(
select version,rinok,entity,id,qname,
substitutiongroup "Тип",minlength,pattern,
length(replace(pattern,'(','')) = length(replace(pattern,')','')) round_skobka,
length(replace(pattern,'[','')) = length(replace(pattern,']','')) square_skobka
from elements where pattern is not null
) e 
left join elements ee on e.id=split_part(ee.typeddomainref,'#',-1) and e.version=ee.version
) e
left join 
(
select a.version,a.entity,a.rinok,a.parentrole uri_razdel,l.href_id 
from arcs a
join locators l on l.label=a.arcto and l.version=a.version and l.rinok=a.rinok and l.entity=a.entity and a.parentrole=l.parentrole 
where arctype='definition'
) dd on dd.version=e.version and dd.href_id=e.all_id
order by uri_razdel nulls last,taxis nulls last

"""
df=do_sql(sql_1)
save_to_excel(df,sql_1,'64 final_6_git')


