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
select distinct version "Версия",entity "Файл",parentrole "Роль",id "ID",get_kiril_element(id) latin from
(
select version,entity,parentrole,id,'va_edimensions' chto from va_edimensions union all
select version,entity,parentrole,id,'va_tdimensions' from va_tdimensions union all
select version,entity,parentrole,id,'va_concepts' from va_concepts union all
select version,entity,parentrole,id,'va_factvars' from va_factvars union all
select version,entity,parentrole,id,'va_assertions' from va_assertions union all
select version,entity,parentrole,id,'va_generals' from va_generals union all
select version,entity,parentrole,id,'va_aspectcovers' from va_aspectcovers union all
select version,entity,parentrole,id,'va_assertionsets' from va_assertionsets union all
select version,entity,parentrole,id,'tableschemas' from tableschemas union all
select version,entity,parentrole,id,'labels' from labels union all
select version,entity,parentrole,id,'rend_edimensions' from rend_edimensions union all
select version,entity,parentrole,id,'rulenodes' from rulenodes union all
select version,entity,parentrole,id,'aspectnodes' from aspectnodes union all
select version,entity,parentrole,id,'preconditions' from preconditions union all
select version,entity,parentrole,id,'messages' from messages 
) all_id
where entity in 
(select href from linkbaserefs)
and id SIMILAR TO '%[\u0410-\u044f]%'
"""
df=do_sql(sql_1)
save_to_excel(df,sql_1,'кириллица в 162')


