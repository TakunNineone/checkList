import psycopg2,pandas as pd,gc,warnings,pickle

warnings.filterwarnings("ignore")
def do_sql(sql):
    connect = psycopg2.connect(user="postgres",
                               password="124kosm21",
                               host="127.0.0.1",
                               port="5432",
                               database="final_6_predv")
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
select distinct a.rinok,coalesce(rt.role_definition,a.parentrole) role_definition,parentrole role_table from arcs a
left join (select distinct * from roles_table_definition) rt on rt.role_table=replace(a.parentrole,'2024-11-01','2024-11-30')
where arctype like '%table%'
order by 1,2

"""
df=do_sql(sql_1)
save_to_excel(df,sql_1,'Маппинг ролей 6.0')


