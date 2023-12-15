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

with fv as 
(
select fv.version,fv.rinok,fv.entity,fv.parentrole,fv.id,arcfrom,arcto fv_child,fallbackvalue,coalesce(l.label,fv.label) fv_label,e_qname,e_type,e_type_p
from va_factvars fv
left join locators l on l.version=fv.version and l.rinok=fv.rinok and l.entity=fv.entity and l.href_id=fv.id
join arcs a on fv.version=a.version and fv.rinok=a.rinok and fv.entity=a.entity and a.parentrole=fv.parentrole and coalesce(l.label,fv.label)=a.arcfrom
join (select vc.*,e.type e_type,qname e_qname, 
case when e.type in ('xbrli:monetaryItemType','xbrli:decimalItemType','xbrli:integerItemType') then 1 
when e.type in ('xbrli:stringItemType','xbrli:dateItemType','enum:enumerationItemType','enum2:enumerationSetItemType','xbrli:timeItemType') then 0 
when e.type not in ('xbrli:monetaryItemType','xbrli:decimalItemType','xbrli:integerItemType','xbrli:stringItemType','xbrli:dateItemType',
					'enum:enumerationItemType','enum2:enumerationSetItemType','xbrli:timeItemType') then -1 else 999 end e_type_p
from va_concepts vc join elements e on e.qname=vc.value order by vc.version,vc.rinok,vc.entity,vc.parentrole) vc on 
fv.version=vc.version and fv.rinok=vc.rinok and fv.entity=vc.entity and fv.parentrole=vc.parentrole and a.arcto=vc.label
where fallbackvalue is not null
	order by fv.version,fv.rinok,fv.entity,fv.parentrole,fv_label
),
va as
(
select l.version,l.rinok,l.entity,a.parentrole,a.arcto fv_label,va.id va_id,test,p_test,'$'||a.name name_c
from locators l
join arcs a on l.version=a.version and l.rinok=a.rinok and l.entity=a.entity and a.arcfrom=l.label
join va_assertions va on va.version=l.version and l.rinok=va.rinok and l.entity=va.entity and va.id=l.href_id
left join 
(
select p.version,p.entity,p.rinok,p.parentrole,a.arcfrom,test p_test
from preconditions p
join arcs a on a.version=p.version and a.rinok=p.rinok and a.entity=p.entity and a.parentrole=p.parentrole and a.arcto=p.label
) p on p.version=va.version and p.rinok=va.rinok and p.entity=va.entity and p.parentrole=va.parentrole and p.arcfrom=va.label
order by l.version,l.rinok,l.entity,a.parentrole,a.arcto 
)

select distinct version,rinok,entity,parentrole,fv_label,va_id,e_type,is_number,fallbackvalue,name_c,test
from
(
select va.version,va.rinok,va.entity,va.parentrole,fv_label,va_id,e_type,e_type_p,regexp_like(fallbackvalue,'^[0-9\.]+$') is_number,fallbackvalue,name_c,test
from va
join fv using(version,rinok,entity,fv_label)
)z
where (e_type_p = 1 and not is_number) or (e_type_p = 0 and is_number) or e_type_p =-1
"""
df=do_sql(sql_1)

save_to_excel(df,sql_1,'fallback')


