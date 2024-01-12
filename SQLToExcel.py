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
with locloc as 
(
select * from locators where locfrom='formula' order by version,rinok,entity,parentrole
),
arar as not materialized
(
select * from arcs where arctype='formula' order by version,rinok,entity,parentrole
),
fv as
(
select fv.version,fv.rinok,fv.entity,fv.parentrole,fv.id,arcfrom,arcto fv_child,fallbackvalue,coalesce(l.label,fv.label) fv_label,e_qname,e_type,e_type_p
from (select * from va_factvars order by version,rinok,entity,parentrole) fv
left join locloc l on l.version=fv.version and l.rinok=fv.rinok and l.entity=fv.entity and l.href_id=fv.id
join arar a on fv.version=a.version and fv.rinok=a.rinok and fv.entity=a.entity and a.parentrole=fv.parentrole and coalesce(l.label,fv.label)=a.arcfrom
join (select vc.*,e.type e_type,qname e_qname, 
case when e.type in ('xbrli:monetaryItemType','xbrli:decimalItemType','xbrli:integerItemType') then 1 
when e.type in ('xbrli:stringItemType','xbrli:dateItemType','enum:enumerationItemType','enum2:enumerationSetItemType','xbrli:timeItemType') then 0 
when e.type not in ('xbrli:monetaryItemType','xbrli:decimalItemType','xbrli:integerItemType','xbrli:stringItemType','xbrli:dateItemType',
					'enum:enumerationItemType','enum2:enumerationSetItemType','xbrli:timeItemType') then -1 else 999 end e_type_p
from va_concepts vc 
join elements e on e.qname=vc.value
order by vc.version,vc.rinok,vc.entity,vc.parentrole) vc on 
a.version=vc.version and a.rinok=vc.rinok and a.entity=vc.entity and a.parentrole=vc.parentrole and a.arcto=vc.label
where fallbackvalue is not null
order by fv.version,fv.rinok,fv.entity,fv_label
),
va as 
(
select l.version,l.rinok,l.entity,a.parentrole,a.arcto fv_label,va.id va_id,test,p_test,'$'||a.name name_c
from locloc l
join arar a on l.version=a.version and l.rinok=a.rinok and l.entity=a.entity and a.arcfrom=l.label
join (select * from va_assertions order by version,rinok,entity,parentrole) va on va.version=l.version and l.rinok=va.rinok and l.entity=va.entity and va.id=l.href_id
left join 
(
select p.version,p.entity,p.rinok,p.parentrole,a.arcfrom,test p_test
from preconditions p
join arar a on a.version=p.version and a.rinok=p.rinok and a.entity=p.entity and a.parentrole=p.parentrole and a.arcto=p.label
order by p.version,p.rinok,p.entity,p.parentrole
) p on p.version=va.version and p.rinok=va.rinok and p.entity=va.entity and p.parentrole=va.parentrole and p.arcfrom=va.label
--	where va.id='valueAssertion_0420719_003'
order by l.version,l.rinok,l.entity,a.arcto 
)

select version, rinok, entity, parentrole, fv_label, va_id, e_type, is_number, fallbackvalue, name_c, mm, ss, mm2, ss2, test, p_test 
from (
select distinct version,rinok,entity,parentrole,fv_label,va_id,e_type,is_number,fallbackvalue,name_c,
regexp_like(test, '.*matches\s*\(\s*' || '\'||name_c || '.*', 'i') mm,
regexp_like(test, '.*count\s*\(\s*' || '\'||name_c || '.*', 'i') or regexp_like(test, '.*sum\s*\(\s*' || '\'||name_c || '.*', 'i') ss,
regexp_like(p_test, '.*matches\s*\(\s*' || '\'||name_c || '.*', 'i') mm2,
regexp_like(p_test, '.*count\s*\(\s*' || '\'||name_c || '.*', 'i') or regexp_like(p_test, '.*sum\s*\(\s*' || '\'||name_c || '.*', 'i') ss2,
test,p_test
from
(
select va.version,va.rinok,va.entity,va.parentrole,fv_label,va_id,e_type,e_type_p,regexp_like(fallbackvalue,'^[0-9\.]+$') is_number,fallbackvalue,name_c,test,p_test
from va
join fv using(version,rinok,entity,fv_label)
)z
where ((e_type_p = 1 and not is_number) or (e_type_p = 0 and is_number) or e_type_p =-1)
	) zz where (mm = true and is_number) or (ss = true and not is_number)
	or (mm2 = true and is_number) or (ss2 = true and not is_number)

"""
df=do_sql(sql_1)
print(df)

save_to_excel(df,sql_1,'fallback')


