import psycopg2,pandas as pd,gc,warnings,pickle

warnings.filterwarnings("ignore")
def do_sql(sql):
    connect = psycopg2.connect(user="postgres",
                               password="124kosm21",
                               host="127.0.0.1",
                               port="5432",
                               database="final_a_6_1_0_2")
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
with 
loc as not materialized
(
select distinct rinok,entity,parentrole,label,href_id 
from locators where locfrom='formula' --and rinok='ins'
order by rinok,entity,parentrole,label
),
arc as 
(
select distinct va.rinok,va.entity,va.parentrole,split_part(va.parentrole,'/',-1) arcfrom,va.label arcto,name,arcrole,a.title,'formula' arctype,complement,cover
from va_assertions va
join arcs a on a.rinok=va.rinok and a.entity=va.entity and a.parentrole=va.parentrole and a.arcfrom=va.label 
where arctype='formula' --and va.rinok='ins'

union all
 
select distinct rinok,entity,parentrole,arcfrom,arcto,name,arcrole,title,arctype,complement,cover
from arcs
where arctype='formula' --and rinok='ins'
order by 1,2,3,4
),
assert_all as 
(
select version,rinok,entity,parentrole,label,id,base_type,value,period,tag_type,gv_test,fallbackvalue
from
( 
select version,rinok,entity,parentrole,label,id,'t' base_type,value,null period,null tag_type,null gv_test,null fallbackvalue
from va_tdimensions 
union all
select version,e.rinok,e.entity,e.parentrole,e.label,e.id,'e' base_type,dimension||case when member is not null then '#'||member else '' end dimension,null,null,null,null fallbackvalue
from va_edimensions e
left join 
(select distinct rinok,entity,parentrole,dimension_id,member 
from va_edmembers
) m on m.rinok=e.rinok and m.entity=e.entity and m.parentrole=e.parentrole and m.dimension_id=e.id
union all
select version,rinok,entity,parentrole,label,id,'md' base_type,dimension value,null,null,'$'||variable null,null  
from va_mdimensions
union all
select version,cc.rinok,cc.entity,cc.parentrole,cc.label,cc.id,'c' base_type,cc.value,null,null,null,null fallbackvalue
from va_concepts cc
union all
select version,rinok,entity,parentrole,label,id,'pr' base_type,null,null,null,test,null fallbackvalue
from preconditions
union all
select version,rinok,entity,parentrole,label,id,'ac' base_type,dims,null,null,null,null fallbackvalue
from va_aspectcovers
union all
select version,rinok,entity,parentrole,label,id,'fv' base_type,null,null,null,null,fallbackvalue
from va_factvars
union all
select version,rinok,entity,parentrole,label,id,'as' base_type,null,null,null,null,null fallbackvalue
from va_assertionsets
union all
select version,rinok,entity,parentrole,label,id,'of' base_type,null,null,null,null,null fallbackvalue
from va_orfilters
union all
select version,rinok,entity,parentrole,label,id,'af' base_type,null,null,null,null,null fallbackvalue
from va_andfilters
union all
select version,rinok,entity,parentrole,label,id,'g' base_type,null,null,null,test,null fallbackvalue
from va_generals
union all
select version,rinok,entity,parentrole,label,id,'gf' base_type,null,null,null,test,null fallbackvalue
from va_fgenerals
union all
select va.version,va.rinok,va.entity,va.parentrole,va.label,va.id,'va' base_type,m.lang||'|'||m.text,null,null,test,null fallbackvalue
from va_assertions va
left join (select * from loc  order by rinok,entity,href_id )l on  l.rinok=va.rinok and l.entity=va.entity and l.href_id=va.id  
left join (select * from arcs where arctype='formula' and arcrole='http://xbrl.org/arcrole/2010/assertion-unsatisfied-message' order by rinok,entity,parentrole,arcfrom) a on a.rinok=va.rinok and a.entity=va.entity and a.parentrole=l.parentrole and a.arcfrom=coalesce(l.label,va.label)
left join (select * from messages order by rinok,entity,label) m on m.rinok=va.rinok and m.entity=va.entity and m.label=a.arcto 
union all
select version,rinok,entity,parentrole,label,id,'p' base_type,null,date,tag_type,null,null fallbackvalue
from va_periods
) zz
order by rinok,entity,parentrole,label
),

assert_loc as
(
select a.version,a.rinok,a.entity,l.parentrole,a.parentrole role_elem,l.label,a.label label_elem,a.id,base_type,value,period,tag_type,gv_test,fallbackvalue
from assert_all a
join loc l on l.rinok=a.rinok and l.entity=a.entity and l.href_id=a.id 
order by a.rinok,a.entity,l.parentrole,l.label
),
assert_loc_agg as not materialized
(
select rinok,entity,parentrole,role_elem role_agg,array_agg(distinct label) labels
from assert_loc
group by rinok,entity,parentrole,role_elem
having array_length(array_agg(distinct label),1)>1
)
,
assert_cl as
(
select a.version,a.rinok,a.entity,a.parentrole,a.parentrole role_elem,coalesce(l.label,a.label) label,a.label label_elem,a.id,base_type,value,period,tag_type,gv_test,fallbackvalue
from assert_all a
left join (select * from loc order by rinok,entity,parentrole,href_id) l on l.rinok=a.rinok and l.entity=a.entity and l.parentrole=a.parentrole and l.href_id=a.id 
order by a.version,a.rinok,a.entity,a.parentrole
)
,
roles_t as
(
select 'loc' fromtype,l.version,l.rinok,l.entity,a.parentrole,role_elem,l.label,label_elem,l.id,base_type,value,a.name,period,tag_type,gv_test,a.arcfrom,a.arcto,
 coalesce(aa.arcfrom,a.arcfrom) arcfrom_elem,coalesce(aa.arcto,a.arcto) arcto_elem,fallbackvalue
from assert_loc l
left join arc a on a.rinok=l.rinok and a.entity=l.entity and a.parentrole=l.parentrole and a.arcto=l.label
left join arc aa on a.rinok=l.rinok and aa.entity=l.entity and aa.parentrole=l.role_elem and aa.arcto=l.label_elem

union all

select 'cl' fromtype,l.version,l.rinok,l.entity,a.parentrole,role_elem,l.label,label_elem,l.id,base_type,value,a.name,period,tag_type,gv_test,arcfrom,arcto,arcfrom arcfrom_elem,arcto arcto_elem,fallbackvalue  
from assert_cl l
join arc a on a.rinok=l.rinok and a.entity=l.entity and a.parentrole=l.parentrole and a.arcto=l.label
order by 1,2,3,4
),
roles as
(
select distinct fromtype,version,rinok,entity,parentrole,role_elem,label,label_elem,id,base_type,value,name,period,tag_type,gv_test,arcfrom,arcto,arcfrom_elem,arcto_elem,
fallbackvalue
from
(
select fromtype,version,rinok,entity,parentrole,role_elem,label,label_elem,id,base_type,value,name,period,tag_type,gv_test,arcfrom,labels arcto,arcfrom_elem,labels arcto_elem,
fallbackvalue
from
(
select fromtype,version,rinok,entity,parentrole,role_elem,label,label_elem,id,base_type,value,name,period,tag_type,gv_test,arcfrom,arcto,arcfrom_elem,arcto_elem,
fallbackvalue,unnest(labels) labels
from roles_t r
join (select distinct rinok,entity,parentrole,href_id id,array_agg(distinct label) labels from loc l
group by rinok,entity,parentrole,href_id) l using (rinok,entity,parentrole,id)
where fromtype='loc' 
order by base_type
) l 
union all 
select * from roles_t  
) r
),
assert_rec as 
(
WITH 
 RECURSIVE recursive_hierarchy AS ( 
 select parentrole prole,fromtype,version,rinok,entity,parentrole parentrole,role_elem,label,id c_id,id fv_id,id parent_id,id child_id,base_type,base_type base_type_line,value,name c_name,period,tag_type,gv_test,
 arcfrom,arcto,arcfrom_elem,arcto_elem,fallbackvalue,split_part(value,'|',-1) msg
 from roles p
 WHERE  base_type = 'va'
 
 UNION ALL
 
 select prole,c.fromtype,c.version,c.rinok,c.entity,c.parentrole,c.role_elem,c.label,child_id,case when c.base_type='fv' then c.id else fv_id end fv_id,parent_id,c.id child_id,c.base_type,p.base_type_line||' - '||c.base_type,c.value,case when c.name is null then c_name else c.name end c_name,c.period,c.tag_type,
  c.gv_test,c.arcfrom,c.arcto,c.arcfrom_elem,c.arcto_elem,case when c.fallbackvalue is not null then c.fallbackvalue else p.fallbackvalue end fallbackvalue,msg
 from roles c
 INNER JOIN recursive_hierarchy p on p.rinok=c.rinok and p.entity=c.entity
 and (c.arcfrom=case when p.fromtype='loc' then p.arcto_elem else p.arcto end)
 and (c.parentrole=ANY(array[p.role_elem,p.parentrole]))
 where c.base_type!='va'
 )
 
 select distinct * from recursive_hierarchy 

),
res as not materialized
(
select distinct version,rinok,entity,prole,base_type,base_type_line,parent_id,child_id,fv_id,c_id,value,case when base_type='c' then c_name else null end c_name, 
period,tag_type,gv_test,fallbackvalue,msg
from assert_rec
order by entity,parent_id,base_type
)

select oo.version "Версия",oo.rinok "Рынок",oo.entity "Файл",oo.prole "Роль",oo.parent_id "ID КС",oo.base_type "Тип фильтра",oo.filter_id "ID фиьтра",cc.c_id "Дочерний элемент" from 
(
select distinct version,rinok,entity,prole,parent_id,base_type,child_id filter_id,c_id
from res
where base_type in ('fv','of','af')
order by rinok,entity,prole,parent_id
) oo
left join 
(
select distinct version,rinok,entity,prole,parent_id,base_type,c_id,child_id
from res
order by rinok,entity,prole,parent_id
) cc on cc.rinok=oo.rinok and cc.entity=oo.entity and cc.parent_id=oo.parent_id and cc.c_id=oo.filter_id
where cc.c_id is null
 
"""
df=do_sql(sql_1)
save_to_excel(df,sql_1,'Проверка слоя formula. Фильтры factVarible, orFilter, andFilter не имеют дочерних элементов (пустые)')


