import psycopg2,pandas as pd,gc,warnings,pickle

warnings.filterwarnings("ignore")
def do_sql(sql):
    connect = psycopg2.connect(user="postgres",
                               password="124kosm21",
                               host="127.0.0.1",
                               port="5432",
                               database="final_6_1")
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
df as 
(
        select replace(entity,'.xsd','-formula.xml') entity,array_agg(distinct dim_def) dim_def
        from
        (
        select tp.entity,unnest(dim_def) dim_def
        from tableparts tp 
        join tables t on t.namespace=tp.uri_table and t.rinok=tp.rinok
        left join (
                select dict_entity,array_agg(dim||'#'||mem) dim_def
                from
                (
                select e.qname dim,em.qname mem,e.entity,em.entity,a.entity,split_part(a.entity,'-definition.xml',1)||'.xsd' dict_entity,a.arcrole
                from locators l 
                join arcs a on  a.rinok=l.rinok and a.entity=l.entity and a.arcfrom=l.label and a.parentrole=l.parentrole and arcrole='http://xbrl.org/int/dim/arcrole/dimension-default'
                join locators lm on  a.rinok=lm.rinok and a.entity=lm.entity and a.arcto=lm.label and a.parentrole=lm.parentrole
                join elements e on e.id=l.href_id
                join elements em on em.id=lm.href_id
				left join 
				(select pl.parentrole,qname,string_agg(distinct text,';') pl_text
					from preferred_labels pl
				group by pl.parentrole,qname) plm on plm.qname=em.qname and plm.parentrole=l.parentrole
                ) z
                group by dict_entity
                ) df on df.dict_entity = ANY(string_to_array(imports,';'))
        ) ee 
        group by replace(entity,'.xsd','-formula.xml')
	order by 1
),
def_temp as not materialized
(
 select l.version,l.rinok,l.entity,l.parentrole,qname,l.label,arcfrom,arcto,arcrole,e_type,coalesce(l.abstract,'false') abstract,a.usable,targetrole,
         case when arcrole='http://xbrl.org/int/dim/arcrole/domain-member' and coalesce(e_type,'')!='nonnum:domainItemType' then 1
         when arcrole='http://xbrl.org/int/dim/arcrole/hypercube-dimension' then 2
         when arcrole='http://xbrl.org/int/dim/arcrole/dimension-domain' then 3
         when arcrole='http://xbrl.org/int/dim/arcrole/domain-member' then 4 
         when arcrole='http://xbrl.org/int/dim/arcrole/notAll' then 5 
         when arcrole='http://xbrl.org/int/dim/arcrole/all' then 0 else -1 end type_elem,
  typeddomainref
        from (select l.*,e.type e_type,abstract,typeddomainref,qname from (select * from locators order by 1,2,3,4) l join elements e on e.id=l.href_id order by l.version,l.rinok,l.entity,l.parentrole) l
        join arcs a on a.arcto=l.label and l.rinok=a.rinok and l.entity=a.entity and a.parentrole=l.parentrole
        and a.arctype='definition'  
		where lower(l.parentrole) not similar to '%chasti%'
        order by l.version,l.rinok,l.entity,l.parentrole,l.qname
),
def as
(
 select l.version,d_.rinok,d_.entity,d_.parentrole,qname,l.label,case when arcrole='http://xbrl.org/int/dim/arcrole/dimension-domain' and l.parentrole is not null then d_.label else arcfrom end  arcfrom,arcto,arcrole,e_type,coalesce(l.abstract,'false') abstract,a.usable,d_.targetrole targetrole,
         case when arcrole='http://xbrl.org/int/dim/arcrole/domain-member' and coalesce(e_type,'')!='nonnum:domainItemType' then 1
         when arcrole='http://xbrl.org/int/dim/arcrole/hypercube-dimension' then 2
         when arcrole='http://xbrl.org/int/dim/arcrole/dimension-domain' then 3
         when arcrole='http://xbrl.org/int/dim/arcrole/domain-member' then 4 
         when arcrole='http://xbrl.org/int/dim/arcrole/notAll' then 5 
         when arcrole='http://xbrl.org/int/dim/arcrole/all' then 0 else -1 end type_elem,
  typeddomainref
        from (select l.*,e.type e_type,abstract,typeddomainref,qname from (select * from locators order by 1,2,3,4) l join elements e on e.id=l.href_id order by l.version,l.rinok,l.entity,l.parentrole) l
        join arcs a on a.arcto=l.label and l.rinok=a.rinok and l.entity=a.entity and a.parentrole=l.parentrole
        and a.arctype='definition'
		join (select distinct version,rinok,entity,parentrole,targetrole,label from def_temp ) d_ on d_.targetrole=l.parentrole
		where lower(l.parentrole) not similar to '%chasti%'
		
	
	union all
	
	select * from def_temp
),
pl as not materialized
(
select rinok,entity,c.parentrole,concept qname,dims,eps,text,role
from
(
select distinct rinok,entity,parentrole,qname concept from def 
where type_elem =1
) c
left join
(
select distinct rinok,entity,parentrole,array_unique(array_agg(distinct qname)) dims  
from def 
where type_elem =2
group by rinok,entity,parentrole
) d using (rinok,entity,parentrole)
join
(
select definition,array_unique(array_agg(distinct entity)) eps
from
(
select distinct entity,replace(split_part(schemalocation,'/',-1),'.xsd','-definition.xml') definition
from tables t
where entity not like '%support%'
) t
group by definition
) ee on ee.definition=c.entity
join (select distinct parentrole,qname,text,role from preferred_labels) pl on pl.parentrole=c.parentrole and pl.qname=c.concept
order by entity,parentrole,qname
),
arcs_ as 
(
select  distinct va.rinok,va.entity,va.parentrole,split_part(va.parentrole,'/',-1) arcfrom,va.label arcto,name,arcrole,a.title,'formula' arctype,complement,cover
from va_assertions va
join arcs a on a.rinok=va.rinok and a.entity=va.entity and a.parentrole=va.parentrole and a.arcfrom=va.label	
where arcrole not in ('http://xbrl.org/arcrole/2016/assertion-unsatisfied-severity','http://xbrl.org/arcrole/2008/variable-set-precondition')
and arctype='formula' 
and va.rinok='uk'
union all
	
select distinct rinok,entity,parentrole,arcfrom,arcto,name,arcrole,title,arctype,complement,cover
from arcs
where arctype='formula'
and rinok='uk'
order by 1,2,3,4
)
,
assert_all as not materialized
(
select 	rinok,entity,parentrole,label,id,base_type,value,period,tag_type,gv_test
from
(	
select ii.rinok,ii.entity,ii.parentrole,va_d.label,dimension_id id,'m' base_type,member value,null period,null tag_type,null gv_test
from va_edmembers ii
join va_edimensions va_d on va_d.id=ii.dimension_id and va_d.entity=ii.entity and va_d.parentrole=ii.parentrole
union all
select rinok,entity,parentrole,label,id,'t' base_type,value,null,null,null
from va_tdimensions 
union all
select rinok,entity,parentrole,label,id,'e' base_type,dimension,null,null,null from
(
select e.rinok,e.entity,e.parentrole,label,id,dimension,dimension||'#'||member dim_e
from va_edimensions e
join va_edmembers m on m.rinok=e.rinok and m.entity=e.entity and m.parentrole=e.parentrole and m.dimension_id=e.id
order by e.entity
) ee 
left join df using(entity)
where case when dim_e=ANY(dim_def) then 1 else 0 end = 0
union all
select cc.rinok,cc.entity,cc.parentrole,cc.label,cc.id,'c' base_type,cc.value,null,null,null
from va_concepts cc
union all
select rinok,entity,parentrole,label,id,'ac' base_type,dims,null,null,null
from va_aspectcovers
union all
select rinok,entity,parentrole,label,id,'fv' base_type,null,null,null,null
from va_factvars
union all
select rinok,entity,parentrole,label,id,'as' base_type,null,null,null,null
from va_assertionsets
union all
select rinok,entity,parentrole,label,id,'of' base_type,null,null,null,null
from va_orfilters
union all
select rinok,entity,parentrole,label,id,'g' base_type,null,null,null,test
from va_generals
union all
select rinok,entity,parentrole,label,id,'va' base_type,null,null,null,null
from va_assertions
union all
select rinok,entity,parentrole,label,id,'p' base_type,null,date,tag_type,null
from va_periods
) zz
where rinok='uk'
order by rinok,entity,parentrole,label
),
check_roles as
(
select distinct v.rinok,v.entity,v.id,
array_unique(array_agg(distinct al.parentrole||array[v.parentrole])) roles,array_unique(array_agg(v.arcto)||array_agg(l.href_id)||array_agg(al.label)) hrefs
	from
(
select distinct v.rinok,v.entity,a.parentrole,id,arcto
from va_link v
join (select * from arcs where arctype='formula' and arcrole not in ('http://xbrl.org/arcrole/2016/assertion-unsatisfied-severity','http://xbrl.org/arcrole/2008/variable-set-precondition') order by rinok,entity,parentrole,arcfrom) a on a.parentrole=v.role and a.entity=v.entity
join (select version,rinok,entity,parentrole,label,id from va_assertions order by rinok,entity,parentrole,label) aa on aa.rinok=a.rinok and aa.entity=a.entity and aa.parentrole=a.parentrole  and aa.label=a.arcfrom
) v
left join (select * from locators where locfrom='formula' order by rinok,entity,parentrole,label) l on l.rinok=v.rinok and  l.entity=v.entity and l.parentrole=v.parentrole and l.label=v.arcto
left join assert_all al on al.rinok=l.rinok and  al.entity=l.entity and al.id=l.href_id  
group by v.rinok,v.entity,v.id
)
,
assert_al_t as
(
select '1' type_,a.rinok,a.entity,a.parentrole,aa.id,aa.base_type,aa.value,a.arcfrom,arcto arcto,null check_va,a.name,a.arcrole,a.title,period,tag_type,gv_test,a.complement,a.cover
from va_link v
join arcs_ a on a.parentrole=v.role and a.entity=v.entity
join assert_all aa on aa.rinok=a.rinok and aa.entity=a.entity and aa.parentrole=a.parentrole and aa.label=a.arcto and aa.base_type='va'
and  arcrole='http://xbrl.org/arcrole/2008/variable-set'

union all

select '2' type_,a.rinok,a.entity,l.parentrole,aa.id,aa.base_type,aa.value,a.arcfrom,unnest(array[a.arcto,aa.label]) arcto,null check_va,a.name,a.arcrole,a.title,aa.period,aa.tag_type,aa.gv_test,a.complement,a.cover
from va_link v
join arcs_ a on a.parentrole=v.role and a.entity=v.entity and a.rinok=v.rinok
join (select * from locators where locfrom='formula' order by rinok,entity,parentrole,label) l on l.rinok=a.rinok and l.entity=a.entity and l.parentrole=a.parentrole and l.label=a.arcto
join assert_all aa on aa.rinok=l.rinok and aa.entity=l.entity and aa.id=l.href_id 
--and a.arcrole in ('http://xbrl.org/arcrole/2008/variable-filter','http://xbrl.org/arcrole/2008/variable-set-filter','http://xbrl.org/arcrole/2008/boolean-filter')

union all

select '3' type_,a.rinok,a.entity,a.parentrole,aa.id,aa.base_type,aa.value,a.arcfrom,a.arcto,null check_va,a.name,a.arcrole,a.title,period,tag_type,gv_test,a.complement,a.cover
from va_link v
join arcs_ a on a.parentrole=v.role and a.entity=v.entity and a.rinok=v.rinok
join assert_all aa on aa.rinok=a.rinok and aa.entity=a.entity and aa.parentrole=a.parentrole and aa.label=a.arcto
where base_type!='va'
order by 1,2,3,4,5
)
,
roles
as 
(
select distinct a.*,roles,hrefs from assert_al_t a
left join check_roles c on c.id=a.id and c.entity=a.entity and c.rinok=a.rinok
order by rinok,entity,arcfrom,arcto
)
,
assert_rec as not materialized
(
WITH RECURSIVE recursive_hierarchy AS ( 
	select base_type parent_base_type,base_type base_type_line,type_,rinok,entity,parentrole crole,parentrole prole,roles,hrefs ch,hrefs ph,id child_id,id parent_id,base_type,value,arcfrom,arcto,check_va,name c_name,arcrole,title,period,tag_type,gv_test,
	complement,cover
	from roles p
	WHERE  base_type = 'va'
	
	UNION ALL
	
	select 	p.base_type,p.base_type_line||' - '||c.base_type,c.type_,c.rinok,c.entity,c.parentrole,prole,p.roles,c.hrefs,ph,c.id,parent_id,c.base_type,c.value,c.arcfrom,c.arcto,c.check_va,case when c.name is null then c_name  else c.name end c_name,c.arcrole,c.title,c.period,c.tag_type,c.gv_test,
	c.complement,c.cover
	from roles c
	INNER JOIN recursive_hierarchy p on p.rinok=c.rinok and p.entity=c.entity and c.arcfrom=p.arcto  and (c.parentrole = ANY (p.roles) or c.parentrole=prole)
	and case when c.base_type='fv' then c.arcto else '1' end = ANY (ph||array['1'])
	where c.base_type!='va'
	)
	
	select distinct * from recursive_hierarchy 
	order by rinok,entity,crole,value
)

-- select * from assert_rec where parent_id='valueAssertion_0420502_rassh_r3_p1_p1_01'


select rinok,parent_id,c_name,value,string_agg( distinct text,'/') text
from
(
select distinct crole,base_type_line,rinok,parent_id,c_name,value,dims_r,
coalesce(dims_r,taxiss) osi,coalesce(coalesce(pl.dims,pl2.dims),'{}'::text[]) dims_pl,
coalesce(coalesce(pl.text,pl2.text),e.text) text,
case when tag_type = 'instant' then case when period like 'max(%' or period = '$repdate' or period like '%xs:date(.))' or period like 'max(%xbrli:instant)' then 'end' else 'start' end when tag_type in ('start','end') then tag_type else null end period_type,
tag_type,
case when pl.text is not null then 'преферд с периодами' when pl.text is null and pl2.text is not null then 'просто преферд' when pl.text is null and pl2.text is null and e.text is not null then 'обычный лейбл' end type_label,tag_type,
period,zz.entity,child_id,zz.eps,coalesce(pl.eps,pl2.eps) pl_eps
from
(
select distinct cc.rinok,cc.entity,cc.child_id,cc.value,dims_r,array_unique(array_agg(distinct taxiss)||array_agg(distinct axiss)) taxiss,cc.parent_id,coalesce(gg.gv_test,pp.period) period,pp.tag_type,cc.c_name,cc.crole,eps,gg.entity g_entity,cc.base_type_line
from (select * from assert_rec where base_type='c' order by roles,entity,rinok,arcfrom) cc
left join
(
select parent_id,c_name,remove_elements_from_array(dims_e,value::text[]) dims_r
from assert_rec ac
join
(
select distinct parent_id,array_agg(distinct value) dims_e
from assert_rec
where base_type in ('e','t') and coalesce(complement,'')!='true'
group by parent_id
) ee using(parent_id)
where base_type='ac' 
and value !='{}'
order by parent_id,c_name
) ac using(parent_id,c_name)
left join
(
select formula,array_unique(array_agg(distinct entity)) eps
from
(
select distinct entity,replace(split_part(schemalocation,'/',-1),'.xsd','-formula.xml') formula
from tables t
where entity not like '%support%'
) t
group by formula
) ee on ee.formula=cc.entity
left join (select * from assert_rec where base_type='p' order by roles,entity,rinok,arcfrom) pp on pp.crole=pp.crole and pp.entity=cc.entity and pp.rinok=cc.rinok and pp.arcfrom=cc.arcfrom and pp.parent_id=cc.parent_id
left join (select distinct entity,rinok,parent_id,value taxiss,arcfrom,base_type_line
from assert_rec where base_type='t' and complement!='true' order by entity,rinok,parent_id,arcfrom) tt on tt.entity=cc.entity and tt.rinok=cc.rinok and tt.parent_id=cc.parent_id and (cc.arcfrom=tt.arcfrom or tt.base_type_line not like '%fv%')
left join (select distinct entity,rinok,parent_id,value axiss,arcfrom,base_type_line
from assert_rec where base_type='e'  and complement!='true' order by entity,rinok,parent_id,arcfrom) xx on xx.entity=cc.entity and xx.rinok=cc.rinok and xx.parent_id=cc.parent_id and (cc.arcfrom=xx.arcfrom or xx.base_type_line not like '%fv%')
left join (select * from assert_rec where base_type='g' order by roles,entity,rinok,arcfrom) gg on pp.crole=gg.crole and pp.entity=gg.entity and pp.rinok=gg.rinok and '$'||gg.c_name=pp.period and gg.parent_id=cc.parent_id
where cc.base_type in ('c') 
and cc.base_type_line not like '%of%'
group by cc.rinok,cc.entity,cc.child_id,cc.value,cc.parent_id,coalesce(gg.gv_test,pp.period),pp.tag_type,cc.c_name,cc.crole,eps,gg.entity,cc.base_type_line,dims_r
order by eps,6,value--cc.crole,cc.value
)zz
left join 
(
select parentrole,replace(entity,'-definition.xml','-formula.xml') entity,qname,dims,eps,case when role = 'http://www.xbrl.org/2003/role/periodEndLabel' then replace(role,'http://www.xbrl.org/2003/role/periodEndLabel','end') 
when role = 'http://www.xbrl.org/2003/role/periodStartLabel' then replace(role,'http://www.xbrl.org/2003/role/periodStartLabel','start') end period_type_role,role,text
from pl where role in ('http://www.xbrl.org/2003/role/periodEndLabel','http://www.xbrl.org/2003/role/periodStartLabel') 
order by eps,dims,qname
) pl on compare_arrays_tt(pl.eps,zz.eps)=1 and pl.qname=zz.value and arr1_in_arr2_hard(coalesce(dims_r,taxiss),coalesce(pl.dims,'{}'::text[]))=1 
and (pl.period_type_role=case when tag_type = 'instant' then case when period like 'max(%' or period = '$repdate' or period like '%xs:date(.))' or period like 'max(%xbrli:instant)' then 'end' else 'start' end when tag_type in ('start','end') then tag_type else null end
	or tag_type is null)
left join 
(
select distinct qname,text from elements_labels e where e.lang='ru' and e.role='http://www.xbrl.org/2003/role/label'
	order by qname
) e on e.qname=zz.value 
left join 
(
select distinct parentrole,replace(entity,'-definition.xml','-formula.xml') entity,qname,dims,eps,text 
from pl where role not in ('http://www.xbrl.org/2003/role/periodEndLabel','http://www.xbrl.org/2003/role/periodStartLabel')
order by eps,dims,qname
) pl2 on pl2.qname=zz.value and compare_arrays_tt(pl2.eps,zz.eps)=1 and arr1_in_arr2_hard(coalesce(dims_r,taxiss),coalesce(pl2.dims,'{}'::text[]))=1 
-- where rinok='npf' and  base_type_line not like '%of%'
) zz
group by rinok,parent_id,c_name,value
order by rinok,parent_id,value,c_name

"""
df=do_sql(sql_1)
save_to_excel(df,sql_1,'tver_uk_2')


