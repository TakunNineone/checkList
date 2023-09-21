import psycopg2,pandas as pd,gc,warnings

warnings.filterwarnings("ignore")
def do_sql(sql):
    connect = psycopg2.connect(user="postgres",
                               password="124kosm21",
                               host="127.0.0.1",
                               port="5432",
                               database="final_5_30_2")
    df = pd.read_sql_query(sql, connect)
    connect.close()
    gc.collect()
    return df

def save_to_excel(df,sql,name):
    with pd.ExcelWriter(f"{name}.xlsx") as writer:
        df.to_excel(writer, index=False, sheet_name='result')
        df_sql=pd.DataFrame({'sql':[sql]})
        df_sql.to_excel(writer,index=False,sheet_name='SQL')
sql_bfo="""
   with ep as
(
select distinct replace(tp.entity,'.xsd','-definition.xml') entity_def,targetnamespace ep
from tableparts tp 
join tables t on t.version=tp.version and t.namespace=tp.uri_table and t.rinok=tp.rinok
-- where lower(targetnamespace) not similar to ('%support%')
order by 1
),
df as 
(
        select replace(entity,'.xsd','-definition.xml') entity,array_agg(distinct dim_def) dim_def
        from
        (
        select tp.entity,unnest(dim_def) dim_def
        from tableparts tp 
        join tables t on t.version=tp.version and t.namespace=tp.uri_table and t.rinok=tp.rinok
        left join (
                select dict_entity,array_agg(dim||'#'||mem) dim_def
                from
                (
                select e.qname dim,em.qname mem,e.entity,em.entity,a.entity,split_part(a.entity,'-definition.xml',1)||'.xsd' dict_entity,a.arcrole
                from locators l 
                join arcs a on a.version=l.version and a.rinok=l.rinok and a.entity=l.entity and a.arcfrom=l.label and a.parentrole=l.parentrole and arcrole='http://xbrl.org/int/dim/arcrole/dimension-default'
                join locators lm on  a.version=lm.version and a.rinok=lm.rinok and a.entity=lm.entity and a.arcto=lm.label and a.parentrole=lm.parentrole
                join elements e on e.id=l.href_id and e.version=l.version
                join elements em on em.id=lm.href_id and em.version=lm.version
                ) z
                group by dict_entity
                ) df on df.dict_entity = ANY(string_to_array(imports,';'))
        ) ee 
        group by replace(entity,'.xsd','-definition.xml')
),
def as
(
	select l.version,l.rinok,l.entity,l.parentrole,e.qname,l.label,arcfrom,arcto,arcrole,e.type,coalesce(e.abstract,'false') abstract,a.usable,targetrole,
        	case when arcrole='http://xbrl.org/int/dim/arcrole/domain-member' and coalesce(e.type,'')!='nonnum:domainItemType' then 1
        	when arcrole='http://xbrl.org/int/dim/arcrole/hypercube-dimension' then 2
        	when arcrole='http://xbrl.org/int/dim/arcrole/dimension-domain' then 3
        	when arcrole='http://xbrl.org/int/dim/arcrole/domain-member' then 4 
        	when arcrole='http://xbrl.org/int/dim/arcrole/notAll' then 5 
        	when arcrole='http://xbrl.org/int/dim/arcrole/all' then 0 else -1 end type_elem,
		typeddomainref
        from locators l
        join elements e on e.id=href_id and e.version=l.version
        join arcs a on a.arcto=l.label and l.version=a.version and l.rinok=a.rinok and l.entity=a.entity and a.parentrole=l.parentrole
        and a.arctype='definition' and l.rinok='bfo' and e.rinok!='eps'
-- 	and l.parentrole in ('http://www.cbr.ru/xbrl/bfo/rep/2023-11-30/tab/FR_4_003_05_01',
-- 'http://www.cbr.ru/xbrl/bfo/rep/2023-11-30/tab/FR_4_003_05a_01_39',
-- 'http://www.cbr.ru/xbrl/bfo/rep/2023-11-30/tab/FR_4_003_05b_01',
-- 'http://www.cbr.ru/xbrl/bfo/rep/2023-11-30/tab/FR_4_003_05c_01')
        order by arcrole
),
dd as
(
WITH RECURSIVE recursive_hierarchy AS ( 
  SELECT 
    version,rinok,entity,parentrole,targetrole,qname AS child_qname, 
    qname AS parent_qname,  -- Сохраняем "qname" родителя 
    arcfrom, 
    arcto, 
    label, 
    type_elem,typeddomainref
  FROM 
    def z
	
  WHERE 
    type_elem = 2  -- Начинаем с элементов типа 2 
 
  UNION ALL 
 
  SELECT 
    c.version,c.rinok,c.entity,c.parentrole,c.targetrole,c.qname AS child_qname, 
    p.parent_qname,  -- Передаем "qname" родителя 
    c.arcfrom, 
    c.arcto, 
    c.label, 
    c.type_elem,c.typeddomainref
  FROM 
    def c 
  INNER JOIN 
    recursive_hierarchy p ON c.arcfrom = p.arcto and c.version=p.version and c.rinok=p.rinok and c.entity=p.entity and c.parentrole=p.parentrole  
  WHERE 
    c.type_elem IN (3, 4)  -- Дети могут быть типа 3 или 4 
)

select version,rinok,entity,parentrole,string_to_array(unnest(generate_combinations(array_agg(dims))),'|') dims
	from
	(
select version,rinok,entity,parentrole,targetrole,parent_qname dim,
string_agg(parent_qname||case when child_qname=parent_qname then '' else '#' end||case when child_qname=parent_qname then '' else child_qname end,'|') dims
FROM 
recursive_hierarchy
where (type_elem>2 and typeddomainref is null or type_elem>=2 and typeddomainref is not null)
group by version,rinok,entity,parentrole,targetrole,parent_qname	
		)dd
group by version,rinok,entity,parentrole 	
)
		select version,rinok,ep,concept,array_to_string(coalesce(array_unique(dimensions),'{}'),';') dimensions,
 		array_length(array_agg(distinct parentrole),1) cnt,string_agg(distinct parentrole,';')
		from
		(
  		select distinct dd.version,dd.rinok,dd.entity,parentrole,rt.definition parentrole_text,concept,remove_elements_from_array(dims,dim_def) dimensions,dims
        from
        (
        select cc.version,cc.rinok,cc.entity,cc.parentrole,cc.qname concept,dims,
		compare_arrays(dims,dims_minus) is_minus
        from 
        (
        select version,rinok,entity,parentrole,qname,arcfrom,label,usable,targetrole 
        from def
        where type_elem=1
        and abstract='false'
        ) cc 
        left join dd using (version,rinok,entity,parentrole)
        left join 
        (
		select d1.version,d1.rinok,d1.entity,d1.parentrole,d1.arcfrom,array_agg(array_to_string(dims,';')) dims_minus
        from def d1 
        join dd d2 on d1.version=d2.version and d1.rinok=d2.rinok and d2.parentrole=d1.targetrole
		and d1.type_elem=5
		group by d1.version,d1.rinok,d1.entity,d1.parentrole,d1.arcfrom
        ) tr on tr.version=cc.version and cc.rinok=tr.rinok and cc.entity=tr.entity and cc.parentrole=tr.parentrole and tr.arcfrom=cc.label
		) dd
		left join df on df.entity=dd.entity	
        left join roletypes rt on rt.roleuri=dd.parentrole
        where is_minus=0
		) zz
		left join ep on ep.entity_def=zz.entity
		--where concept='ifrs-ru:AktivyOtchetnyxSegmentov'
 		group by version,rinok,ep,concept,array_to_string(coalesce(array_unique(dimensions),'{}'),';')
 		having array_length(array_agg(distinct parentrole),1)>1
        order by version,rinok,ep,concept


"""
sql_1="""
select r.version,r.rinok,r.entity,r.parentrole,rc.value,count(distinct r.id) cnt_rulenodes,string_agg(distinct r.id,';') rulenodes
from rulenodes r
join rulenodes_c rc on rc.version=r.version and rc.entity=r.entity and rc.rinok=r.rinok and rc.parentrole=r.parentrole and rc.rulenode_id=r.id
group by r.version,r.rinok,r.entity,r.parentrole,rc.value
having count(distinct r.id)>1
"""
df=do_sql(sql_bfo)
save_to_excel(df,sql_bfo,'дубли точек данных - 2')


