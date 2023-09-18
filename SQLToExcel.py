import psycopg2,pandas as pd,gc,warnings

warnings.filterwarnings("ignore")
def do_sql(sql):
    connect = psycopg2.connect(user="postgres",
                               password="124kosm21",
                               host="127.0.0.1",
                               port="5432",
                               database="final_5_2_b")
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
      with 
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
        and a.arctype='definition' and l.rinok!='bfo'
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
		select version,rinok,entity,concept,dimensions,array_agg(distinct parentrole)
		from
		(
  		select distinct dd.version,dd.rinok,dd.entity,parentrole,rt.definition parentrole_text,concept,array_to_string(dims,';') dimensions
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
        left join roletypes rt on rt.roleuri=dd.parentrole
        where is_minus=0
		) zz
		group by version,rinok,entity,concept,dimensions
		having array_length(array_agg(distinct parentrole),1)>1
        order by version,rinok,concept
"""
sql_1="""

        with 
          def as
        (
        select l.version,l.rinok,l.entity,l.parentrole,e.qname,l.label,arcfrom,arcto,arcrole,e.type,coalesce(e.abstract,'false') abstract,a.usable,targetrole,
        	case when arcrole='http://xbrl.org/int/dim/arcrole/domain-member' and coalesce(e.type,'')!='nonnum:domainItemType' then 1
        	when arcrole='http://xbrl.org/int/dim/arcrole/hypercube-dimension' then 2
        	when arcrole='http://xbrl.org/int/dim/arcrole/dimension-domain' then 3
        	when arcrole='http://xbrl.org/int/dim/arcrole/domain-member' then 4 
        	when arcrole='http://xbrl.org/int/dim/arcrole/notAll' then 5 
        	when arcrole='http://xbrl.org/int/dim/arcrole/all' then 0 else -1 end type_elem
        from locators l
        join elements e on e.id=href_id and e.version=l.version and e.rinok!='eps'
        join arcs a on a.arcto=l.label and l.version=a.version and l.rinok=a.rinok and l.entity=a.entity and a.parentrole=l.parentrole
        and a.arctype='definition' 
        where l.parentrole in 
                (select roleuri from rolerefs where entity in ('FR_4_004_01a_01_39-definition.xml','FR_4_008_01_02-definition.xml','FR_2_055_02a_01_39-definition.xml','FR_4_003_03_01-definition.xml','FR_2_043_02_01_39-definition.xml','FR_2_024_01_02-definition.xml','FR_2_017_01a_01_39-definition.xml','FR_4_005_02a_01_39-definition.xml','FR_3_018_01a_01-definition.xml','FR_2_012_04_01_39-definition.xml','FR_4_009_01a_01_39-definition.xml','FR_4_002_02a_01_39-definition.xml','FR_2_026_01_02-definition.xml','FR_3_006_01a_01_39-definition.xml','FR_4_003_01a_02_39-definition.xml','FR_4_008_02a_05_39-definition.xml','FR_2_012_01a_03_39-definition.xml','FR_2_010_01a_02_39-definition.xml','FR_2_042_01a_01_39-definition.xml','FR_4_003_04_01-definition.xml','FR_2_023_02_01-definition.xml','FR_4_008_08a_01_39-definition.xml','FR_1_001_01a_01_39-definition.xml','FR_4_003_06a_02_39-definition.xml','FR_4_004_04a_03_39-definition.xml','FR_2_015_01_01_39-definition.xml','FR_4_008_04_02-definition.xml','FR_3_002_01_01-definition.xml','FR_3_019_01_02-definition.xml','FR_2_055_04a_01-definition.xml','FR_2_047_03a_01_39-definition.xml','FR_4_008_02a_01_39-definition.xml','FR_2_030_01a_03-definition.xml','FR_2_013_04a_01_39-definition.xml','FR_3_012_01a_01_39-definition.xml','FR_4_007_03a_01_39-definition.xml','FR_2_011_01a_02_39-definition.xml','FR_PL_NPF_AO_39-definition.xml','FR_2_024_02_01-definition.xml','FR_3_014_01a_01_39-definition.xml','FR_3_003_01a_01_39-definition.xml','FR_2_027_09a_01-definition.xml','FR_3_036_01a_01_39-definition.xml','FR_4_005_01_03_39-definition.xml','FR_2_045_02_01_39-definition.xml','FR_2_028_01_02-definition.xml','FR_2_016_04a_01_39-definition.xml','FR_2_020_01_02_39-definition.xml','FR_2_025_01_02-definition.xml','FR_3_001_02_02-definition.xml','FR_4_002_03a_01_39-definition.xml','FR_3_005_02a_01_39-definition.xml','FR_4_008_01a_05_39-definition.xml','FR_4_004_17a_01_39-definition.xml','FR_2_047_09a_01_39-definition.xml','FR_2_013_03a_01_39-definition.xml','FR_2_011_01a_01_39-definition.xml','FR_2_043_01a_01_39-definition.xml','FR_4_004_18a_01_39-definition.xml','FR_4_004_35a_01_39-definition.xml','FR_4_004_19a_01-definition.xml','FR_4_003_02a_01_39-definition.xml','FR_4_008_01a_07_39-definition.xml','FR_4_008_01a_08_39-definition.xml','FR_4_005_04_02-definition.xml','FR_2_055_01a_01-definition.xml','FR_4_005_03a_02_39-definition.xml','FR_2_047_04a_01_39-definition.xml','FR_2_008_02_01-definition.xml','FR_4_004_31a_01_39-definition.xml','FR_3_004_01a_01_39-definition.xml','FR_4_008_03a_05_39-definition.xml','FR_2_016_02a_01_39-definition.xml','FR_2_027_07_01-definition.xml','FR_4_004_10a_01_39-definition.xml','FR_2_012_02_03-definition.xml','FR_1_003_01a_01_39-definition.xml','FR_4_005_02a_04_39-definition.xml','FR_3_013_01_01_39-definition.xml','FR_3_018_01_02-definition.xml','FR_2_048_01_02_39-definition.xml','FR_3_018_02_02-definition.xml','FR_2_027_02_02-definition.xml','FR_4_007_03a_02-definition.xml','FR_2_027_03a_01_39-definition.xml','FR_4_004_33a_01_39-definition.xml','FR_3_001_01_02-definition.xml','FR_CF_NPF_AO_39_open-definition.xml','FR_4_004_03a_02_39-definition.xml','FR_2_046_01a_01_39-definition.xml','FR_2_012_01a_01_39-definition.xml','FR_CoA_npf_ao_39-definition.xml','FR_4_003_03_02-definition.xml','FR_4_008_03a_01_39-definition.xml','FR_4_004_31a_02_39-definition.xml','FR_4_003_01a_01_39-definition.xml','FR_2_024_03_01-definition.xml','FR_3_018_04_02-definition.xml','FR_4_010_03a_01_39-definition.xml','FR_SOCIE_NPF_AO_39-definition.xml','FR_2_009_01a_02_39-definition.xml','FR_2_046_01a_02_39-definition.xml','FR_4_004_03a_03_39-definition.xml','FR_4_004_34a_01_39-definition.xml','FR_4_004_04a_01_39-definition.xml','FR_4_008_02a_08_39-definition.xml','FR_4_003_06a_03_39-definition.xml','FR_2_047_06a_01_39-definition.xml','FR_3_001_04_01-definition.xml','FR_2_047_08a_01_39-definition.xml','FR_3_001_07_01-definition.xml','FR_4_003_06a_01_39-definition.xml','FR_2_047_02a_01_39-definition.xml','FR_4_008_04a_01_39-definition.xml','FR_2_042_02a_01_39-definition.xml','FR_3_015_01a_01_39-definition.xml','FR_2_020_01a_01_39-definition.xml','FR_3_019_01a_01_39-definition.xml','FR_4_002_01a_01_39-definition.xml','FR_2_044_02a_01_39-definition.xml','FR_2_013_01a_01_39-definition.xml','FR_4_008_01a_02_39-definition.xml','FR_4_005_04_01-definition.xml','FR_2_055_03a_01-definition.xml','FR_4_006_04_01-definition.xml','FR_4_008_02a_06_39-definition.xml','FR_2_016_01a_01_39-definition.xml','FR_3_002_02_02-definition.xml','FR_2_001_03_01_39-definition.xml','FR_2_055_06a_01-definition.xml','FR_2_042_01a_02_39-definition.xml','FR_2_021_02_01-definition.xml','FR_4_005_01_01_39-definition.xml','FR_4_001_01_01-definition.xml','FR_2_012_02_01_39-definition.xml','FR_4_008_03a_08_39-definition.xml','FR_3_002_01_02-definition.xml','FR_3_037_01a_01_39-definition.xml','FR_4_011_01_01-definition.xml','FR_4_004_10a_02_39-definition.xml','FR_2_008_02_02-definition.xml','FR_2_027_01a_01_39-definition.xml','FR_BS_NPF_AO_39_open-definition.xml','FR_3_017_01a_01_39-definition.xml','FR_2_025_02a_01_39-definition.xml','FR_2_014_01a_01_39-definition.xml','FR_4_006_03a_01_39-definition.xml','FR_2_043_05_01_39-definition.xml','FR_2_026_02_02-definition.xml','FR_4_005_07_02-definition.xml','FR_1_004_01a_01_39-definition.xml','FR_4_004_03a_01_39-definition.xml','FR_2_027_08_01-definition.xml','FR_4_005_02a_02_39-definition.xml','FR_CF_NPF_AO_39-definition.xml','FR_4_008_03a_02_39-definition.xml','FR_4_005_02a_03_39-definition.xml','FR_2_047_05a_01_39-definition.xml','FR_3_001_07_03-definition.xml','FR_3_001_07_02-definition.xml','FR_Tekst_raskr-definition.xml','FR_2_001_01a_01_39-definition.xml','FR_1_002_01a_01_39-definition.xml','FR_4_005_03a_04_39-definition.xml','FR_2_025_02_02-definition.xml','FR_4_007_05a_01_39-definition.xml','FR_4_007_05_02-definition.xml','FR_2_008_01a_01_39-definition.xml','FR_2_021_01_01-definition.xml','FR_4_005_07a_01_39-definition.xml','FR_2_047_01a_01_39-definition.xml','FR_3_014_01_02-definition.xml','FR_4_008_05a_01-definition.xml','FR_4_005_01_04_39-definition.xml','FR_4_010_02a_01_39-definition.xml','FR_4_005_03a_01_39-definition.xml','FR_2_001_03_02_39-definition.xml','FR_4_004_10a_03_39-definition.xml','FR_2_017_03_01-definition.xml','FR_4_005_03a_03_39-definition.xml','FR_3_002_04_01-definition.xml','FR_4_008_01a_04_39-definition.xml','FR_4_005_05a_01_39-definition.xml','FR_2_048_01_01_39-definition.xml','FR_2_012_03a_01_39-definition.xml','FR_2_009_01a_01_39-definition.xml','FR_3_005_03a_01_39-definition.xml','FR_4_003_06_04-definition.xml','FR_2_001_01a_02_39-definition.xml','FR_3_018_04_01-definition.xml','FR_3_001_01a_01_39-definition.xml','FR_4_005_01_02_39-definition.xml','FR_4_008_03a_07_39-definition.xml','FR_4_003_05a_01_39-definition.xml','FR_4_007_04a_01_39-definition.xml','FR_ORGINFO-definition.xml','FR_2_015_03_01-definition.xml','FR_2_025_01_01-definition.xml','FR_4_004_31a_03_39-definition.xml','FR_3_005_01a_01_39-definition.xml','FR_SOCIE_NPF_AO_39_open-definition.xml','FR_4_008_02a_02_39-definition.xml','FR_2_043_03_01_39-definition.xml','FR_4_007_01_01_39-definition.xml','FR_4_010_01_02-definition.xml','FR_2_027_02a_01_39-definition.xml','FR_2_029_01a_01_39-definition.xml','FR_3_018_02_03-definition.xml','FR_2_045_01a_01_39-definition.xml','FR_2_024_01_01-definition.xml','FR_2_030_01_01-definition.xml','FR_4_005_06a_01_39-definition.xml','FR_3_001_03a_01_39-definition.xml','FR_2_055_05_01_39-definition.xml','FR_4_004_31a_04_39-definition.xml','FR_PL_NPF_AO_39_open-definition.xml','FR_4_004_04a_02_39-definition.xml','FR_4_008_01a_03_39-definition.xml','FR_3_018_02a_01-definition.xml','FR_2_027_04a_01_39-definition.xml','FR_2_020_03a_01_39-definition.xml','FR_4_008_03a_03_39-definition.xml','FR_3_002_03_01-definition.xml','FR_2_044_01a_01_39-definition.xml','FR_4_003_01_03-definition.xml','FR_2_010_01a_01_39-definition.xml','FR_2_027_06_02-definition.xml','FR_4_004_15a_01_39-definition.xml','FR_4_008_01a_06_39-definition.xml','FR_2_043_04_01_39-definition.xml','FR_2_008_03_02-definition.xml','FR_BS_NPF_AO_39-definition.xml','FR_3_017_02a_01_39-definition.xml','FR_2_012_01a_02_39-definition.xml','FR_4_008_06a_01_39-definition.xml','FR_4_003_04a_02_39-definition.xml','FR_2_014_01_02-definition.xml','FR_4_004_32a_01_39-definition.xml','FR_2_023_01_01-definition.xml','FR_4_008_02a_03_39-definition.xml','FR_3_002_02_01_39-definition.xml','FR_3_001_02a_01_39-definition.xml','FR_2_021_01_02-definition.xml','FR_2_026_01_01-definition.xml','FR_2_023_03a_01_39-definition.xml','FR_4_007_02a_01_39-definition.xml','FR_4_008_02a_07_39-definition.xml','FR_4_004_02a_01_39-definition.xml','FR_2_016_03a_01_39-definition.xml','FR_4_010_01a_01_39-definition.xml','FR_4_008_03_02-definition.xml','FR_2_008_03a_01_39-definition.xml','FR_3_004_02a_01_39-definition.xml','FR_4_003_07a_01_39-definition.xml','FR_2_028_01_01_39-definition.xml','FR_4_008_03a_06_39-definition.xml','FR_2_001_02_01_39-definition.xml','FR_4_008_02a_04_39-definition.xml','FR_2_026_02_01-definition.xml','FR_4_004_16a_01_39-definition.xml','FR_4_008_03a_04_39-definition.xml','FR_4_003_01_Info-definition.xml','FR_2_012_02_02-definition.xml','FR_2_027_06a_01_39-definition.xml','FR_4_004_20a_01-definition.xml','FR_2_027_05a_01_39-definition.xml','FR_2_027_05_02-definition.xml','FR_4_008_01a_01_39-definition.xml') and rinok='bfo' and roleuri is not null)
                 
        	order by arcrole
        )
		
		
		
        select version,rinok,dd.entity,parentrole,split_part(dims,'#',1) dim, 
		array_to_string(case when array_agg(dims) is not null and dim_def is not null then delete_default_dims(array_agg(dims),dim_def) else array_agg(dims) end,'|') dims,
		array_agg(dims) dims_w,dim_def
        from
        (
        select version,rinok,entity,parentrole,unnest(case when array_length(dim2,1)>0 then dim1||dim2 else dim1 end) dims
        from
        (
         select dd.version,dd.rinok,dd.entity,dd.parentrole,
                array_agg(dd.qname||case when dd.qname||'#'||coalesce(dd3.qname,dd2.qname) is not null then '#' else '' end||coalesce(coalesce(dd3.qname,dd2.qname),'')) dim1,
                array_remove(array_agg(distinct case when  dd3.qname is not null then dd.qname||'#'||dd2.qname end),null) dim2
                from 
        		(select version,rinok,entity,parentrole,qname,arcfrom,label,usable from def
                where type_elem=2) dd
                left join (select version,rinok,entity,parentrole,qname,arcfrom,label,usable from def
                where type_elem=3) dd2 on dd.version=dd2.version and dd2.rinok=dd.rinok and dd2.entity=dd.entity and dd2.parentrole=dd.parentrole and dd2.arcfrom=dd.label
                left join (select version,rinok,entity,parentrole,qname,arcfrom,label,usable from def
                where type_elem=4) dd3 on dd3.version=dd2.version and dd2.rinok=dd3.rinok and dd2.entity=dd3.entity and dd2.parentrole=dd3.parentrole and dd3.arcfrom=dd2.label
                group by dd.version,dd.rinok,dd.entity,dd.parentrole
        ) dd
        )dd
		left join
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
where targetnamespace = 'http://www.cbr.ru/xbrl/bfo/rep/2023-03-31/ep/ep_npf_ao_y_39' 
) ee group by entity
		) df on df.entity=dd.entity
        group by version,rinok,dd.entity,parentrole,split_part(dims,'#',1),dim_def
        
		
		
"""
df=do_sql(sql_bfo)
save_to_excel(df,sql_bfo,'41')


