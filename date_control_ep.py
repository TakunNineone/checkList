import numpy,openpyxl
import numpy as np
import psycopg2, warnings, gc, os
import pandas as pd
from multiprocessing.pool import ThreadPool

warnings.filterwarnings("ignore")


class date_control():
    def __init__(self, ep):
        self.result_list = []
        self.query_resul = []
        self.connect = psycopg2.connect(user="postgres",
                                        password="124kosm21",
                                        host="127.0.0.1",
                                        port="5432",
                                        database="final_5_2")
        self.data = f"""
        select distinct targetnamespace||';'||tp.entity||';'||tp.rinok data
        from tableparts tp 
        join tables t on t.version=tp.version and t.namespace=tp.uri_table and t.rinok=tp.rinok
        where targetnamespace = '{ep}' and lower(tp.entity) not like '%eps_chasti%'
        """
        self.data1=f"""
select distinct targetnamespace||';'||tp.entity||';'||tp.rinok data
        from tableparts tp 
        join tables t on t.version=tp.version and t.namespace=tp.uri_table and t.rinok=tp.rinok
        where targetnamespace = 'http://www.cbr.ru/xbrl/nso/npf/rep/2024-11-01/ep/ep_nso_npf_q_30d_reestr_0420258' and lower(tp.entity) not like '%eps_chasti%'
		 and tp.entity='sr_0420258.xsd'
        """

    def read_data(self):
        data = pd.read_sql_query(self.data, self.connect)
        return data


    def do_sql(self, xsd, roles_def, rinok, ep):
        self.parentrole_table = f"""
                (select roleuri from roletypes where entity in {xsd} and rinok='{rinok}')
                """
        self.parentrole_razdel = f"""
                (select roleuri from rolerefs where entity in {roles_def} and rinok='{rinok}' 
              --  and roleuri='http://www.cbr.ru/xbrl/nso/ins/rep/2023-03-31/tab/sr_0420154/sr_0420154_R2_P21_2/1'
                )
                """
        null_arr='{}'
        self.sql_table = f"""
 

WITH RECURSIVE recursive_hierarchy AS not materialized 
(
select version,rinok,entity,parentrole,id parent_id,label parent_label,id child_id,label child_label,tagselector,dimension,arcfrom,arcto,cell_type,0 level,p.label rulenodes
from
(	
select distinct r.version,r.rinok,r.entity,r.parentrole,r.id,r.label,r.tagselector,null dimension,a.arcfrom,a.arcto,'rulenode' cell_type
from rulenodes r
left join arcs a on a.version=r.version and a.rinok=r.rinok and a.entity=r.entity and a.parentrole=r.parentrole and a.arcto=r.label
left join arcs aa on aa.version=r.version and aa.rinok=r.rinok and aa.entity=r.entity and aa.parentrole=r.parentrole and aa.arcfrom=r.label
where a.arcrole='http://xbrl.org/arcrole/2014/breakdown-tree'
and r.parentrole in {self.parentrole_table} and r.rinok='{rinok}'

union all

select distinct r.version,r.rinok,r.entity,r.parentrole,r.id,r.label,null tagselector,dimension,a.arcfrom,a.arcto,'aspectnode' cell_type from aspectnodes r
left join arcs a on a.version=r.version and a.rinok=r.rinok and a.entity=r.entity and a.parentrole=r.parentrole and a.arcto=r.label
left join arcs aa on aa.version=r.version and aa.rinok=r.rinok and aa.entity=r.entity and aa.parentrole=r.parentrole and aa.arcfrom=r.label
where a.arcrole='http://xbrl.org/arcrole/2014/breakdown-tree'
and r.parentrole in {self.parentrole_table} and r.rinok='{rinok}'
order by 1,2,3,4
) p
	union all

select c.version,c.rinok,c.entity,c.parentrole,parent_id,parent_label,c.id child_id,c.label child_label,p.tagselector,c.dimension,
	c.arcfrom,c.arcto,c.cell_type,level+1,rulenodes||';'||c.label
from
(
select distinct r.version,r.rinok,r.entity,r.parentrole,r.id,r.label,r.tagselector,null dimension,a.arcfrom,a.arcto,'rulenode' cell_type
from rulenodes r
left join arcs a on a.version=r.version and a.rinok=r.rinok and a.entity=r.entity and a.parentrole=r.parentrole and a.arcto=r.label
left join arcs aa on aa.version=r.version and aa.rinok=r.rinok and aa.entity=r.entity and aa.parentrole=r.parentrole and aa.arcfrom=r.label
where a.arcrole!='http://xbrl.org/arcrole/2014/breakdown-tree'
and r.parentrole in {self.parentrole_table} and r.rinok='{rinok}'

union all

select distinct r.version,r.rinok,r.entity,r.parentrole,r.id,r.label,null tagselector,dimension,a.arcfrom,a.arcto,'aspectnode' cell_type 
	from aspectnodes r
left join arcs a on a.version=r.version and a.rinok=r.rinok and a.entity=r.entity and a.parentrole=r.parentrole and a.arcto=r.label
left join arcs aa on aa.version=r.version and aa.rinok=r.rinok and aa.entity=r.entity and aa.parentrole=r.parentrole and aa.arcfrom=r.label
where a.arcrole!='http://xbrl.org/arcrole/2014/breakdown-tree'
and r.parentrole in {self.parentrole_table} and r.rinok='{rinok}'
order by 1,2,3,4

) c 
inner join recursive_hierarchy p ON c.arcfrom = p.arcto and c.version=p.version and c.rinok=p.rinok and c.entity=p.entity and c.parentrole=p.parentrole
where c.arcfrom is not null
),
pp as
(
select distinct r.version,r.rinok,r.entity,r.parentrole,rp.start,rp.end,rp.tag,period_type
from recursive_hierarchy r
join rulenodes_p rp on rp.version=r.version and rp.rinok=r.rinok and rp.entity=r.entity and rp.parentrole=r.parentrole and (rp.tag=tagselector or rp.rulenode_id=r.child_id)
),
cc as
(
select distinct r.version,r.rinok,r.entity,r.parentrole,r.parent_label,rc.value concept,re.dimension||'#'||re.member dimension,
rp.start,rp.end,r.level,rc.parent_tag,r.rulenodes,e.periodtype period_type,tagselector
from recursive_hierarchy r 
join rulenodes_c rc on rc.version=r.version and rc.rinok=r.rinok and rc.entity=r.entity and rc.parentrole=r.parentrole and (rc.tag=tagselector or rc.rulenode_id=r.child_id)
join elements e  on e.qname=rc.value and e.version=rc.version and coalesce(abstract,'false')='false'
left join rulenodes_e re on re.version=r.version and re.rinok=r.rinok and re.entity=r.entity and re.parentrole=r.parentrole and (re.tag=tagselector or re.rulenode_id=r.child_id)
left join rulenodes_p rp on rp.version=r.version and rp.rinok=r.rinok and rp.entity=r.entity and rp.parentrole=r.parentrole and (rp.tag=tagselector or rp.rulenode_id=r.child_id) and rp.period_type=e.periodtype
),
ee as
(
select r.version,r.rinok,r.entity,r.parentrole,r.parent_label,coalesce(re.dimension||'#'||re.member,r.dimension) dimension,rulenodes,
case when cc.parent_label is null then 0 else 1 end is_parent
from recursive_hierarchy r
left join (select distinct version,rinok,entity,parentrole,parent_label from cc) cc using(version,rinok,entity,parentrole,parent_label)
left join rulenodes_e re on re.version=r.version and re.rinok=r.rinok and re.entity=r.entity and re.parentrole=r.parentrole and (re.tag=tagselector or re.rulenode_id=r.child_id)
where coalesce(re.dimension||'#'||re.member,r.dimension) is not null
)


select version,rinok,entity,parentrole,concept,period_start,period_end,
array_unique(array_agg(distinct dim_c)||array_agg(distinct dim_e)||array_agg(distinct dim_a)) dim
from
(
select cc.version,cc.rinok,cc.entity,cc.parentrole,concept,cc.parent_label,
string_to_array(cc.rulenodes,';') cc_rulenodes,
string_to_array(ee.rulenodes,';') ee_rulenodes,
compare_arrays2(string_to_array(cc.rulenodes,';'),string_to_array(ee.rulenodes,';')) compare,
cc.dimension dim_c,ee.dimension dim_e,ee2.dimension dim_a,
coalesce(cc.start,pp.start) period_start,
coalesce(cc.end,pp.end) period_end
from cc
left join pp on pp.version=cc.version and pp.rinok=cc.rinok and pp.entity=cc.entity and pp.parentrole=cc.parentrole and pp.period_type=cc.period_type and cc.start is null
left join ee on ee.version=cc.version and ee.rinok=cc.rinok and ee.entity=cc.entity and ee.parentrole=cc.parentrole  and ee.parent_label=cc.parent_label and ee.parent_label=cc.parent_label and is_parent=1
left join ee ee2 on ee2.version=cc.version and ee2.rinok=cc.rinok and ee2.entity=cc.entity and ee2.parentrole=cc.parentrole  and ee2.is_parent=0
where ((parent_tag='table:ruleset' and tagselector is not null) or parent_tag!='table:ruleset')	
and compare_arrays2(string_to_array(cc.rulenodes,';'),string_to_array(ee.rulenodes,';'))=1
) cc
group by version,rinok,entity,parentrole,concept,period_start,period_end
order by cc.version,cc.rinok,cc.entity,cc.parentrole,concept
                """
        self.sql_definition = f"""
        with 
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
def as not materialized
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
        and a.arctype='definition'
		and l.parentrole in {self.parentrole_razdel} and l.rinok='{rinok}'
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
  select zz.version,zz.rinok,zz.entity,zz.parentrole,rt.definition parentrole_text,concept,coalesce(array_unique(dimensions),'{null_arr}') dim
  from
  (
    select distinct dd.version,dd.rinok,dd.entity,parentrole,concept,remove_elements_from_array(dims,dim_def) dimensions,dims
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
  left join roletypes rt on rt.version=zz.version and rt.roleuri=zz.parentrole
   order by version,rinok,entity,parentrole
        """

        with open('data.txt', 'w') as f:
            f.write(self.data)
        with open('table.txt', 'w') as f:
            f.write(self.sql_table)
        with open('def.txt', 'w') as f:
            f.write(self.sql_definition)

        table = pd.read_sql_query(self.sql_table, self.connect)
        print('выгрузка по тэйблу')
        definition = pd.read_sql_query(self.sql_definition, self.connect)
        print('выгрузка по дефинишину')

        columns_to_excel = ['entrypoint', 'concept', 'hypercube', 'ogrn', 'period_start', 'period_end', 'parentrole','parentrole_table','parentrole_text']

        results = []
        l = 0
        for idx,dd in definition.iterrows():

            dim_def=np.array(dd['dim'])
            dim_def_clear=np.array([xx.split('#')[0] for xx in dim_def])
            check_error = True
            for idx2,tt in table.iterrows():
                dim_tab = np.array(tt['dim'])
                dim_tab_clear = np.array([xx.split('#')[0] for xx in dim_tab])
                if tt['parentrole'] in dd['parentrole'] and tt['concept']==dd['concept']:
                    if numpy.isin(dim_def, dim_tab).all() or dd['dim']==[]:
                        # print('ok1',tt['parentrole'], '---', dd['parentrole'])
                        check_error = False
                        results.append([ep, dd['concept'], ';'.join(dd['dim']) if dd['dim'] else None, None, tt['period_start'], tt['period_end'],dd['parentrole'],tt['parentrole'],dd['parentrole_text']])
                    elif numpy.isin(dim_def_clear, dim_tab_clear).all() or dd['dim']==[]:
                        # print('ok2',tt['parentrole'], '---', dd['parentrole'])
                        check_error = False
                        results.append([ep, dd['concept'], ';'.join(dd['dim']) if dd['dim'] else None, None, tt['period_start'], tt['period_end'], dd['parentrole'],tt['parentrole'],dd['parentrole_text']])
                else:
                    None

            if check_error==True:
                print(dd['parentrole'],'--- ERROR')
                results.append([ep, dd['concept'], ';'.join(dd['dim']) if dd['dim'] else None, None, 'ERROR', 'ERROR', dd['parentrole'],'ERROR',dd['parentrole_text']])

        df_to_excel = pd.DataFrame(data=results, columns=columns_to_excel)
        df_to_excel = df_to_excel.drop_duplicates()
        df_to_excel = df_to_excel.sort_values(by=['entrypoint', 'parentrole', 'concept'], ignore_index=True)
        print('сохраняю exel')
        self.save_large_dataframe_to_excel(df_to_excel, f"{os.getcwd()}/datecontrol/{ep.split('/')[-1]}.xlsx")
        # df_to_excel.to_excel(f"{os.getcwd()}/datecontrol/{prefix}{ep.split('/')[-1]}11.xlsx", index=False)
        print('завершено')

    def save_large_dataframe_to_excel(self,df, excel_file_path, max_rows_per_sheet=1048575):
        num_chunks = len(df) // max_rows_per_sheet + 1
        with pd.ExcelWriter(excel_file_path) as writer:
            for i in range(num_chunks):
                print(i,'сохраняю лист')
                start_row = i * max_rows_per_sheet
                end_row = (i + 1) * max_rows_per_sheet
                chunk_df = df[start_row:end_row]
                chunk_df.to_excel(writer, sheet_name=f'Sheet {i + 1}', index=False)
        print(f'Сохранено в {excel_file_path}')



if __name__ == "__main__":
    #ep = 'http://www.cbr.ru/xbrl/nso/uk/rep/2023-03-31/ep/support_ep_all_nso_uk'
    ep = 'http://www.cbr.ru/xbrl/nso/uk/rep/2023-03-31/ep/support_ep_all_nso_uk'
    ss = date_control(ep)
    xsds = ["'" + row['data'].split(';')[1] + "'" for xx, row in ss.read_data().iterrows()]
    xsds_str = '(' + ",".join(xsds) + ')'
    roles_def = xsds_str.replace('.xsd', '-definition.xml')
    ss.do_sql(xsds_str, roles_def, 'uk', ep)