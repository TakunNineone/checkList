import numpy,openpyxl
import numpy as np
import psycopg2, warnings, gc, os
import pandas as pd

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

    def read_data(self):
        data = pd.read_sql_query(self.data, self.connect)
        return data

    def do_sql(self, xsd, roles_def, rinok, ep, iskl):
        self.parentrole_table = f"""
                (select roleuri from roletypes where entity in {xsd} and rinok='{rinok}' 
               -- and roleuri='http://www.cbr.ru/xbrl/nso/ins/rep/2023-03-31/tab/sr_0420154/sr_0420154_R2_P21_2'
                )
                """
        self.parentrole_razdel = f"""
                (select roleuri from rolerefs where entity in {roles_def} and rinok='{rinok}' 
              --  and roleuri='http://www.cbr.ru/xbrl/nso/ins/rep/2023-03-31/tab/sr_0420154/sr_0420154_R2_P21_2/1'
                )
                """
        self.sql1 = f"""
                select version,rinok,entity,parentrole,id,label,string_agg(dimension,';') dimension,concept,period_type,
                tag,period_start,period_end,father,is_child
                from
                (
                select distinct r.version,r.rinok,r.entity,r.parentrole,r.id,r.label,dimension,concept,
                case when concept is not null then period_type end period_type,tag,
                case 
                when concept is not null and tagselector is not null and period_type='duration' then rs.per_start 
                when concept is not null and tagselector is not null and period_type='instant' then rs.per_start 
                when concept is not null and tagselector is null and period_type='instant' then period_start
                when concept is not null and tagselector is null and period_type='duration' then period_start else period_start end period_start,

                case 
                when concept is not null and tagselector is not null and period_type='duration' then rs.per_end
                when concept is not null and tagselector is not null and period_type='instant' then rs.per_end
                when concept is not null and tagselector is null and period_type='duration' then period_end 
                when concept is not null and tagselector is null and period_type='instant' then period_end 
        		else period_end end period_end,
                a2.arcfrom father,case when a.arcto is null then 0 else 1 end is_child
                from
                (
                select r.version,r.rinok,r.entity,r.parentrole,r.id,r.label,re.dimension||'#'||re.member dimension,
                case when coalesce(e.abstract,'false')='false' then rc.value else null end concept,tagselector,coalesce(rp.period_type,e.periodtype) period_type,
                rp.start period_start,rp.end period_end
                from rulenodes r
                left join rulenodes_e re on re.version=r.version and re.rinok=r.rinok and re.entity=r.entity and re.parentrole=r.parentrole
                and re.rulenode_id=r.id
                left join rulenodes_c rc on rc.version=r.version and rc.rinok=r.rinok and rc.entity=r.entity and rc.parentrole=r.parentrole
                and rc.rulenode_id=r.id
                left join rulenodes_p rp on rp.version=r.version and rp.rinok=r.rinok and rp.entity=r.entity and rp.parentrole=r.parentrole
                and rp.rulenode_id=r.id
                left join elements e on e.qname=rc.value and e.version=rc.version
                where r.parentrole in {self.parentrole_table} 
                --and r.parentrole in ('http://www.cbr.ru/xbrl/nso/uk/rep/2023-03-31/tab/sr_0420503_R3')
                order by concept
                ) r
                left join (
                select distinct version,rinok,entity,parentrole,tag,p_type,
                coalesce(per_start,per_instant) per_start,
                per_end
                from
                (
                select rs.*,case when per_start is not null then 'duration' else 'instant' end p_type 
                from rulesets rs
                	where parentrole in {self.parentrole_table} 
                	--and parentrole in ('http://www.cbr.ru/xbrl/nso/uk/rep/2023-03-31/tab/sr_0420503_R3')
                ) rs
                ) rs on rs.version=r.version and rs.rinok=r.rinok and rs.entity=r.entity and rs.parentrole=r.parentrole and rs.tag=r.tagselector and rs.p_type=r.period_type
                left join arcs a on a.version=r.version and a.rinok=r.rinok and a.entity=r.entity and a.parentrole=r.parentrole and a.arcfrom=r.label  
                left join arcs a2 on a2.version=r.version and a2.rinok=r.rinok and a2.entity=r.entity and a2.parentrole=r.parentrole and a2.arcto=r.label  

                union all

                select distinct an.version,an.rinok,an.entity,an.parentrole,an.id,an.label,coalesce(re.dimension||'#'||rm.member,an.dimension) dimension,null concept,null period_type,null tag,null period_start,null period_end,
                a2.arcfrom father,case when a.arcto is null then 0 else 1 end is_child
                from aspectnodes an
                left join arcs a on a.version=an.version and a.rinok=an.rinok and a.entity=an.entity and a.parentrole=an.parentrole and a.arcfrom=an.label 
                left join rend_edimensions re on a.version=re.version and a.rinok=re.rinok and a.entity=re.entity and a.parentrole=re.parentrole and re.label=a.arcto
	            left join rend_edmembers rm on rm.version=re.version and rm.rinok=re.rinok and rm.entity=re.entity and rm.parentrole=re.parentrole and rm.dimension_id=re.id 
                left join arcs a2 on a2.version=an.version and a2.rinok=an.rinok and a2.entity=an.entity and a2.parentrole=an.parentrole and a2.arcto=an.label  
                where an.parentrole in {self.parentrole_table} 
                --and an.parentrole in ('http://www.cbr.ru/xbrl/nso/uk/rep/2023-03-31/tab/sr_0420503_R3')
                ) r group by version,rinok,entity,parentrole,id,label,concept,period_type,
                tag,period_start,period_end,father,is_child
                """
        self.sql_tt = f"""
                select t.version,t.rinok,t.entity,t.parentrole,table_label,br_label,t.axis,a.arcto root_rulenodes
                from
                (
                select t.version,t.rinok,t.entity,t.parentrole,a.arcfrom table_label,a.arcto br_label,axis
                from tableschemas t
                join arcs a on a.version=t.version and a.rinok=t.rinok and a.entity=t.entity and a.parentrole=t.parentrole and a.arcto=t.label
                and a.arcrole='http://xbrl.org/arcrole/2014/table-breakdown'
                where rolefrom='breakdown'
                and t.parentrole in {self.parentrole_table}
                ) t 
                join arcs a on a.version=t.version and a.rinok=t.rinok and a.entity=t.entity and a.parentrole=t.parentrole and a.arcfrom=t.br_label
                order by t.version,t.rinok,t.entity,t.parentrole,a.arcto
                """
        self.sql_pp = f"""
                	select distinct rn.version,rn.rinok,rn.entity,rn.parentrole,period_type,rp.start,rp.end 
                	from rulenodes rn
                	join rulenodes_p rp on rp.version=rn.version and rp.rinok=rn.rinok and rp.entity=rn.entity and rp.parentrole=rn.parentrole and rp.rulenode_id=rn.id
                	where rn.parentrole in {self.parentrole_table} 
                """
        null__ = '{NULL}'
        self.sql_def = f"""
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
        join elements e on e.id=href_id and e.version=l.version and e.rinok!='{iskl}'
        join arcs a on a.arcto=l.label and l.version=a.version and l.rinok=a.rinok and l.entity=a.entity and a.parentrole=l.parentrole
        and a.arctype='definition' 
        where l.parentrole in {self.parentrole_razdel}
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

  		select distinct dd.version,dd.rinok,dd.entity,parentrole,rt.definition parentrole_text,concept,dims dimension
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
        order by version,rinok,parentrole,concept
        """
        self.sql_df = f"""
        select replace(entity,'.xsd','-definition.xml') entity,string_agg(distinct dim_def,';') dim_def
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
        where targetnamespace = '{ep}' 
        ) ee 
        group by entity
        """
        with open('data.txt', 'w') as f:
            f.write(self.data)
        with open('table.txt', 'w') as f:
            f.write(self.sql1)
        with open('def.txt', 'w') as f:
            f.write(self.sql_def)
        with open('per.txt', 'w') as f:
            f.write(self.sql_pp)

        re = pd.read_sql_query(self.sql1, self.connect)
        print('выгрузка по тэйблу')
        tt = pd.read_sql_query(self.sql_tt, self.connect)
        print('выгрузка по брейкдаунам')
        pp = pd.read_sql_query(self.sql_pp, self.connect)
        print('выгрузка по периодам')
        dd = pd.read_sql_query(self.sql_def, self.connect)
        print('выгрузка по дефинишину')
        re_e = re[re['concept'].isnull() == True]
        df = pd.read_sql_query(self.sql_df, self.connect)
        print('выгрузка дефолтных')

        for indx, row in re_e.iterrows():

            while row['label'] not in [xx['root_rulenodes'] for i, xx in tt.iterrows() if
                                       xx['parentrole'] == row['parentrole']]:
                try:
                    father = \
                        re[(re['parentrole'] == row['parentrole']) & (re['label'] == row['father'])]['label'].values[0]
                    grandfather = \
                        re[(re['parentrole'] == row['parentrole']) & (re['label'] == row['father'])]['father'].values[0]
                except:
                    re_e.drop(index=indx)
                    # print(row['parentrole'], row['concept'], row['label'],row['entity'],row['father'])
                    break
                if father:
                    row['label'] = father
                    row['father'] = grandfather
        re_e = re_e[re_e['dimension'].isnull() == False]

        re_c = re[re['concept'].isnull() == False]
        re_c.loc[:, "label_up"] = re_c['label']

        for indx, row in re_c.iterrows():
            while row['label_up'] not in [xx['root_rulenodes'] for i, xx in tt.iterrows()]:
                father = re[(re['parentrole'] == row['parentrole']) & (re['label'] == row['father'])]['label'].values[0]
                grandfather = \
                    re[(re['parentrole'] == row['parentrole']) & (re['label'] == row['father'])]['father'].values[0]
                dimensions = \
                    re[(re['parentrole'] == row['parentrole']) & (re['label'] == row['father'])]['dimension'].values[0]
                period_start = \
                    re[(re['parentrole'] == row['parentrole']) & (re['label'] == row['father'])]['period_start'].values[
                        0]
                period_end = \
                    re[(re['parentrole'] == row['parentrole']) & (re['label'] == row['father'])]['period_end'].values[0]
                period_type = \
                    re[(re['parentrole'] == row['parentrole']) & (re['label'] == row['father'])]['period_type'].values[
                        0]
                if father:
                    dim = ";".join([dimensions, row['dimension']]) if dimensions and row[
                        'dimension'] else dimensions if dimensions else row['dimension'] if row['dimension'] else None
                    row['label_up'] = father
                    row['father'] = grandfather
                    row['period_type'] = row['period_type'] if row[
                        'period_type'] else period_type if period_type else None
                    if row['period_start'] == None:
                        re_c['period_start'][indx] = period_start
                    if row['period_end'] == None:
                        re_c['period_end'][indx] = period_end
                    row['dimension'] = dim

        # for i,row in re_e.iterrows():
        #     print(row['parentrole'],row['label'],row['dimension'])
        # print('|||||||||||||||||||||||||||||||||||||')

        for p, row in re_c.iterrows():
            try:
                child_period_start = \
                    re[(re['parentrole'] == row['parentrole']) & (re['father'] == row['label'])]['period_start'].values[
                        0]
            except:
                child_period_start = None
            try:
                child_period_end = \
                    re[(re['parentrole'] == row['parentrole']) & (re['father'] == row['label'])]['period_end'].values[0]
            except:
                child_period_end = None
            try:
                dimensions = \
                    re[(re['parentrole'] == row['parentrole']) & (re['father'] == row['label'])]['dimension'].values[0]
            except:
                dimensions = None
            dim = ";".join([dimensions, row['dimension']]) if dimensions and row[
                'dimension'] else dimensions if dimensions else row['dimension'] if row['dimension'] else None

            if child_period_start: re_c['period_start'][p] = child_period_start
            if child_period_end: re_c['period_end'][p] = child_period_end
            re_c['dimension'][p] = dim

        line_add = []
        for p, row in re_c.iterrows():
            try:
                start = pp[(pp['parentrole'] == row['parentrole']) & (pp['period_type'] == row['period_type'])  & (pp['rinok'] == row['rinok'])][
                    'start'].values
                print(start)
            except:
                start = None
            # print(row['concept'],row['label'],start)
            try:
                end = pp[(pp['parentrole'] == row['parentrole']) & (pp['period_type'] == row['period_type'])  & (pp['rinok'] == row['rinok'])][
                    'end'].values
            except:
                end = None
            print(row['parentrole'],row['concept'],row['period_type'],row['period_start'],start,end)
            if row['period_start']==None:
                re_c['period_start'][p] = start[0]
                re_c['period_end'][p] = end[0]

                for xx in range(1, len(start)):
                    line_add.append(
                        [row['version'], row['rinok'], row['entity'], row['parentrole'], row['id'], row['label'],
                         row['dimension'],
                         row['concept'], row['period_type'], row['tag'], start[xx], end[xx], row['father'],
                         row['is_child'], row['label_up']])
        line_add_df = pd.DataFrame(data=line_add,
                                   columns=['version', 'rinok', 'entity', 'parentrole', 'id', 'label', 'dimension',
                                            'concept', 'period_type', 'tag', 'period_start', 'period_end', 'father',
                                            'is_child', 'label_up'])
        re_c = pd.concat([re_c, line_add_df]).reset_index(drop=True)
        del line_add, line_add_df

        #################
        columns_df_period = ['parentrole', 'concept', 'period_start', 'period_end']
        line_df_period = []
        for i, row in re_c.iterrows():
            line_df_period.append([row['parentrole'], row['concept'], row['period_start'], row['period_end']])
        df_period = pd.DataFrame(data=line_df_period, columns=columns_df_period)
        df_period = df_period.drop_duplicates()
        df_period.sort_values(by=['parentrole', 'concept'])
        ####################

        # for xx,row in re_c.iterrows():
        #     print(row['parentrole'],row['label'],'----',row['father'],'----',row['concept'],row['period_start'],row['period_end'],'-----',row['dimension'])
        # print('########################')

        re_c_agg = pd.DataFrame({'dimension_agg':
                                     re_c[re_c['dimension'].isnull() == False].groupby(['parentrole', 'concept'])[
                                         'dimension'].aggregate(lambda x: list(x))}).reset_index()
        re_c_agg_per = pd.DataFrame(
            {'start_agg': re_c.groupby(['parentrole', 'concept'])['period_start'].aggregate(lambda x: list(x)),
             'end_agg': re_c.groupby(['parentrole', 'concept'])['period_end'].aggregate(
                 lambda x: list(x))}).reset_index()

        for i, row in re_c.iterrows():
            ser_concept = \
                re_c_agg[(re_c_agg['concept'] == row['concept']) & (re_c_agg['parentrole'] == row['parentrole'])][
                    'dimension_agg']
            if ser_concept.values:
                ser_concept = [xx for xx in ser_concept if xx]
                if ser_concept:
                    re_c['dimension'][i] = ';'.join(ser_concept[0])
                else:
                    None
            ser_start = re_c_agg_per[
                (re_c_agg_per['concept'] == row['concept']) & (re_c_agg_per['parentrole'] == row['parentrole'])][
                'start_agg']
            ser_end = re_c_agg_per[
                (re_c_agg_per['concept'] == row['concept']) & (re_c_agg_per['parentrole'] == row['parentrole'])][
                'end_agg']
            if ser_start.values:
                ser_start = [xx for xx in ser_start if xx]
                if ser_start:
                    re_c['period_start'][i] = ser_start[0]
                else:
                    None
            if ser_end.values:
                ser_end = [xx for xx in ser_end if xx]
                if ser_end:
                    re_c['period_end'][i] = ser_end[0]
                else:
                    None

        columns_final_df = ['to_sort', 'parentrole', 'concept', 'dimension', 'period_start', 'period_end',
                            'new_dimension',
                            'uri_razdel', 'parentrole_agg']
        line_final_df = []
        for i, row in re_c.iterrows():
            if row['dimension']:
                dim_concept = [row['dimension']]
            else:
                dim_concept = []
            # print('re_e: ',row['concept'],row['parentrole'],' DIM = ',row['dimension'])
            try:
                dim_re = ";".join(re_e[re_e['parentrole'] == row['parentrole']]['dimension'])
            except:
                dim_re = None
            if dim_re:
                dim_re = [dim_re]
            else:
                dim_re = []
            dim_all = dim_concept + dim_re
            dim_all.sort()
            dim_final = ";".join(dim_all)
            # print(row['concept'],len(dim_final.split(';')))
            line_final_df.append([len(dim_final.split(';')), row['parentrole'], row['concept'], dim_final,
                                  row['period_start'], row['period_end'],
                                  None, None, None])
        final_df = pd.DataFrame(data=line_final_df, columns=columns_final_df)

        # for xx,row in final_df.iterrows():
        #     print(row['concept'],row['parentrole'],len(row['dimension'].split(';')))
        #     print('-------------')
        # print('########################')

        final_df = final_df.sort_values(by=['parentrole', 'concept', 'to_sort'], ascending=False)

        # for xx,row in final_df.iterrows():
        #     print(['concept'],row['dimension'])

        roles_bfo_dict = {}
        with open('roles_bfo.txt', 'r') as f:
            roles_bfo = f.readlines()
        roles_bfo = [xx.strip() for xx in roles_bfo]
        for xx in roles_bfo:
            roles_bfo_dict[xx.split('|')[0]] = xx.split('|')[1]

        # for i, row in final_df.iterrows():
        #     print('final_df - ', row['parentrole'],row['concept'],': ',row['period_start'],'--',row['period_end'],row['uri_razdel'],row['dimension'])
        for xx, se in final_df.iterrows():
            if se['parentrole'] == 'http://www.cbr.ru/xbrl/bfo/rep/2023-03-31/tab/FR_4_008_02a_01_39_2':
                final_df['parentrole_agg'][xx] = 'http://www.cbr.ru/xbrl/bfo/rep/2023-03-31/tab/FR_4_008_02a_01_39'
            elif se['parentrole'] == 'http://www.cbr.ru/xbrl/bfo/rep/2023-03-31/tab/FR_4_008_03a_01_39_2':
                final_df['parentrole_agg'][xx] = 'http://www.cbr.ru/xbrl/bfo/rep/2023-03-31/tab/FR_4_008_03a_01_39'
            elif se['parentrole'] == 'http://www.cbr.ru/xbrl/bfo/rep/2023-03-31/tab/FR_4_003_05a_01_39/1':
                final_df['parentrole_agg'][xx] = 'http://www.cbr.ru/xbrl/bfo/rep/2023-03-31/tab/FR_4_003_05a_01_39'
            elif se['parentrole'] == 'http://www.cbr.ru/xbrl/bfo/rep/2023-03-31/tab/FR_4_003_05a_01_39/2':
                final_df['parentrole_agg'][xx] = 'http://www.cbr.ru/xbrl/bfo/rep/2023-03-31/tab/FR_4_003_05a_01_39'
            elif se['parentrole'] == 'http://www.cbr.ru/xbrl/bfo/rep/2023-03-31/tab/FR_3_006_01a_01_39_LastQuarter':
                final_df['parentrole_agg'][xx] = 'http://www.cbr.ru/xbrl/bfo/rep/2023-03-31/tab/FR_3_006_01a_01_39'
            # elif se['parentrole'] == 'http://www.cbr.ru/xbrl/bfo/rep/2023-03-31/tab/1_FR_BS_NPF_AO_39_retrospective':
            #     final_df['parentrole_agg'][xx] = 'http://www.cbr.ru/xbrl/bfo/rep/2023-03-31/tab/1_FR_BS_NPF_AO_39'
            else:
                final_df['parentrole_agg'][xx] = se['parentrole']

        # for i, row in final_df.iterrows():
        #     print('final_df - ', row['parentrole'],row['concept'],': ',row['parentrole_agg'],len(row['dimension'].split(';')),row['period_start'],row['period_end'])

        dim_agg = pd.DataFrame({'dimension_agg': final_df.groupby(['parentrole_agg', 'concept'])['dimension'].aggregate(
            lambda x: list(x))}).reset_index()

        for i, row in dim_agg.iterrows():
            dim_temp = []
            for tt in row['dimension_agg']:
                dim_temp = dim_temp + tt.split(';')
            dim_temp = list(set(dim_temp))
            dim_agg['dimension_agg'][i] = dim_temp

        for i, row in final_df.iterrows():
            dim_temp = \
                dim_agg[(dim_agg['parentrole_agg'] == row['parentrole_agg']) & (dim_agg['concept'] == row['concept'])][
                    'dimension_agg'].values[0]
            final_df['dimension'][i] = dim_temp


        columns_final_df_dd = ['concept', 'dimension', 'period_start', 'period_end', 'new_dimension', 'uri_razdel','uri_table',
                               'parentrole_text', 'entity']
        check2 = False

        res=self.compare_dataframes(dd,final_df,roles_bfo_dict)
        final_df_dd = pd.DataFrame(data=res, columns=columns_final_df_dd)



        columns_to_excel = ['entrypoint', 'concept', 'hypercube', 'ogrn', 'period_start', 'period_end', 'parentrole','parentrole_table',
                            'parentrole_text']
        line_temp = []
        len_df = len(final_df_dd)
        l = 0
        print("удаляю default")
        for j, yy in final_df_dd.iterrows():
            l+=1
            dim = yy['new_dimension']
            if dim:
                df_am = df[df['entity'] == yy['entity']]['dim_def']
                if not df_am.empty:
                    df_am = df_am.values[0].split(';')
                else:
                    df_am = []
                    dim = []
            to_remove = []

            if dim and df_am:
                for xx in self.generator_(df_am, dim):
                    to_remove.append(xx)
                dim = [xx for ii, xx in enumerate(dim) if xx not in to_remove]

            line_temp.append(
                [ep, yy['concept'], ';'.join(dim) if dim else None, None, yy['period_start'], yy['period_end'],
                 yy['uri_razdel'],yy['uri_table'], yy['parentrole_text']])
            print(f"\r{l}..{len_df}", end="", flush=True)
        print("удалил default")


        df_to_excel = pd.DataFrame(data=line_temp, columns=columns_to_excel)
        df_to_excel = df_to_excel.drop_duplicates()
        df_to_excel = df_to_excel.sort_values(by=['entrypoint', 'parentrole', 'concept'], ignore_index=True)
        prefix = '!!!error!!!_' if check2 == True else ''
        print('сохраняю exel')
        self.save_large_dataframe_to_excel(df_to_excel,f"{os.getcwd()}/datecontrol/{prefix}{ep.split('/')[-1]}.xlsx")
        #df_to_excel.to_excel(f"{os.getcwd()}/datecontrol/{prefix}{ep.split('/')[-1]}11.xlsx", index=False)
        print('завершено')

    def compare_dataframes(self, dataframe1, dataframe2,roles_bfo_dict):
        print('склеиваю table и definition')
        results = []
        len_df=len(dataframe1)
        i=0
        for row1 in self.generator_dd_df(dataframe1):
            i+=1
            check = False
            if row1['parentrole'] in roles_bfo_dict.keys():
                parentrole1 = roles_bfo_dict.get(row1['parentrole'])
            else:
                parentrole1 = row1['parentrole']
            dimension1 = set(row1['dimension']) if row1['dimension'] else {}
            dimension1_clear = set(list(set([xx.split('#')[0] for xx in row1['dimension']]))) if row1['dimension'] else {}
            ser=dataframe2[(dataframe2['concept']==row1['concept'])]
            for row2 in self.generator_dd_df(ser):
                dimension2 = set(row2['dimension']) if row2['dimension'] else {}
                dimension2_clear = set(list(set([xx.split('#')[0] for xx in row2['dimension']])))  if row2['dimension'] else {}
                if dimension2.issuperset(dimension1) and row2['parentrole_agg'] in parentrole1:
                    check=True
                    for xx in range(len(row2['period_start'])):
                        results.append([row2['concept'], row2['dimension'],
                                        row2['period_start'][xx] if row2['period_start'][
                                            xx] else '$par:refPeriodEnd', row2['period_end'][xx],
                                        row1['dimension'],
                                        row1['parentrole'],row2['parentrole'], row1['parentrole_text'], row1['entity']])
                    break
                elif dimension2.issuperset(dimension1)==False and dimension2_clear.issuperset(dimension1_clear) and row2['parentrole_agg'] in parentrole1:
                    # print(parentrole1,row2['parentrole_agg'])
                    check = True
                    for xx in range(len(row2['period_start'])):
                        results.append([row2['concept'], row2['dimension'],
                                        row2['period_start'][xx] if row2['period_start'][
                                            xx] else '$par:refPeriodEnd', row2['period_end'][xx],
                                        row1['dimension'],
                                        row1['parentrole'],row2['parentrole'], row1['parentrole_text'], row1['entity']])
                    break
                else:
                    None
            if check==False:
                results.append([row1['concept'], row1['dimension'],
                                   'ERROR',
                                   'ERROR', row1['dimension'],
                                   row1['parentrole'],None, row1['parentrole_text'], row1['entity']])
            print(f"\r{i}..{len_df}", end="", flush=True)
        print('\n')


        return results

    def generator_dd_df(self,df:pd.DataFrame):
        for index,row in df.iterrows():
            yield row

    def generator_(self, iterable, dim):
        iterator = iter(iterable)
        for xx in iterator:
            if xx in dim:
                yield xx

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
    ep = 'http://www.cbr.ru/xbrl/nso/uk/rep/2023-03-31/ep/ep_nso_uk_q_y_10rd'
    ss = date_control(ep)
    xsds = ["'" + row['data'].split(';')[1] + "'" for xx, row in ss.read_data().iterrows()]
    xsds_str = '(' + ",".join(xsds) + ')'
    roles_def = xsds_str.replace('.xsd', '-definition.xml')
    ss.do_sql(xsds_str, roles_def, 'uk', ep,'')