import numpy
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
                                        database="taxonomy_db")
        self.data = f"""
        select distinct targetnamespace||';'||tp.entity||';'||tp.rinok data
        from tableparts tp 
        join tables t on t.version=tp.version and t.namespace=tp.uri_table and t.rinok=tp.rinok
        where targetnamespace = '{ep}' --and tp.entity in ('FR_BS_NPF_AO_39.xsd')
        """

    def read_data(self):
        data = pd.read_sql_query(self.data, self.connect)
        return data

    def do_sql(self, xsd, roles_def, rinok, ep,iskl):
        self.parentrole_table = f"""
                (select roleuri from roletypes where entity in {xsd} and rinok='{rinok}')
                """
        self.parentrole_razdel = f"""
                (select roleuri from rolerefs where entity in {roles_def} and rinok='{rinok}' and roleuri is not null)
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

                select distinct an.version,an.rinok,an.entity,an.parentrole,id,label,dimension,null concept,null period_type,null tag,null period_start,null period_end,
                a2.arcfrom father,case when a.arcto is null then 0 else 1 end is_child
                from aspectnodes an
                left join arcs a on a.version=an.version and a.rinok=an.rinok and a.entity=an.entity and a.parentrole=an.parentrole and a.arcfrom=an.label  
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
        	when arcrole='http://xbrl.org/int/dim/arcrole/all' then 0 else -1 end type_elem
        from locators l
        join elements e on e.id=href_id and e.version=l.version and e.rinok!='{iskl}'
        join arcs a on a.arcto=l.label and l.version=a.version and l.rinok=a.rinok and l.entity=a.entity and a.parentrole=l.parentrole
        and a.arctype='definition' 
        where l.parentrole in {self.parentrole_razdel}  
        order by arcrole
        ),
        dd as
        (
        select version,rinok,entity,parentrole,string_to_array(unnest(cross_agregate(array_agg(dims))),'|') dims
        from
        (select version,rinok,dd.entity,parentrole,split_part(dims,'#',1) dim, 
		array_to_string(array_agg(dims),'|') dims
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
        group by version,rinok,dd.entity,parentrole,split_part(dims,'#',1)
		) dd 
        group by version,rinok,entity,parentrole
        )


        select distinct dd.version,dd.rinok,dd.entity,parentrole,rt.definition parentrole_text,concept,array_to_string(dims,';') dimensions
        from
        (
        select cc.version,cc.rinok,cc.entity,cc.parentrole,cc.qname concept,dims,array_sravn_dc2(dims,dims_minus) is_minus
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
                re[(re['parentrole'] == row['parentrole']) & (re['label'] == row['father'])]['period_start'].values[0]
                period_end = \
                re[(re['parentrole'] == row['parentrole']) & (re['label'] == row['father'])]['period_end'].values[0]
                period_type = \
                re[(re['parentrole'] == row['parentrole']) & (re['label'] == row['father'])]['period_type'].values[0]
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
                re[(re['parentrole'] == row['parentrole']) & (re['father'] == row['label'])]['period_start'].values[0]
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
                start = pp[(pp['parentrole'] == row['parentrole']) & (pp['period_type'] == row['period_type'])][
                    'start'].values
            except:
                start = None
            # print(row['concept'],row['label'],start)
            try:
                end = pp[(pp['parentrole'] == row['parentrole']) & (pp['period_type'] == row['period_type'])][
                    'end'].values
            except:
                end = None
            # print(row['concept'],row['period_type'],row['period_start'],start,end)
            if not row['period_start'] and start != []:
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
        df_period = pd.DataFrame(columns=['parentrole', 'concept', 'period_start', 'period_end'])
        for i, row in re_c.iterrows():
            # print(row['parentrole'],row['concept'],row['period_start'],row['period_end'])
            df_period.loc[-1] = [row['parentrole'], row['concept'], row['period_start'], row['period_end']]
            df_period.index = df_period.index + 1
            df_period = df_period.sort_index()
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

        final_df = pd.DataFrame(
            columns=['to_sort', 'parentrole', 'concept', 'dimension', 'period_start', 'period_end', 'new_dimension',
                     'uri_razdel', 'parentrole_agg'])

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
            final_df.loc[-1] = [len(dim_final.split(';')), row['parentrole'], row['concept'], dim_final,
                                row['period_start'], row['period_end'],
                                None, None, None]
            final_df.index = final_df.index + 1
            final_df = final_df.sort_index()

        # for xx,row in final_df.iterrows():
        #     print(row['concept'],row['parentrole'],len(row['dimension'].split(';')))
        #     print('-------------')
        # print('########################')

        final_df = final_df.sort_values(by=['parentrole', 'concept', 'to_sort'], ascending=False)

        # for xx,row in final_df.iterrows():
        #     print(['concept'],row['dimension'])

        final_df_dd = pd.DataFrame(
            columns=['concept', 'dimension', 'period_start', 'period_end', 'new_dimension', 'uri_razdel',
                     'parentrole_text', 'entity'])

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

        dim_agg = pd.DataFrame({'dimension_agg': final_df.groupby(['parentrole_agg', 'concept'])['dimension'].aggregate(lambda x: list(x))}).reset_index()

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
            # print('final_df - ', row['parentrole'],row['concept'],': ',row['parentrole_agg'],dim_temp)
            final_df['dimension'][i] = dim_temp

        # for i, row in dim_agg.iterrows():
        #     print('dim_agg - ', row['concept'],row['parentrole_agg'],len(row['dimension_agg']))

        check2 = False
        for i, row in dd.iterrows():
            if row['dimensions']:
                dim1 = row['dimensions'].split(';')
                dim1.sort()
                dim1_clear = list(set([xx.split('#')[0] for xx in dim1]))
                dim1_clear.sort()
            else:
                dim1 = []
                dim1_clear = []

            if row['parentrole'] in roles_bfo_dict.keys():
                dd_parentrole = roles_bfo_dict.get(row['parentrole'])
            else:
                dd_parentrole = row['parentrole']

            ser = final_df[(final_df['concept'] == row['concept'])]

            # print('проверяю -- ',row['concept'],row['parentrole'])
            check = False
            for j, row2 in ser.iterrows():
                if row2['dimension']:
                    dim2 = row2['dimension']
                    dim2.sort()
                    dim2_clear = list(set([xx.split('#')[0] for xx in dim2]))
                    dim2_clear.sort()
                else:
                    dim2 = []
                    dim2_clear = []

                if numpy.isin(dim1, dim2).all() and row2[
                    'parentrole_agg'] in dd_parentrole:  # and row2['parentrole'] in row['parentrole']
                    for xx in range(len(row2['period_start'])):
                        final_df_dd.loc[-1] = [row2['concept'], row2['dimension'],
                                               row2['period_start'][xx] if row2['period_start'][
                                                   xx] else '$par:refPeriodEnd', row2['period_end'][xx],
                                               row['dimensions'],
                                               row['parentrole'], row['parentrole_text'], row['entity']]
                        final_df_dd.index = final_df_dd.index + 1
                        final_df_dd = final_df_dd.sort_index()
                    check = True
                    # break
                # elif numpy.isin(dim1, dim2).all()==False and numpy.isin(dim1_clear, dim2_clear).all() and row2['parentrole'] in dd_parentrole: # and row2['parentrole'] in row['parentrole']
                #     for xx in range(len(row2['period_start'])):
                #         final_df_dd.loc[-1] = [row2['concept'], row2['dimension'],
                #                                row2['period_start'][xx] if row2['period_start'][xx] else '$par:refPeriodEnd', row2['period_end'][xx], row['dimensions'],
                #                                row['parentrole'], row['parentrole_text']]
                #         final_df_dd.index = final_df_dd.index + 1
                #         final_df_dd = final_df_dd.sort_index()
                #     check = True
                #     print(2,row['parentrole'], row2['parentrole'])
                #     print(2,'dim1_clear',dim1_clear)
                #     print(2,'dim2_clear',dim2_clear)
                #     break

                # elif row2['parentrole_agg'] in dd_parentrole:
                #     print(row['concept'])
                #     print('error')
                #     print(dd_parentrole)
                #     print(row2['parentrole_agg'])
                #     print('row',dim1)
                #     print('row2',dim2)
                # else:
                #     # print(dd_parentrole, '---', row2['parentrole'])
                #     None

            if check == False:
                check2 = True
                print(row['concept'])
                print(row['dimensions'])
                print('--ERROR--')
                print(dd_parentrole)
                print('---------------------')
                final_df_dd.loc[-1] = [row['concept'], row['dimensions'],
                                       'ERROR',
                                       'ERROR', row['dimensions'],
                                       row['parentrole'], row['parentrole_text'], row['entity']]
                final_df_dd.index = final_df_dd.index + 1
                final_df_dd = final_df_dd.sort_index()

        print('сохраняю exel')
        df_to_excel = pd.DataFrame(
            columns=['entrypoint', 'concept', 'hypercube', 'ogrn', 'period_start', 'period_end', 'parentrole',
                     'parentrole_text'])
        for j, yy in final_df_dd.iterrows():
            dim = yy['new_dimension']
            if dim:
                df_am = df[df['entity'] == yy['entity']]
                dimension = self.delete_defaults(dim.split(';'), df_am)
            else:
                dimension = None
            df_to_excel.loc[-1] = [ep, yy['concept'], dimension, None, yy['period_start'], yy['period_end'],
                                   yy['uri_razdel'], yy['parentrole_text']]
            df_to_excel.index = df_to_excel.index + 1
            df_to_excel = df_to_excel.sort_index()
        df_to_excel = df_to_excel.drop_duplicates()
        df_to_excel = df_to_excel.sort_values(by=['entrypoint', 'parentrole', 'concept'], ignore_index=True)
        prefix = '!!!error!!!_' if check2 == True else ''
        df_to_excel.to_excel(f"{os.getcwd()}/datecontrol/{prefix}{ep.split('/')[-1]}.xlsx", index=False)
        print('завершено')

    def delete_defaults(self, arr1: list, arr2: list):
        try:
            for xx in arr2['dim_def'].values[0].split(';'):
                if xx in arr1:
                    arr1.remove(xx)
            return ';'.join(arr1)
        except:
            return None


if __name__ == "__main__":
    ep = 'http://www.cbr.ru/xbrl/bfo/rep/2023-03-31/ep/ep_npf_ao_y_39'
    ss = date_control(ep)
    xsds = ["'" + row['data'].split(';')[1] + "'" for xx, row in ss.read_data().iterrows()]
    xsds_str = '(' + ",".join(xsds) + ')'
    roles_def = xsds_str.replace('.xsd', '-definition.xml')
    ss.do_sql(xsds_str, roles_def, 'bfo', ep,'eps')
