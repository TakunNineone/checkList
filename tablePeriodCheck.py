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
                                        database="final_6_2")


    def do_sql(self):
        self.parentrole_table = f"""
                (select distinct uri_table from tableparts where uri_table='http://www.cbr.ru/xbrl/bfo/rep/2024-11-01/tab/FR_2_055_01b_01')
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

        with open('table.txt', 'w') as f:
            f.write(self.sql1)

        re = pd.read_sql_query(self.sql1, self.connect)
        print('выгрузка по тэйблу')
        tt = pd.read_sql_query(self.sql_tt, self.connect)
        print('выгрузка по брейкдаунам')
        pp = pd.read_sql_query(self.sql_pp, self.connect)
        print('выгрузка по периодам')
        re_e = re[re['concept'].isnull() == True]



        re_c = re[re['concept'].isnull() == False]
        re_c.loc[:, "label_up"] = re_c['label']

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
            print(row['concept'],row['period_type'],row['period_start'],start,end)
            if row['period_start']==None and start != []:
                print(1111111111)
                re_c['period_start'] = start
                re_c['period_end'] = end

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


        for ii,row in re_c.iterrows():
            print(row['parentrole'],row['concept'],row['period_start'],row['period_end'])


        #################
        columns_df_period = ['parentrole', 'concept', 'period_start', 'period_end']
        line_df_period = []
        for i, row in re_c.iterrows():
            line_df_period.append([row['parentrole'], row['concept'], row['period_start'], row['period_end']])
        df_period = pd.DataFrame(data=line_df_period, columns=columns_df_period)
        df_period = df_period.drop_duplicates()
        df_period.sort_values(by=['parentrole', 'concept'])
        ####################
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
        for ii,row in df_period.iterrows():
            print(row['parentrole'],row['concept'],row['period_start'],row['period_end'])

        self.save_large_dataframe_to_excel(re_c,'periods.xlsx')

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
    ep = 'http://www.cbr.ru/xbrl/nso/ins/rep/2023-03-31/ep/ep_SSDNEMED_10rd_sr_m'
    ss = date_control(ep)
    ss.do_sql()