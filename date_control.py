import numpy
import numpy as np
import psycopg2, warnings, gc
import pandas as pd

warnings.filterwarnings("ignore")

class date_control():
    def __init__(self,xsd,name):
        self.result_list = []
        self.query_resul = []
        self.parentrole_table = f"""
        (select roleuri from roletypes where entity='{xsd}')
        """
        self.parentrole_razdel = f"""
        (select uri_razdel from tableparts where entity='{xsd}')
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
        when concept is not null and tagselector is null and period_type='duration' then period_start end period_start,

        case 
        when concept is not null and tagselector is not null and period_type='duration' then rs.per_end
        when concept is not null and tagselector is not null and period_type='instant' then rs.per_end
        when concept is not null and tagselector is null and period_type='duration' then period_end 
        when concept is not null and tagselector is null and period_type='instant' then period_end end period_end,
        a2.arcfrom father,case when a.arcto is null then 0 else 1 end is_child
        from
        (
        select r.version,r.rinok,r.entity,r.parentrole,r.id,r.label,re.dimension||'#'||re.member dimension,
        rc.value concept,tagselector,coalesce(rp.period_type,e.periodtype) period_type,
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
        self.sql_dop = f"""
        select distinct tp.version,tp.entity,tp.rinok,targetnamespace entrypoint
        from tableparts tp 
        join tables t on t.version=tp.version and t.namespace=tp.uri_table
        where tp.entity='{xsd}'
        """
        self.sql_def = f"""
        with def as
        (
        select l.version,l.rinok,l.entity,l.parentrole,e.qname,l.label,arcfrom,arcto,arcrole,e.type,a.usable
        from locators l
        join elements e on e.id=href_id and e.version=l.version
        join arcs a on a.arcto=l.label and l.version=a.version and l.rinok=a.rinok and l.entity=a.entity and a.parentrole=l.parentrole
        where l.parentrole in {self.parentrole_razdel}
        and a.arctype='definition'
        order by parentrole
        ),
        cc as
        (
        select version,rinok,entity,parentrole,qname concept from def
        where arcrole='http://xbrl.org/int/dim/arcrole/domain-member' and (type not in ('nonnum:domainItemType') or type is null)
        )
        select version,rinok,entity,parentrole,parentrole_text,concept,dimensions||case when dimensions_group is not null then ';' else '' end||coalesce(dimensions_group,'') dimensions
        from
        (
        select cc.version,cc.rinok,cc.entity,cc.parentrole,concept,
        string_agg(dd.qname||case when dd.qname||'#'||coalesce(dd3.qname,dd2.qname) is not null then '#' else '' end||coalesce(coalesce(dd3.qname,dd2.qname),''),';') dimensions,
        string_agg(distinct case when coalesce(dd2.usable,'true') ='true' and dd3.qname is not null then dd.qname||'#'||dd2.qname end,';') dimensions_group,
        rt.definition parentrole_text
        from cc
        left join roletypes rt on rt.roleuri=cc.parentrole
        left join def dd on dd.version=cc.version and dd.entity=cc.entity and dd.rinok=cc.rinok and dd.parentrole=cc.parentrole and dd.arcrole='http://xbrl.org/int/dim/arcrole/hypercube-dimension'
        left join def dd2 on dd.version=dd2.version and dd2.rinok=dd.rinok and dd2.entity=dd.entity and dd2.parentrole=dd.parentrole and dd2.arcfrom=dd.label
        and dd2.arcrole='http://xbrl.org/int/dim/arcrole/dimension-domain'
        left join def dd3 on dd3.version=dd2.version and dd2.rinok=dd3.rinok and dd2.entity=dd3.entity and dd2.parentrole=dd3.parentrole and dd3.arcfrom=dd2.label
        and dd3.arcrole='http://xbrl.org/int/dim/arcrole/domain-member'
        group by cc.version,cc.rinok,cc.entity,cc.parentrole,concept,rt.definition
        ) zz
        """
        self.name=name

    def do_sql(self):
        connect = psycopg2.connect(user="postgres",
                                   password="124kosm21",
                                   host="127.0.0.1",
                                   port="5432",
                                   database="taxonomy_db")



        re = pd.read_sql_query(self.sql1, connect)
        tt = pd.read_sql_query(self.sql_tt, connect)
        pp = pd.read_sql_query(self.sql_pp, connect)
        dd = pd.read_sql_query(self.sql_def, connect)
        dop = pd.read_sql_query(self.sql_dop, connect)

        re_e = re[re['concept'].isnull() == True]



        for indx, row in re_e.iterrows():

            while row['label'] not in [xx['root_rulenodes'] for i, xx in tt.iterrows() if
                                       xx['parentrole'] == row['parentrole']]:
                try:
                    father = re[(re['parentrole'] == row['parentrole']) & (re['label'] == row['father'])]['label'].values[0]
                    grandfather = re[(re['parentrole'] == row['parentrole']) & (re['label'] == row['father'])]['father'].values[0]
                except:
                    re_e.drop(index=indx)
                    # print(row['parentrole'], row['concept'], row['label'],row['entity'],row['father'])
                    break
                if father:
                    row['label'] = father
                    row['father'] = grandfather
        re_e = re_e[re_e['dimension'].isnull() == False]
        # for i,row in re_e.iterrows():
        #     print(row['label'],row['dimension'])


        re_c = re[re['concept'].isnull() == False]
        re_c.loc[:, "label_up"] = re_c['label']


        for indx, row in re_c.iterrows():
            while row['label_up'] not in [xx['root_rulenodes'] for i, xx in tt.iterrows()]:
                father = re[(re['parentrole'] == row['parentrole']) & (re['label'] == row['father'])]['label'].values[0]
                grandfather = re[(re['parentrole'] == row['parentrole']) & (re['label'] == row['father'])]['father'].values[0]
                dimensions = re[(re['parentrole'] == row['parentrole']) & (re['label'] == row['father'])]['dimension'].values[0]
                period_start =re[(re['parentrole'] == row['parentrole']) & (re['label'] == row['father'])]['period_start'].values[0]
                period_end = re[(re['parentrole'] == row['parentrole']) & (re['label'] == row['father'])]['period_end'].values[0]
                period_type = re[(re['parentrole'] == row['parentrole']) & (re['label'] == row['father'])]['period_type'].values[0]
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

        for p, row in re_c.iterrows():
            try: child_period_start = re[(re['parentrole'] == row['parentrole']) & (re['father'] == row['label'])]['period_start'].values[0]
            except: child_period_start = None
            try: child_period_end = re[(re['parentrole'] == row['parentrole']) & (re['father'] == row['label'])]['period_end'].values[0]
            except: child_period_end = None
            try: dimensions =  re[(re['parentrole'] == row['parentrole']) & (re['father'] == row['label'])]['dimension'].values[0]
            except: dimensions = None
            dim = ";".join([dimensions, row['dimension']]) if dimensions and row[
                'dimension'] else dimensions if dimensions else row['dimension'] if row['dimension'] else None

            if child_period_start: re_c['period_start'][p] = child_period_start
            if child_period_end: re_c['period_end'][p] = child_period_end
            re_c['dimension'][p] = dim

        for p, row in re_c.iterrows():
            try: start = pp[(pp['parentrole'] == row['parentrole']) & (pp['period_type'] == row['period_type'])]['start'].values[0]
            except: start = None
            try: end = pp[(pp['parentrole'] == row['parentrole']) & (pp['period_type'] == row['period_type'])]['end'].values[0]
            except:  end = None
            if not row['period_start']: re_c['period_start'][p] = start
            if not row['period_end']: re_c['period_end'][p] = end
#################
        df_period= pd.DataFrame(columns=['parentrole','concept','period_start','period_end'])
        for i,row in re_c.iterrows():
            # print(row['parentrole'],row['concept'],row['period_start'],row['period_end'])
            df_period.loc[-1] = [row['parentrole'], row['concept'], row['period_start'], row['period_end']]
            df_period.index = df_period.index + 1
            df_period = df_period.sort_index()
        df_period=df_period.drop_duplicates()
        df_period.sort_values(by=['parentrole','concept'])
####################

        re_c_agg=pd.DataFrame({'dimension_agg' : re_c[re_c['dimension'].isnull() == False].groupby(['parentrole','concept'])['dimension'].aggregate(lambda x: list(x))}).reset_index()
        re_c_agg_per=pd.DataFrame({'start_agg' : re_c.groupby(['parentrole','concept'])['period_start'].aggregate(lambda x: list(x)),
                                   'end_agg' : re_c.groupby(['parentrole','concept'])['period_end'].aggregate(lambda x: list(x))}).reset_index()

        # for i,row in re_c_agg_per.iterrows():
        #     print('agg  =  ',row['parentrole'],row['concept'],row['start_agg'],row['end_agg'])

        for i, row in re_c.iterrows():
            ser_concept=re_c_agg[(re_c_agg['concept']==row['concept']) & (re_c_agg['parentrole']==row['parentrole'])]['dimension_agg']
            if ser_concept.values:
                ser_concept=[xx for xx in ser_concept if xx]
                if ser_concept:
                    re_c['dimension'][i]=';'.join(ser_concept[0])
                else:
                    None
            ser_start= re_c_agg_per[(re_c_agg_per['concept'] == row['concept']) & (re_c_agg_per['parentrole'] == row['parentrole'])]['start_agg']
            ser_end = re_c_agg_per[(re_c_agg_per['concept'] == row['concept']) & (re_c_agg_per['parentrole'] == row['parentrole'])]['end_agg']
            if ser_start.values:
                ser_start=[xx for xx in ser_start if xx]
                if ser_start:
                    re_c['period_start'][i]=ser_start[0]
                else:
                    None
            if ser_end.values:
                ser_end=[xx for xx in ser_end if xx]
                if ser_end:
                    re_c['period_end'][i]=ser_end[0]
                else:
                    None

        # for i,row in re_c.iterrows():
        #     print(row['parentrole'],row['concept'],row['period_start'],row['period_end'])

        final_df = pd.DataFrame(
            columns=['to_sort','parentrole', 'concept', 'dimension', 'period_start', 'period_end', 'new_dimension', 'uri_razdel'])

        for i, row in re_c.iterrows():
            if row['dimension']: dim_concept = [row['dimension']]
            else: dim_concept = []
            # print('re_e: ',row['concept'],row['parentrole'],' DIM = ',row['dimension'])
            try: dim_re = ";".join(re_e[re_e['parentrole'] == row['parentrole']]['dimension'])
            except: dim_re = None
            if dim_re: dim_re = [dim_re]
            else: dim_re = []
            # try: dim_an = [an[an['parentrole'] == row['parentrole']]['an_dim'].values[0]]
            # except: dim_an = None
            # if dim_an: dim_an = dim_an
            # else: dim_an = []

            dim_all = dim_concept + dim_re
            dim_all.sort()
            dim_final = ";".join(dim_all)
            # print(row['concept'],len(dim_final.split(';')))
            final_df.loc[-1] = [len(dim_final.split(';')),row['parentrole'], row['concept'], dim_final, row['period_start'], row['period_end'],
                                None, None]
            final_df.index = final_df.index + 1
            final_df = final_df.sort_index()

        final_df = final_df.sort_values(by=['parentrole','concept','to_sort'],ascending=False)

        # for i,row in final_df.iterrows():
        #     print(row['concept'],row['to_sort'],row['parentrole'],row['period_start'],row['period_end'])

        final_df_dd = pd.DataFrame(columns=['concept', 'dimension', 'period_start', 'period_end', 'new_dimension', 'uri_razdel','parentrole_text'])

        for i, row in dd.iterrows():
            if row['dimensions']:
                dim1 = row['dimensions'].split(';')
                dim1.sort()
                dim1_clear = list(set([xx.split('#')[0] for xx in dim1]))
                dim1_clear.sort()
            else:
                dim1 = []
                dim1_clear = []
            ser=final_df[(final_df['concept']==row['concept'])]
            check = False
            for j,row2 in ser.iterrows():
                if row2['dimension']:
                    dim2 = row2['dimension'].split(';')
                    dim2.sort()
                    dim2_clear = list(set([xx.split('#')[0] for xx in dim2]))
                    dim2_clear.sort()
                else:
                    dim2 = []
                    dim2_clear = []
                # print('TABLE',row2['concept'],row2['parentrole'],row2['dimension'])
                # print('table_osi',dim2_clear)
                # print('defin_osi',dim1_clear)
                if numpy.isin(dim1, dim2).all() and row2['parentrole'] in row['parentrole']:
                    # print(row['concept'])
                    # print('with member')
                    # print(row['parentrole'])
                    # print(row2['parentrole'])
                    # print(row2['period_start'],row2['period_end'])
                    # print('---------------------')
                    for xx in range(len(row2['period_start'])):
                        # print(row2['parentrole'], row2['concept'], ' DIM = ', row2['dimension'])
                        final_df_dd.loc[-1] = [row2['concept'], row2['dimension'],
                                               row2['period_start'][xx] if row2['period_start'][xx] else '$par:refPeriodEnd', row2['period_end'][xx], row['dimensions'],
                                               row['parentrole'], row['parentrole_text']]
                        final_df_dd.index = final_df_dd.index + 1
                        final_df_dd = final_df_dd.sort_index()
                    check = True
                    break
                elif numpy.isin(dim1, dim2).all()==False and numpy.isin(dim1_clear, dim2_clear).all() and row2['parentrole'] in row['parentrole']:
                    # print(row['concept'])
                    # print('withOUT member')
                    # print(row['parentrole'])
                    # print(row2['parentrole'])
                    # print(row2['period_start'],row2['period_end'])
                    # print('---------------------')
                    for xx in range(len(row2['period_start'])):
                        # print(row2['parentrole'], row2['concept'], ' DIM = ', row2['dimension'])
                        final_df_dd.loc[-1] = [row2['concept'], row2['dimension'],
                                               row2['period_start'][xx] if row2['period_start'][xx] else '$par:refPeriodEnd', row2['period_end'][xx], row['dimensions'],
                                               row['parentrole'], row['parentrole_text']]
                        final_df_dd.index = final_df_dd.index + 1
                        final_df_dd = final_df_dd.sort_index()
                    check = True
                    break

                elif row2['parentrole'] in row['parentrole']:
                    print(row['concept'])
                    print('error')
                    print(row['parentrole'])
                    print(row2['parentrole'])
                    print('row',dim1_clear)
                    print('row2',dim2_clear)
                    print('---------------------')

            if check==False:
                print(row['concept'])
                print(row['dimensions'])
                print('--ERROR--')
                print(row['parentrole'])
                print('---------------------')
                final_df_dd.loc[-1] = [row['concept'], row['dimensions'],
                                       'ERROR',
                                       'ERROR', row['dimensions'],
                                       row['parentrole'], row['parentrole_text']]
                final_df_dd.index = final_df_dd.index + 1
                final_df_dd = final_df_dd.sort_index()

        # for i,row in final_df_dd.iterrows():
        #     print(row['concept'],row['uri_razdel'],row['period_start'],row['period_end'])

        #final_df_dd=final_df_dd.drop_duplicates()

        # final_df_dd=final_df_dd.sort_values(by=['parentrole', 'concept'], ignore_index=True)
        # for i,row in final_df_dd.iterrows():
        #     print(row['concept'],row['uri_razdel'],' DIM = ',row['new_dimension'])

        df_to_excel=pd.DataFrame(columns=['entrypoint','concept','hypercube','ogrn','period_start','period_end','parentrole','parentrole_text'])
        for i,xx in dop.iterrows():
            for j,yy in final_df_dd.iterrows():
                dim=yy['new_dimension']
                df_to_excel.loc[-1] = [xx['entrypoint'],yy['concept'],dim,None,yy['period_start'],yy['period_end'],yy['uri_razdel'],yy['parentrole_text']]
                df_to_excel.index = df_to_excel.index + 1
                df_to_excel = df_to_excel.sort_index()
            break
        df_to_excel=df_to_excel.drop_duplicates()
        df_to_excel=df_to_excel.sort_values(by=['entrypoint', 'parentrole','concept'],ignore_index=True)
        df_to_excel.to_excel(f"{self.name}_output.xlsx",index=False)
        # df_period.to_excel("date_control_output.xlsx", index=False)


if __name__ == "__main__":
    forms=['sr_0420504.xsd']
#     forms=['sr_0420502.xsd',
# 'sr_0420514.xsd',
# 'sr_0420506.xsd',
# 'sr_0420526.xsd',
# 'sr_0420507.xsd',
# 'sr_0420503.xsd',
# 'sr_0420513.xsd',
# 'sr_0420501.xsd',
# 'sr_0420508.xsd',
# 'sr_0420509.xsd',
# 'sr_0420512.xsd',
# 'sr_sved_otch_org.xsd',
# 'EPS_chasti.xsd',
# 'sr_soprovod.xsd']
    for xx in forms:
        ss = date_control(xx,xx.split('.')[0])
        ss.do_sql()
