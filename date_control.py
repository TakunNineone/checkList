import numpy
import numpy as np
import psycopg2,warnings,gc
import pandas as pd
warnings.filterwarnings("ignore")
sql1="""
select r.version,r.rinok,r.entity,r.parentrole,r.id,r.label,dimension,concept,period_type,tag,
case when tag is null and period_type='instant' then per_instant 
	when tag is null and period_type='duration' then per_start else period_start end period_start,
case when tag is null and period_type='instant' then null 
	when tag is null and period_type='duration' then per_end else period_end end period_end,
	a.arcto child,a2.arcfrom father
from
(
select r.version,r.rinok,r.entity,r.parentrole,r.id,r.label,re.dimension||'#'||re.member dimension,
rc.value concept,rs.rulenode_id rs_rulenode_id,rs.tag,coalesce(rs.period_type,e.periodtype) period_type,
case when rs.per_start is null then coalesce(rs.per_instant,rs.per_end) else  rs.per_start end period_start,rs.per_end period_end
from rulenodes r
left join (select rs.*,case when per_start is not null then 'duration' else 'instant' end period_type from rulesets rs) rs on 
	rs.version=r.version and rs.rinok=r.rinok and rs.entity=r.entity and rs.parentrole=r.parentrole and rs.tag=r.tagselector
left join rulenodes_e re on re.version=r.version and re.rinok=r.rinok and re.entity=r.entity and re.parentrole=r.parentrole
and re.rulenode_id=r.id
left join rulenodes_c rc on rc.version=r.version and rc.rinok=r.rinok and rc.entity=r.entity and rc.parentrole=r.parentrole
and rc.rulenode_id=r.id
left join elements e on e.qname=rc.value and e.version=rc.version
where r.parentrole in ('http://www.cbr.ru/xbrl/nso/ins/rep/2023-03-31/tab/sr_0420154/sr_0420154_R1')
order by concept
) r
left join (select distinct version,rinok,entity,parentrole,
max(case when p_type='duration' then per_start end) per_start,
max(case when p_type='duration' then per_end end) per_end,
max(case when p_type='instant' then '$par:refPeriodEnd' end) per_instant
from
(
select rs.*,case when per_start is not null then 'duration' else 'instant' end p_type 
from rulesets rs
) rs
group by version,rinok,entity,parentrole) rs on rs.version=r.version and rs.rinok=r.rinok and rs.entity=r.entity and rs.parentrole=r.parentrole
left join arcs a on a.version=r.version and a.rinok=r.rinok and a.entity=r.entity and a.parentrole=r.parentrole and a.arcfrom=r.label  
left join arcs a2 on a2.version=r.version and a2.rinok=r.rinok and a2.entity=r.entity and a2.parentrole=r.parentrole and a2.arcto=r.label  
order by r.version,r.rinok,r.entity,r.parentrole,r.label
"""
sql_tt="""
select t.version,t.rinok,t.entity,t.parentrole,table_label,br_label,t.axis,a.arcto root_rulenodes
from
(
select t.version,t.rinok,t.entity,t.parentrole,a.arcfrom table_label,a.arcto br_label,axis
from tableschemas t
join arcs a on a.version=t.version and a.rinok=t.rinok and a.entity=t.entity and a.parentrole=t.parentrole and a.arcto=t.label
and a.arcrole='http://xbrl.org/arcrole/2014/table-breakdown'
where rolefrom='breakdown'
and t.parentrole like 'http://www.cbr.ru/xbrl/nso/ins/rep/2023-03-31/tab/sr_0420154/sr_0420154_R1'
) t 
join arcs a on a.version=t.version and a.rinok=t.rinok and a.entity=t.entity and a.parentrole=t.parentrole and a.arcfrom=t.br_label
order by t.version,t.rinok,t.entity,t.parentrole,a.arcto
"""
class date_control():
    def __init__(self):
        self.result_list=[]
        self.query_resul=[]

    def do_sql(self):
        connect = psycopg2.connect(user="postgres",
                                password="124kosm21",
                                host="127.0.0.1",
                                port="5432",
                                database="final_5_2")

        re = pd.read_sql_query(sql1, connect)
        tt = pd.read_sql_query(sql_tt,connect)

        re_e=re[re['concept'].isnull()==True]
        re_c=re[re['concept'].isnull()==False]

        for indx, row in re_c.iterrows():
            print(row['label'],row['concept'],row['dimension'])
            for i,row2 in re.iterrows():
                if row2['child']==row['label']:
                    print(row2['father'],row2['dimension'])

        # for indx,row in re_e.iterrows():
        #     while row['label'] not in [xx['root_rulenodes'] for i,xx in tt.iterrows()]:
        #         for i,row2 in re_e.iterrows() :
        #             if row['label']==row2['child']:
        #                 row['label']=row2['label']
        #
        # re_e=re_e[re_e['dimension'].isnull()==False]
        # re_e=re_e.sort_values(by=['label'],ignore_index=True)
        # for i, xx in re_e.iterrows():
        #     print(i,xx['label'],'dimension: ',xx['dimension'])


if __name__ == "__main__":
    ss=date_control()
    ss.do_sql()
