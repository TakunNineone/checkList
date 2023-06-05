import psycopg2,pandas as pd,gc,warnings

warnings.filterwarnings("ignore")
def do_sql(sql):
    connect = psycopg2.connect(user="postgres",
                               password="124kosm21",
                               host="127.0.0.1",
                               port="5432",
                               database="testdb")
    df = pd.read_sql_query(sql, connect)
    connect.close()
    gc.collect()
    return df

def save_to_excel(df,sql):
    with pd.ExcelWriter("result_sql.xlsx") as writer:
        df.to_excel(writer, index=False, sheet_name='result')
        df_sql=pd.DataFrame({'sql':[sql]})
        df_sql.to_excel(writer,index=False,sheet_name='SQL')

sql="""with at as
(
select distinct r.version,r.rinok,r.entity,r.parentrole,r.label as rulenode,l.label,l.text,rc.value,
	r.entity||'#'||r.label,rt.definition uri_text
from rulenodes r
left join roletypes rt on rt.version=r.version and rt.rinok=r.rinok and rt.roleuri=r.parentrole
left join rulenodes_c rc on rc.version=r.version and rc.rinok=r.rinok and rc.entity=r.entity and rc.parentrole=r.parentrole and rc.rulenode_id=r.id
left join
(
select l.href,l.version,l.rinok,lb.label,lb.lang,lb.text
from locators l
join arcs a on a.version=l.version and a.rinok=l.rinok and a.entity=l.entity and a.arcfrom=l.label
join labels lb on lb.version=a.version and lb.rinok=a.rinok and lb.entity=a.entity and lb.label=a.arcto
where l.locfrom='lab' and lb.role='http://www.xbrl.org/2008/role/label'
) l on l.version=r.version and l.rinok=r.rinok and l.href=r.entity||'#'||r.label
where rc.value is not null 
),
ap as
(
select distinct a.version,a.rinok,a.entity,a.parentrole,l.href_id,pl.text,pl.qname,pl.role
from arcs a
join locators l on l.label=a.arcto and l.version=a.version and l.rinok=a.rinok and l.entity=a.entity and a.parentrole=l.parentrole 
join elements_labels pl on pl.version=l.version and pl.id=l.href_id and pl.role=a.preferredlabel 
where arctype ='presentation'
and pl.role not in ('http://www.xbrl.org/2003/role/periodEndLabel','http://www.xbrl.org/2003/role/periodStartLabel')
-- 	and a.entity like 'sr_0420162%'
order by href_id
),
ad as 
(
select distinct a.version,a.rinok,a.entity,a.parentrole,l.href_id,el.text,el.qname,el.abstract
from arcs a
join locators l on l.label=a.arcto and l.version=a.version and l.rinok=a.rinok and l.entity=a.entity and a.parentrole=l.parentrole 
join elements_labels el on el.version=l.version and el.id=l.href_id and el.role='http://www.xbrl.org/2003/role/label' and el.lang='ru'
where arctype ='definition'
-- 		and a.entity like 'sr_0420162%'
order by href_id
	
)

select distinct at.version "Версия",at.entity "Файл",at.rinok "Рынок",at.parentrole "URI в table",
case when ad.parentrole is not null then 'да'else 'нет' end "Найдена роль в definition",
uri_text "Раздел",at.rulenode "ID ruleNode",at.value "Показатель в ruleNode",
ap.qname "Показатель в presentation",ad.qname "Показатель в definition", 
at.text "Лайбл рулнода",ap.text "Лейбл presentation",ad.text "Лейбл в definition"
from at
-- left join ap on ap.qname=value and ap.parentrole=at.parentrole and ap.rinok=at.rinok and ap.version=at.version
-- left join ad on ad.qname=value and ad.parentrole=at.parentrole and ad.rinok=at.rinok and ad.version=at.version 
left join ap on ap.qname=value and (ap.parentrole similar to at.parentrole||'\D%' or ap.parentrole=at.parentrole)
left join ad on ad.qname=value and (ad.parentrole similar to at.parentrole||'\D%' or ad.parentrole=at.parentrole)  
--where at.text!=ap.text and ap.text is not null
where (at.entity like 'sr_0420154%' or at.entity like 'sr_0420162%')
order by 1,2,3,4


"""

df=do_sql(sql)
save_to_excel(df,sql)
