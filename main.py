import psycopg2,warnings,gc
import pandas as pd
warnings.filterwarnings("ignore")
from multiprocessing.pool import ThreadPool
import datetime


def timer(func):
    def _wrapper(*args, **kwargs):
        start = datetime.datetime.now()
        func(*args, **kwargs)
        stop = datetime.datetime.now()
        time_delta = stop - start
        print(f'{args[2]}|{args[3]}|{time_delta}')
    return _wrapper


class checkList():
    def __init__(self,version):
        d_time = str(datetime.datetime.now()).replace(':','_')
        self.name_result = f"{version}_checkList_result({d_time}).xlsx"
        self.result_list=[]
        self.query_resul=[]

    def connect_to_bd(self):
        conn = psycopg2.connect(user="postgres",
                                 password="124kosm21",
                                 host="192.168.174.104",
                                 port="5432",
                                 database="final_6_3")
        return conn

    @timer
    def do_sql(self,sql,id,text):
        connect = psycopg2.connect(user="postgres",
                                password="124kosm21",
                                host="192.168.174.104",
                                port="5432",
                                database="final_6_3")

        dat = pd.read_sql_query(sql, connect)
        if dat.empty==False:
            self.query_resul.append([dat,id])
            self.result_list.append({'ID': id,
                                     'TEXT': text,
                                     'RESULT': f'=HYPERLINK("[{self.name_result}]{id}!A1", "FAIL")',
                                    })
        else:
            self.result_list.append({'ID':id,'TEXT':text,'RESULT':'OK'})
        connect.close()
        del dat
        gc.collect()

    def save_to_excel(self,result_list,query_result):
        res_pd = pd.DataFrame(result_list)
        res_pd = res_pd.astype({'ID': int}).sort_values(by=['RESULT','ID'])
        res_pd = res_pd.style.applymap(lambda x: "background-color: yellow" if 'FAIL' in x else None, subset=['RESULT'])
        with pd.ExcelWriter(self.name_result) as writer:
            res_pd.to_excel(writer,index=False,sheet_name='result')
            for xx in query_result:
                xx[0].to_excel(writer,index=False,sheet_name=str(xx[1]))

    def openCheckList(self,path,version): #для одного потока
        xlsx = pd.ExcelFile(path)
        df = xlsx.parse(xlsx.sheet_names[0])
        xlsx.close()
        for index, row in df.iterrows():
            sql=row['SQL'].replace('HID', f"'{version}'")
            id=row['ID']
            text=row['TEXT']
            self.do_sql(sql,id,text)
    def openCheckList_th(self,temp_rows): #многопоточный
        sql, id, text = temp_rows[0],temp_rows[1],temp_rows[2]
        self.do_sql(sql,id,text)

    def startThread(self,path, version, cnt_process):
        xlsx = pd.ExcelFile(path)
        df = xlsx.parse(xlsx.sheet_names[0])
        xlsx.close()
        temp_rows=[]
        for index,row in df.iterrows():
            temp_rows.append([row['SQL'].replace('HID', f"'{version}'"),
                              row['ID'],
                              row['TEXT']]
                             )
        with ThreadPool(processes=cnt_process) as pool:
            pool.map(self.openCheckList_th, temp_rows)


if __name__ == "__main__":
    path='checkList.xlsx'
    version='final_6_3'
    cnt_process = 5 #кол-во потоков
    ss=checkList(version)
    ss.startThread(path,version, cnt_process) #многопотоков
   # ss.openCheckList() #однопоточный
    ss.save_to_excel(ss.result_list,ss.query_resul)
