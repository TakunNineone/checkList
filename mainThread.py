import psycopg2,warnings,gc
import pandas as pd
warnings.filterwarnings("ignore")
from multiprocessing.pool import ThreadPool

class checkList():
    def __init__(self):
        self.result_list=[]
        self.query_resul=[]
        self.version='final_6_4'
        self.path='checkList.xlsx'
        self.count=0
    def connect_to_bd(self):
        conn = psycopg2.connect(user="postgres",
                                 password="124kosm21",
                                 host="127.0.0.1",
                                 port="5432",
                                 database="final_6_4")
        return conn



    def do_sql(self,sql,id,text):
        connect = psycopg2.connect(user="postgres",
                                password="124kosm21",
                                host="127.0.0.1",
                                port="5432",
                                database="final_6_4")
        # print(id,'зупущено',text)
        dat = pd.read_sql_query(sql, connect)
        if dat.empty==False:
            self.query_resul.append([dat,id])
            self.result_list.append({'ID': id, 'TEXT': text, 'RESULT': 'FAIL'})
        else:
            self.result_list.append({'ID':id,'TEXT':text,'RESULT':'OK'})
        connect.close()
        del dat
        self.count += 1
        print(id,'завершено',self.count, text)

        print(self.count)
        gc.collect()


    def save_to_excel(self,result_list,query_result):
        res_pd=pd.DataFrame(result_list)
        with pd.ExcelWriter("final_6_4_checkList_result.xlsx") as writer:
            res_pd.to_excel(writer,index=False,sheet_name='result')
            for xx in query_result:
                xx[0].to_excel(writer,index=False,sheet_name=str(xx[1]))

    def openCheckList(self,row):
        sql=row[0].replace('HID', f"'{self.version}'")
        id=row[1]
        text=row[2]
        self.do_sql(sql,id,text)

    def startThread(self):
        xlsx = pd.ExcelFile(self.path)
        df = xlsx.parse(xlsx.sheet_names[0])
        xlsx.close()
        temp_rows=[]
        for index,row in df.iterrows():
            temp_rows.append([row['SQL'],row['ID'],row['TEXT']])
        with ThreadPool(processes=10) as pool:
            pool.map(self.openCheckList, temp_rows)
        print(111)

if __name__ == "__main__":
    ss=checkList()
    ss.startThread()
    ss.save_to_excel(ss.result_list,ss.query_resul)
