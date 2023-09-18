import psycopg2,warnings,gc
import pandas as pd
warnings.filterwarnings("ignore")

class checkList():
    def __init__(self):
        self.result_list=[]
        self.query_resul=[]

    def connect_to_bd(self):
        conn = psycopg2.connect(user="postgres",
                                 password="124kosm21",
                                 host="127.0.0.1",
                                 port="5432",
                                 database="final_5_30")
        return conn



    def do_sql(self,sql,id,text):
        connect = psycopg2.connect(user="postgres",
                                password="124kosm21",
                                host="127.0.0.1",
                                port="5432",
                                database="final_5_30")
        print(id,text)
        dat = pd.read_sql_query(sql, connect)
        if dat.empty==False:
            self.query_resul.append([dat,id])
            self.result_list.append({'ID': id, 'TEXT': text, 'RESULT': 'FAIL'})
        else:
            self.result_list.append({'ID':id,'TEXT':text,'RESULT':'OK'})
        connect.close()
        del dat
        gc.collect()


    def save_to_excel(self,result_list,query_result):
        res_pd=pd.DataFrame(result_list)
        with pd.ExcelWriter("final_5_30_checkList_result.xlsx") as writer:
            res_pd.to_excel(writer,index=False,sheet_name='result')
            for xx in query_result:
                xx[0].to_excel(writer,index=False,sheet_name=str(xx[1]))

    def openCheckList(self,path,version):
        xlsx = pd.ExcelFile(path)
        df = xlsx.parse(xlsx.sheet_names[0])
        xlsx.close()
        for index, row in df.iterrows():
            sql=row['SQL'].replace('HID', f"'{version}'")
            id=row['ID']
            text=row['TEXT']
            self.do_sql(sql,id,text)

if __name__ == "__main__":
    path='checkList.xlsx'
    version='final_5_30'
    ss=checkList()
    ss.openCheckList(path,version)
    ss.save_to_excel(ss.result_list,ss.query_resul)
