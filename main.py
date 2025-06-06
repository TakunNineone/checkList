import psycopg2,warnings,gc
import pandas as pd
warnings.filterwarnings("ignore")
from multiprocessing.pool import ThreadPool
from openpyxl.styles import Font, Color
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
        self.version = version
        self.result_list=[]
        self.query_resul=[]

    def connect_to_bd(self):
        conn = psycopg2.connect(user="postgres",
                                 password="124kosm21",
                                 host="127.0.0.1",
                                 port="5432",
                                 database=self.version)
        return conn

    @timer
    def do_sql(self,sql,id,text,comment):
        connect = psycopg2.connect(user="postgres",
                                password="124kosm21",
                                host="127.0.0.1",
                                port="5432",
                                database=self.version)

        dat = pd.read_sql_query(sql, connect)
        if dat.empty==False:
            self.query_resul.append([dat,id,text])
            self.result_list.append({'ID': id,
                                     'TEXT': text,
                                     'RESULT': f'=HYPERLINK("[{self.name_result}]{id}!A1", "FAIL")',
                                     'RESULT_TEMP': "FAIL",
                                     'Comment':comment
                                    })
        else:
            self.result_list.append({'ID':id,'TEXT':text,'RESULT':'OK','Comment':comment})
        connect.close()
        del dat
        gc.collect()

    def add_first_line(self,df,text):
        df_c = list(df.columns)
        firs_row = [None] * len(df_c)
        firs_row[0] = text
        first_row_df = pd.DataFrame(data=[firs_row], columns=df_c)
        df = pd.concat([first_row_df, df]).reset_index(drop=True)
        df.applymap(lambda x: "background-color: yellow")
        return df

    def save_to_excel(self,result_list,query_result):
        res_pd = pd.DataFrame(result_list)
        res_pd['ID'] = [float(xx) for xx in res_pd['ID']]
        res_pd = res_pd.sort_values(by=['RESULT_TEMP','ID']).reset_index()
        res_pd = res_pd [['ID','TEXT','RESULT','Comment']]
        res_pd_temp=res_pd.copy()
        res_pd = res_pd.style.applymap(lambda x: "background-color: yellow" if 'FAIL' in x else None, subset=['RESULT'])
        with pd.ExcelWriter(self.name_result,engine='xlsxwriter') as writer:
            res_pd.to_excel(writer,index=False,sheet_name='result')
            for xx in range(len(query_result)):
                link_=res_pd_temp.loc[res_pd_temp['ID'] == query_result[xx][1]].index[0]+2
                zz=query_result[xx][1]
                query_result[xx][0].insert(0, 'НАЗАД',f'=HYPERLINK("#result!B{link_}", "НАЗАД")')
                query_result[xx][0].to_excel(writer,sheet_name=str(query_result[xx][1]), startrow = 1, index=False, freeze_panes=(2, 1))
                worksheet = writer.sheets[str(query_result[xx][1])]
                text = query_result[xx][2]
                cell_format = writer.book.add_format()
                cell_format.set_bold()
                cell_format.set_font_size(13)
                cell_format.set_font_color('green')
                column_format=writer.book.add_format({'color':'orange'})
                worksheet.write(0, 0, text)
                worksheet.set_row(0, 30, cell_format)
                worksheet.set_column('A2:A2', None, column_format)
            writer._save()


    def openCheckList(self,path,version): #для одного потока
        xlsx = pd.ExcelFile(path)
        df = xlsx.parse(xlsx.sheet_names[0])
        xlsx.close()
        for index, row in df.iterrows():
            sql=row['SQL'].replace('HID', f"'{version}'")
            id=row['ID']
            text=row['TEXT']
            comment=row['Comment']
            self.do_sql(sql,id,text,comment)

    def openCheckList_th(self,temp_rows): #многопоточный
        sql, id, text,comment = temp_rows[0],temp_rows[1],temp_rows[2],temp_rows[3]
        # if 'va_' not in sql:

        if id > 0: #and id not in (92,88,87,70,56,40,34,30,29,53,54,46,84,86) - формулы
            self.do_sql(sql,id,text,comment)

    def startThread(self,path, version, cnt_process):
        xlsx = pd.ExcelFile(path)
        df = xlsx.parse(xlsx.sheet_names[0])
        xlsx.close()
        temp_rows=[]
        for index,row in df.iterrows():
            temp_rows.append([row['SQL'].replace('HID', f"'{version}'"),
                              row['ID'],
                              row['TEXT'],
                              row['Comment']]
                             )
        with ThreadPool(processes=cnt_process) as pool:
            pool.map(self.openCheckList_th, temp_rows)


if __name__ == "__main__":
    path='checkList.xlsx'
    version='final71_cbr'
    cnt_process = 5 #кол-во потоков
    ss=checkList(version)
    print('Запуск - ',datetime.datetime.now())
    ss.startThread(path,version, cnt_process) #многопотоков
    # ss.openCheckList() #однопоточный
    ss.save_to_excel(ss.result_list,ss.query_resul)
    print('Завершено - ', datetime.datetime.now())
