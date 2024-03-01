import asyncio, asyncpg
import datetime, pandas as pd

def timer(func):
    async def _wrapper(*args, **kwargs):
        start = datetime.datetime.now()
        await func(*args, **kwargs)
        stop = datetime.datetime.now()
        time_delta = stop - start
        print(f'{args[2]}|{args[3]}|{time_delta}')
    return _wrapper

class AsyncPGDatabase:
    def __init__(self, dsn,name_result):
        self.dsn = dsn
        self.result_list = []
        self.query_resul = []
        self.name_result = name_result

    @timer
    async def execute_select(self, query, id, text):
        conn = await asyncpg.connect(self.dsn)
        data = await conn.fetch(query)
        result = pd.DataFrame(data)
        #self.result_list.append(result)
        if result.empty==False:
            self.query_resul.append([result,id,text])
            self.result_list.append({'ID': id,
                                     'TEXT': text,
                                     'RESULT': f'=HYPERLINK("[{self.name_result}]{id}!A1", "FAIL")',
                                     'RESULT_TEMP': "FAIL"
                                    })
        else:
            self.result_list.append({'ID':id,'TEXT':text,'RESULT':'OK'})
        await conn.close()


async def main(data:list, name_result):
    # Укажите ваши параметры подключения к базе данных PostgreSQL
    dsn = "postgresql://postgres:124kosm21@192.168.174.104/final_6_0"

    asyncpg_db = AsyncPGDatabase(dsn, name_result)

    tasks = []

    for query in data:
        tasks.append(asyncpg_db.execute_select(query[0],query[1],query[2]))

    await asyncio.gather(*tasks)

    # Печать результатов
    save_to_excel(asyncpg_db.result_list, asyncpg_db.query_resul, name_result)


def openCheckList(path,version): #для одного потока
        xlsx = pd.ExcelFile(path)
        df = xlsx.parse(xlsx.sheet_names[0])
        xlsx.close()
        list_data =[]
        for index, row in df.iterrows():
            sql=row['SQL'].replace('HID', f"'{version}'")
            id=row['ID']
            text=row['TEXT']
            list_data.append([sql,id,text])
        return list_data

def save_to_excel(result_list,query_result,name_result):
        res_pd = pd.DataFrame(result_list)
        res_pd['ID'] = [float(xx) for xx in res_pd['ID']]
        res_pd = res_pd.sort_values(by=['RESULT_TEMP','ID']).reset_index()
        res_pd = res_pd [['ID','TEXT','RESULT']]
        res_pd_temp=res_pd.copy()
        res_pd = res_pd.style.applymap(lambda x: "background-color: yellow" if 'FAIL' in x else None, subset=['RESULT'])
        with pd.ExcelWriter(name_result,engine='xlsxwriter') as writer:
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

if __name__ == '__main__':
    path = 'checkList.xlsx'
    version = 'final_6_0_test'
    d_time = str(datetime.datetime.now()).replace(':', '_')
    name_result = f"{version}_checkList_result({d_time}).xlsx"
    list_data = openCheckList(path, version)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(list_data,name_result))