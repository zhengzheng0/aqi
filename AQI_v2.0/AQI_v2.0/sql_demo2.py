import psycopg2
import datetime
import pandas as pd


def read_from_ems_capture(table_name,data_addr_list,cur_time):

# table_name是查询的表名，字符串
# data_addr_list是查询的地址，即根据item_addr的值筛选，列表，表里每一项都是字符串
# cur_time是所需数据的开始时间，即根据时间create_time筛选，字符串


    #数据库连接
    db = psycopg2.connect(host='123.249.70.226', port=7004, user='postgres', password='postgres',
                                database='ems_capture')#数据库连接
    cursor = db.cursor()
    #批量查询
    if len(data_addr_list)>0:
        
        print('地址存在')
        #批量添加%s
        tmp_s='('
        for i in range(len(data_addr_list)-1):
            tmp_s=tmp_s+"%"+"s,"
        tmp_s=tmp_s+"%"+"s)"
        #拼接sql语句
        # sql = """SELECT create_time, item_addr, item_val
        #         FROM """+table_name+""" WHERE item_addr in """+tmp_s+""" AND create_time>%s ORDER BY item_addr,create_time DESC;"""
        sql = """SELECT create_time, item_addr, item_val
                FROM """+table_name+""" WHERE create_time>%s ORDER BY item_addr,create_time DESC;"""
       
        data_addr_list.append(cur_time)
        #查询
        # cursor.execute(sql,data_addr_list)
        cursor.execute(sql,[cur_time])
        
        data_tmp=cursor.fetchall()

        print('保留最新数据前的数据是',data_tmp)
    #关闭数据库
    db.close()

    data1=pd.DataFrame(data_tmp,columns=['create_time', 'item_addr', 'item_val'])
    
    output_path = 'output_result/data_0.xlsx'  # 计算结果输出路径
    
    data1.to_excel(output_path)
    
    #每种数据只保留最新的一个
    data=[]
    if data_tmp!=[]:
        data.append(data_tmp[0])
        d_t=data_tmp[0][1]
        for i in range(len(data_tmp)-1):
            if d_t!=data_tmp[i+1][1]:
                data.append(data_tmp[i+1])
                d_t=d_t=data_tmp[i+1][1]
    #转为df格式
    data=pd.DataFrame(data,columns=['create_time', 'item_addr', 'item_val'])
    return data


def write_to_ems_platform(table_name,data_dict):
# table_name是写入的表名，字符串
# data_dict是字典类型，key是数据库列名,value是相应的列对应的值。


    #数据库连接
    db = psycopg2.connect(host='192.168.3.13', port=5432, user='ems', password='Yulin@0903',
                                database='ems_platform')
    cursor = db.cursor()
    #写入时间生成
    write_time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    
    #生成列名
    column_name='create_time'
    data=[write_time]
    for key in data_dict.keys():
        column_name=column_name+','+str(key)
        data.append(data_dict[key])
    data=tuple(data)
    #生成sql语句
    tmp_s='%'+'s'
    for key in data_dict.keys():
        tmp_s=tmp_s+",%"+"s"#批量添加%s
    sql="""INSERT INTO public."""+table_name+"""("""+column_name+""")
        VALUES ("""+tmp_s+""");"""#拼接sql语句
    #执行sql语句
    cursor.execute(sql, data)
    db.commit()
    #关闭数据库
    db.close()

if __name__ == '__main__':

    time_now=datetime.datetime.now().strftime("%Y_%m")
    cur_time = datetime.datetime.now()-datetime.timedelta(minutes=10)
    cur_time = cur_time.strftime("%Y-%m-%d %H:%M")
    print('timenow:',time_now)
    print('curtime:',cur_time)
    t_n_obix='ems_obix_'+time_now
    
    # data_obix=[]

    # for i in range(1, 27):
    #     number = f"{i:02d}"
    #     obaddr=[f'SL3/F1_SL3_{number}/AI_T/']
    #     data_obix[i]=read_from_ems_capture(t_n_obix,obaddr,cur_time)
    
    # print(data_obix)
    
    obaddr=[f'SL3/F1_SL3_01/AI_T/']
    data_obix = read_from_ems_capture(t_n_obix,obaddr,cur_time)

    print(data_obix)

    output_path = 'output_result/data_2.xlsx'  # 计算结果输出路径
    
    data_obix.to_excel(output_path)