from sql_demo import *
import numpy as np

if __name__ == '__main__':
    
    time_now=datetime.datetime.now().strftime("%Y_%m")
    cur_time = datetime.datetime.now()-datetime.timedelta(minutes=10)
    cur_time = cur_time.strftime("%Y-%m-%d %H:%M")
         
    print('timenow:',time_now)
    print('curtime:',cur_time)

    t_n_obix='ems_obix_'+time_now
    # obaddr=['SL3/F1_SL3_01/AI_H/']
    obaddr=['CH_1/T_1/']
    
    data_obix=[]
    data_obix=read_from_ems_capture(t_n_obix,obaddr,cur_time)
    print(data_obix)
    
    for i in range(2, 5):
        number = f"{i:0d}"
        # number = f"{i:02d}"
        # obaddr=[f'SL3/F1_SL3_{number}/AI_T/']
        obaddr=[f'CH_1/T_{number}/']
        
        data = read_from_ems_capture(t_n_obix,obaddr,cur_time)
        
        data_obix=pd.concat([data_obix, data], ignore_index=True)
    
        
    
    print(data_obix)
    
    data_num = pd.to_numeric(data_obix['item_val'])
    
    print(data_num)
    X = data_num.mean()
   
    print(X)
    # obaddr=['SL3/F1_SL3_02/AI_H/']
    
    # t_n_obix='ems_obix_'+time_now

    # data_obix=read_from_ems_capture(t_n_obix,obaddr,cur_time)

    # print(data_obix)