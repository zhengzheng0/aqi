"""
    作者：刘亚萍
    功能：空气质量指数计算

"""
#相关库导入
import pandas as pd
import numpy as np

import traceback
import time

from log_code import TNLog
#日志记录设置
logging = TNLog(dir='log',name='date at ' + time.strftime('%Y%m%d', time.localtime()))


def cal_linear(iaqi_lo,iaqi_hi,bp_lo,bp_hi,cp):
    """
        线性转换
    """
    iaqi = (iaqi_hi - iaqi_lo) * (cp - bp_lo) / (bp_hi - bp_lo) + iaqi_lo
    return iaqi


def check_num(parm, num):
    '''
    异常识别并抛出异常
    :param parm: co/pm25/pm10
    :param num: 数值
    :return:
    '''
    if parm == 'pm25':
        if num > 501:
            raise ValueError("PM2.5值超出正常数值范围！")
    elif parm == 'co':
        if num > 151:
            raise ValueError("CO值超出正常数值范围！")
    elif parm == 'pm10':
        if num > 501:
            raise ValueError("CO值超出正常数值范围！")


def cal_pm25_iaqi(pm25):
    """
    计算pm2.5的IAQI
    """
    try:
        if 0 <= pm25 <36:
            iaqi = cal_linear(0,50,0,35,pm25)
        elif 36 <= pm25<76:
            iaqi = cal_linear(50,100,35,75,pm25)
        elif 76 <= pm25 <116:
            iaqi = cal_linear(100,150,75,115,pm25)
        elif 116 <= pm25 < 151:
            iaqi = cal_linear(150, 200, 115, 150, pm25)
        elif 151 <= pm25 < 251:
            iaqi = cal_linear(200, 300, 150, 250, pm25)
        elif 251 <= pm25 < 351:
            iaqi = cal_linear(300, 400, 250, 350, pm25)
        elif 351 <= pm25 < 501:
            iaqi = cal_linear(400, 500, 350, 500, pm25)
        else:
            check_num('pm25', pm25)
        return iaqi
    except Exception as e:
        print(e)
        logging.error('pm25指数超出正常数值范围.')


def cal_co_iaqi(co):
    """
    计算co的IAQI
    """
    try:
        if 0 <= co < 6:
            iaqi = cal_linear(0, 50, 0, 5, co)
        elif 6 <= co < 11:
            iaqi = cal_linear(50, 100, 5, 10, co)
        elif 11 <= co < 36:
            iaqi = cal_linear(100, 150, 10, 35, co)
        elif 36 <= co  < 61:
            iaqi = cal_linear(150, 200, 35, 60, co)
        elif 61 <= co  < 91:
            iaqi = cal_linear(200, 300, 60, 90, co)
        elif 91 <= co  < 121:
            iaqi = cal_linear(300, 400, 90, 120, co)
        elif 121 <= co  < 151:
            iaqi = cal_linear(400, 500, 120, 150, co)
        else:
            check_num('co', co)
        return iaqi
    except Exception as e:
        print(e)
        logging.error('CO指数超出正常数值范围.')



# def cal_so2_iaqi(so2):
#     """
#     计算so2的IAQI
#     """
#     if 0 <= so2 < 151:
#         iaqi = cal_linear(0, 50, 0, 150, so2)
#     elif 151 <= so2 < 501:
#         iaqi = cal_linear(50, 100, 150, 500, so2)
#     elif 501 <= so2 < 651:
#         iaqi = cal_linear(100, 150, 500, 650, so2)
#     else:
#         iaqi = cal_linear(150, 200, 650, 800, so2)
#     return iaqi

# def cal_no2_iaqi(no2):
#     """
#     计算no2的IAQI
#     """
#     if 0 <= no2 < 101:
#         iaqi = cal_linear(0, 50, 0, 100, no2)
#     elif 101 <= no2 < 201:
#         iaqi = cal_linear(50, 100, 100, 200, no2)
#     elif 201 <= no2 < 701:
#         iaqi = cal_linear(100, 150, 200, 700, no2)
#     elif 701 <= no2  < 1201:
#         iaqi = cal_linear(150, 200, 700, 1200, no2)
#     elif 1201 <= no2  < 2341:
#         iaqi = cal_linear(200, 300, 1200, 2340, no2)
#     elif 2341 <= no2  < 3091:
#         iaqi = cal_linear(300, 400, 2340, 3090, no2)
#     elif 3091 <= no2  < 3841:
#         iaqi = cal_linear(400, 500, 3090, 3840, no2)
#     else:
#         pass
#     return iaqi

def cal_pm10_iaqi(pm10):
    """
    计算pm10的IAQI
    """
    if 0 <= pm10 <51:
        iaqi = cal_linear(0,50,0,50,pm10)
    elif 51 <= pm10<151:
        iaqi = cal_linear(50,100,50,150,pm10)
    elif 151 <= pm10 <251:
        iaqi = cal_linear(100,150,150,250,pm10)
    elif 251 <= pm10 < 351:
        iaqi = cal_linear(150, 200, 250, 350, pm10)
    elif 351 <= pm10 < 421:
        iaqi = cal_linear(200, 300, 350, 420, pm10)
    elif 421 <= pm10 < 501:
        iaqi = cal_linear(300, 400, 420, 500, pm10)
    elif 501 <= pm10 < 601:
        iaqi = cal_linear(400, 500, 500, 600, pm10)
    else:
        pass
    return iaqi


def cal_aqi(param_list):
    """
    :param：param_list  监测指标列表
    :return: aqi AQI指数
    """
    pm25 = param_list[0]
    co = param_list[1]
    pm10 = param_list[2]

    pm25_iaqi =cal_pm25_iaqi(pm25)
    co_iaqi = cal_co_iaqi(co)
    pm10_iaqi = cal_pm10_iaqi(pm10)

    iaqi_list = []
    iaqi_list.append(pm25_iaqi)
    iaqi_list.append(co_iaqi)
    iaqi_list.append(pm10_iaqi)

    aqi = max(iaqi_list)
    logging.info(f'当前计算AQI指数为：{aqi}')
    return aqi


def cal_aqi_evaluation(aqi_val):
    '''
    :param aqi_val
    :return: aqi_est AQI等级
    '''
    if 0 <= aqi_val <= 50:
        aqi_est = '优'
    elif 51 <= aqi_val <= 100:
        aqi_est = '良'
    elif 101 <= aqi_val <= 150:
        aqi_est = '轻度污染'
    elif 151 <= aqi_val <= 200:
        aqi_est = '中度污染'
    elif 201 <= aqi_val <= 300:
        aqi_est = '重度污染'
    else:
        aqi_est = '严重污染'
    logging.info(f'当前计算AQI等级为：{aqi_est}')
    return aqi_est


if __name__ == "__main__":
    input_path = 'input_data/env_data.xlsx'  # 输入数据的路径
    output = pd.DataFrame({'aqi_val': 0, 'aqi_est': 0}, index=['00'])  #输出初始化
    try:
        logging.info('读入数据')

        input_data = pd.read_excel(input_path, index_col='datetime')
    except Exception as e_:
        logging.error('读取excel数据失败,错误原因为')
        logging.error(traceback.format_exc())
        raise e_
    logging.info('数据读取成功')
    # print(input_data)
    start_t = 0  # 待求时段的起始时刻
    # length = 2   #待求时段的长度
    end_t = start_t + len(input_data)  #待求时段的终止时刻
    # end_t = start_t + 2  # 待求时段的终止时刻
    for i in range(start_t, end_t):
        logging.info(f'【第{i}时刻】')
        pm25 = input_data.iloc[i, 0]
        co = input_data.iloc[i, 1]
        pm10 = input_data.iloc[i, 2]
        if pm25 == np.nan or co == np.nan or pm10 == np.nan:
            logging.error(f'【第{i}时刻】的pm25或co存在缺失值，需检查数据，已经设置为默认值0')
            pm25 = pm25 if not pm25 else 0
            co = co if not co else 0
            pm10 = pm10 if not pm10 else 0
        else:
            logging.info('输入正常')
        param_list = []
        param_list.append(pm25)
        param_list.append(co)
        param_list.append(pm10)
        # print(param_list)
        logging.info(f'计算【第{i}时刻】AQI指数及等级')
        '''
        计算AQI指数及等级
        '''
        # 1、AQI指数#
        try:
            aqi_val = cal_aqi(param_list)
        # 2、AQI评价#
            aqi_est = cal_aqi_evaluation(aqi_val)
            print(f'{input_data.index[i]}时刻的AQI指数为：{aqi_val}，AQI评价为：{aqi_est}')
            logging.info('计算完毕')
            is_error = False
        except Exception as e:
            is_error = True
            logging.error('计算AQI失败，原因如下：')
            logging.error(traceback.format_exc())


        '''
        output
        '''
        stamp = input_data.index[i]
        output_data = {'aqi_val': aqi_val, 'aqi_est': aqi_est,'is_error':is_error}
        output_i = pd.DataFrame(output_data, index=[str(stamp)])  # 输出结果构建表格
        output = pd.concat([output, output_i], axis=0)
        logging.info('输出完毕')

    try:
        formatted_time = time.strftime('%Y%m%d_%H_%M', time.localtime())
        output_path = f'output_result/AQI_data_{formatted_time}.xlsx'  # 计算结果输出路径
        output = output.drop(index='00')
        output = output.rename_axis("datetime")
        print(output)
        output.to_excel(output_path)
        logging.info('----------保存输出数据完毕----------')


    except BaseException as E:
        logging.error('保存输出数据失败,错误原因为:')
        logging.error(traceback.format_exc())
        raise Exception


