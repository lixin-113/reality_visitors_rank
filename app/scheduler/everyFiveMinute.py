'''
    每5分钟执行一次
'''

from datetime import datetime, timedelta
from pathlib import Path
from config import INPUTPATH,OUTPUTPATH,WATCHED_FOLDER,MAX_WORKERS,LOCATION
from concurrent.futures import ThreadPoolExecutor,as_completed
from core.global_logger import my_log
# from core.get_db import get_db_main
# from services.match_police_station import match_station_main
from services.get_history_data import anaylsis_history_main
from services.anaylsis_visitors_by_reality import anaylsis_reality_main
from services.feature_extraction import feature_extraction_main
from services.get_wxa_fea import wxa_feature_main
import pandas as pd
import os
import time

# 配置全局全程任务线程池
executor=ThreadPoolExecutor(MAX_WORKERS)
global processed_files 
processed_files = set()
def process_file(file_path):
    # file_name=os.path.basename(file_path[:-3])
    # output_file_name=file_name.split("_")[-1]
    # date=output_file_name[:8]
    file_name=os.path.basename(file_path)
    output_file_name=file_name.split("_")[-1]
    date=output_file_name[:8]
    try:
        # 访问时间 手机号码 访问网址 网址类型 网址风险等级 地市 区县（有""） 访问日期 访问时长 二次研判网址类型 
        my_log.logger.info(f"正在读取数据中，其路径为：{file_path}")
        data=pd.read_csv(file_path,dtype=str,header=None)
        my_log.logger.info(f"数据读取成功，部分数据为：\n{data[:5]}")
    except Exception as e:
        my_log.logger.error(f"数据格式或文件出现问题：{e}！！！，已略过")
        return False

    # try:
    #     my_log.logger.info(f"正在从数据库中拉取地点：{LOCATION}  基础日期：{date} 的数据中。。。")
    #     # history_data=get_db_main(LOCATION,date)

    #     my_log.logger.info(f"从数据库中获取访客历史数据完成。。。")
    # except Exception as e:
    #     my_log.logger.error(f"在从数据空中拉取地点：{LOCATION} 基础日期：{date} 的数据时出现{e}！！！")

    try:
        my_log.logger.info("正在对访客进行特征提取中。。。")
        data=feature_extraction_main(data)
        my_log.logger.info(f"访客特征提取完成。。。")
    except Exception as e:
        my_log.logger.error(f"在对访客进行特征提取时出现{e}！！！")

    try:
        my_log.logger.info("正在根据历史数据对访客进行分析中。。。")
        # data=anaylsis_history_main()
        
        # jiangning_data=data[data['区县']=="1025023"].copy()
        # jiangning_history_data=anaylsis_history_main(jiangning_data,"jiangning",date)


        # 未知编码
        my_log.logger.info(f"正在处理 kunshan 历史 7 天的结果数据中。。。")
        kunshan_data=data[data['区县']=="1051203"].copy()
        kunshan_history_data=anaylsis_history_main(kunshan_data,"kunshan",date)        
        my_log.logger.info(f"kunshan 历史 7 天的结果数据处理完成，其部分数据为：\n{kunshan_history_data[:5]}")

        my_log.logger.info(f"正在处理 jiangyin 历史 7 天的结果数据中。。。")
        jiangyin_data=data[data['区县']=="1051048"].copy()
        jiangyin_history_data=anaylsis_history_main(jiangyin_data,"jiangyin",date)
        my_log.logger.info(f"正在处理 jiangyin 历史 7 天的结果数据中，其部分数据为：\n{jiangyin_history_data[:5]}")

        my_log.logger.info(f"正在处理 nantong 历史 7 天的结果数据中。。。")
        nantong_data=data[data['地市']=="1000513"].copy()
        nantong_history_data=anaylsis_history_main(nantong_data,"nantong",date)
        my_log.logger.info(f"正在处理 nantong 历史 7 天的结果数据中，其部分数据为：\n{nantong_history_data[:5]}")

        other_data=data[(data['地市']!="1000513") & (data['区县'] != "1051048") & (data['区县']!="1051203")].copy()
        other_data['history_res']="['']"
        other_data['date_time_df2']="['']"
        other_history_data=other_data[['手机号','date_time_df2','history_res']].copy()

        total_history_data=pd.concat([nantong_history_data,kunshan_history_data,jiangyin_history_data,other_history_data],axis=0,ignore_index=True)

        del nantong_data,nantong_history_data,jiangyin_history_data,jiangyin_data,kunshan_history_data,kunshan_data,other_data,other_history_data
        my_log.logger.info(f"根据历史数据对访客进行分级完成。。。")

    except Exception as e:
        my_log.logger.error(f"在根据历史数据对访客进行分级时出现{e}！！！")
        total_history_data=pd.DataFrame(columns=['手机号','date_time_df2','history_res'])

    try:
        my_log.logger.info("正在提取网信安数据中。。。")
        phone_number=data['手机号'].drop_duplicates().tolist()
        df_anj_dic, df_ai_dic, df_gw_dic=wxa_feature_main(phone_number,date)
        my_log.logger.info(f"网信安数据提取完成。。。")
    except Exception as e:
        my_log.logger.error(f"在提取网信安数据时出现{e}！！！")


    try:
        my_log.logger.info("正在根据实时数据对访客进行分级中。。。")
        output=anaylsis_reality_main(data,df_anj_dic, df_ai_dic, df_gw_dic,total_history_data,date)
        my_log.logger.info(f"根据实时数据对访客进行分级完成。。。")
    except Exception as e:
        my_log.logger.error(f"在根据实时数据对访客进行分级时出现：{e}！！！")

    try:
        my_log.logger.info("正在根据历史访客数据分级结果 和 实时访客数据分级结果 进行分级中。。。 ")
        my_log.logger.info(f"fff")
    except Exception as e:
        my_log.logger.error(f"在根据历史访客数据分级结果 和 实时访客数据分级结果 进行分级时出现：{e}！！！")


    try:
        # track_data=data[["phone","lng","lat","start_date","end_date","duration"]]
        my_log.logger.info(f"正在根据访客的经纬度，获取其当地所在的派出所中。。。")
        # res=match_station_main(track_data,location=LOCATION)
        # data=pd.merge(data,res,on='phone',how='left')
        # data["DWMC"]=data["DWMC"].fillna("未匹配出所处派出所")
        my_log.logger.info(f"成功获取到访客所处于的派出所，其部分数据为：\n{data[:5]}")
    except Exception as e:
        my_log.logger.error(f"在根据访客的经纬度，获取其当地所在的派出所时出现：{e}！！！")

    try:
        my_log.logger.info("正在导出结果文件中。。。")
        file_name=os.path.basename(file_path)
        output_path_dir=os.path.join(OUTPUTPATH,date)
        os.makedirs(output_path_dir,exist_ok=True)
        output_path=os.path.join(output_path_dir,output_file_name)
        # output=data[['phone','res','DWMC','review']].copy()
        output.to_csv(output_path,index=None)
        my_log.logger.info(f"结果文件导出成功，其路径为{output_path}")
    except Exception as e:
        my_log.logger.error(f"在导出结果文件时出现：{e}！！！")
        return False
    
    my_log.logger.info("这一批的数据任务处理已经处理完成·······")

    return True



# def everyFiveMinute():

#     my_log.logger.info(f"现在的时间为： {datetime.now()}")

#     my_log.logger.info("正在等待新增文件中。。。")
#     """扫描指定文件夹，将新文件提交到线程池"""
    
#     folder = Path(WATCHED_FOLDER)
#     if not folder.exists():
#         my_log.logger.info(f"[警告] 目录不存在: {WATCHED_FOLDER}")
#         return

#     current_files = set()
    
#     for file_path in folder.iterdir():
#         if file_path.is_file():
#             current_files.add(str(file_path.resolve()))

#     # 找出新增文件（未处理过的）
#     new_files = current_files - processed_files
#     for fp in new_files:
#         executor.submit(process_file, fp)

#         processed_files.add(fp)  # 标记为已提交（非已完成）

#     if new_files:
#         my_log.logger.info(f"[发现] 新增 {len(new_files)} 个文件，已提交处理")
#     else:
#         my_log.logger.info("[空闲] 无新文件")


def safe_remove(path):
    """安全删除文件"""
    try:
        os.remove(path)
        my_log.logger.info(f"已删除文件: {path}")
    except Exception as e:
        my_log.logger.error(f"删除文件失败 {path}: {e}")


def everyFiveMinute():
    my_log.logger.info(f"现在的时间为：{datetime.now()}")
    my_log.logger.info("正在等待新增文件中。。。")

    folder = Path(WATCHED_FOLDER)
    if not folder.exists():
        my_log.logger.warning(f"[警告] 目录不存在: {WATCHED_FOLDER}")
        return

    # 获取所有 .csv 文件（原始文件）
    csv_files = [f for f in folder.iterdir() if f.is_file() and f.suffix == '.csv']
    
    # 构建待处理任务：只有存在对应 .csvOK 的才处理
    new_tasks = []
    for csv_file in csv_files:
        ok_file = csv_file.with_name(csv_file.name + "OK")  # xxx.csv -> xxx.csvOK
        if ok_file.exists():
            # 使用绝对路径字符串作为唯一标识
            csv_abs = str(csv_file.resolve())
            if csv_abs not in processed_files:
                new_tasks.append((csv_file, ok_file))
        else:
            my_log.logger.debug(f"跳过 {csv_file.name}：缺少对应的 OK 副本")

    if not new_tasks:
        my_log.logger.info("[空闲] 无符合条件的新文件（需同时存在 .csv 和 .csvOK）")
        return

    # 提交任务并跟踪 Future
    future_to_files = {}
    for csv_file, ok_file in new_tasks:
        future = executor.submit(process_file, str(csv_file.resolve()))
        future_to_files[future] = (str(csv_file.resolve()), str(ok_file.resolve()))

    my_log.logger.info(f"[发现] 新增 {len(new_tasks)} 个有效文件对，已提交处理")

    # 等待完成并清理文件
    for future in as_completed(future_to_files):
        csv_path, ok_path = future_to_files[future]
        try:
            success = future.result()
            if success:
                # 成功才删除
                safe_remove(csv_path)
                safe_remove(ok_path)
                processed_files.add(csv_path)  # 标记为已处理（且已删除）
            else:
                my_log.logger.warning(f"文件处理失败，暂不删除: {csv_path}")
        except Exception as e:
            my_log.logger.error(f"任务执行异常 {csv_path}: {e}")
 