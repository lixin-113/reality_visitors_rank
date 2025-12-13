from fastapi import FastAPI
from api.routes import router
from model.request_body import My_request
from core.global_logger import my_log
from contextlib import asynccontextmanager
from datetime import datetime
from core.life import lifespan
from typing import Optional
from services.match_police_station import match_points,match_station_main,load_map_information
import os
import pandas as pd


app=FastAPI(lifespan=lifespan)

app.include_router(router,prefix="")
map_data=load_map_information()
@app.post("/police_station")
def get_police_station(request:My_request,msg: Optional[str]="请求成功"):
    input_path=request.data_path
    output_path_dir=request.output_path
    os.makedirs(output_path_dir,exist_ok=True)
    file_name=os.path.basename(input_path)
    data=pd.read_csv(input_path,sep="\t",dtype=str)


    # for test
    data['msisdn']=data['phone']
    data['res']=""
    data['remark']=""
    data['city_code']=""



    data['lng']=data['lng'].astype(float)
    data['lat']=data['lat'].astype(float)
    try:
        my_log.logger.info("正在根据访客经纬度判断处于哪一个派出所中。。。")
        res=match_points(data,"lng","lat",map_data)
        my_log.logger.info("判断访客所处派出所已经完成。。。")
    except Exception as e:
        my_log.logger.info(f"在根据访客经纬度判断处于哪一个派出所时出现：{e}！！！")
        return {
            'msg' : "请求失败",
            'process_content':f"ERROR：在根据访客经纬度判断处于哪一个派出所时出现：{e}！！！"
        }
    
    try:
        my_log.logger.info("正在导出访客所处派出所的文件中。。。")
        output_path=os.path.join(output_path_dir,f"OK{file_name}")
        res.to_csv(output_path,index=None)
        my_log.logger.info(f"文件导出的路径为：{output_path}")
    except Exception as e:
        my_log.logger.info(f"在导出访客所处派出所的文件时出现：{e}！！！")
        return {
            'msg' : "请求失败",
            'process_content':f"ERROR：在导出访客所处派出所的文件时出现：{e}"
        }

    
    return { 
            'msg' : msg,
            'process_content':f"文件路径为：{input_path} 已经匹配出相应的派出所，其输出路径为：{output_path_dir}，输出文件名为：OK{file_name}。"
        }
    