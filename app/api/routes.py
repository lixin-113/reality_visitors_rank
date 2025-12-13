from fastapi import APIRouter,status
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from typing import Optional,Any
from model.request_body import My_request
import pandas as pd
from services.match_police_station import match_station_main,load_map_information,match_points
from core.global_logger import my_log
import os


router=APIRouter()


"""
# 成功响应规范
# msg : str|None 请求消息
# process_message : 处理内容
"""

@router.get("/test")
def test(msg: Optional[str]="请求成功"):

    
    return { 
            'msg' : msg,
        }
    

