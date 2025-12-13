from fastapi import FastAPI
from contextlib import asynccontextmanager
from .global_logger import my_log
from datetime import datetime
import os
from config import LOG_NAME,LOG_PATH
from scheduler import scheduler
from services.match_police_station import load_map_information
# 创建生命周期
@asynccontextmanager
async def lifespan(app:FastAPI):

    # 根据当日日期创建日志
    realtime=datetime.now().date().strftime("%Y%m%d")
    log_path=os.path.join(LOG_PATH,realtime)

    # 创建日志路径
    os.makedirs(log_path,exist_ok=True)

    # 创建全局日志，并初始化
    my_log.setup_log(LOG_NAME,log_path)

    """
    定时任务每5分钟将input文件夹下的所有文件导入线程池，后面只导入新增文件    
    """
    my_log.logger.info("正在启动定时任务中。。。")

    scheduler.start()

    yield
    
    scheduler.shutdown()
    my_log.close_logger()
    my_log.logger.info("定时任务已经关闭！！！")