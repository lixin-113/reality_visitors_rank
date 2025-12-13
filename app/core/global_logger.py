from os import name
from loguru import logger 
import os

class my_logger:
    log=None
    def __init__(self):
        self.logger=None
        self.sink_id=None
        
    # 配置日志
    def setup_logger(self,log_filename:str,log_dir:str,level="INFO"):
        
        """设置并返回一个 logger 实例"""
        # 创建 logs 目录（如果不存在）
        
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        log_path = os.path.join(log_dir, log_filename)

        # 日志输出规范
        logger_format = "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {name}:{function}:{line} - {message}"

        # 创建 logger
        sink_id=logger.add(log_path, level=level,format=logger_format,rotation="10 MB")
        self.sink_id=sink_id
        return logger
    def setup_log(self,log_filename,log_dir):
        # 全局 logger 实例（可以在模块级别初始化一次）
        self.logger = self.setup_logger(log_filename,log_dir)
        
    def close_logger(self):
        """关闭 logger，释放资源"""
        self.logger.remove(self.sink_id)
    

my_log=my_logger()
if __name__=="__main__":
    my_log.setup_log("app.log","/data/app/logs")
    my_log.logger.info("ffff")
    def jjj():

        my_log.logger.info("fffgagh")
        
    jjj()

    