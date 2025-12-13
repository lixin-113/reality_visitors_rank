from mysql import connector
import mysql
import sys
sys.path.append('/data/app')


from config import host,user,passwd,database,phone_field,date_field,location_field,MySqlConfig
from datetime import datetime,timedelta
from core.global_logger import my_log
import pandas as pd






def select_sql(query_template: str, phone_list: list):
    """支持 IN 查询的数据库查询函数"""
    connection = None
    cursor = None
    try:
        # 动态生成占位符: %s, %s, %s, ...
        placeholders = ','.join(['%s'] * len(phone_list))
        # 替换模板中的 %s 为实际占位符
        query = query_template % placeholders

        connection = mysql.connector.connect(**MySqlConfig)
        cursor = connection.cursor()
        my_log.logger.debug(f"执行SQL: {query}")
        cursor.execute(query, phone_list)  # 传入 list 作为 tuple 自动转换
        result = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        df = pd.DataFrame(result, columns=columns)
        my_log.logger.info(f"SQL查询返回 {df[['other_number','call_time']]} ")
        my_log.logger.info(f"SQL查询返回 {len(df)} 条记录。")
        return df
    except Exception as e:
        my_log.logger.error(f"数据库查询失败: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# ==================== 安全的SQL查询函数（使用参数化）====================
def get_IM_sql():
    return """
    SELECT * 
    FROM dco_involved_msisdn_clue_extend 
    WHERE other_number IN (%s)
    """

def get_AI_sql():
    return """
    SELECT * 
    FROM dco_ai_warning_clue_extend 
    WHERE other_number IN (%s)
    """

def get_HR_sql():
    return """
    SELECT * 
    FROM dco_high_risk_msisdn_clue_extend 
    WHERE other_number IN (%s)
    """

# 从数据库中获取历史访客分级数据
def get_db_main(location:str,date:str):

    
    start_time = (datetime.strptime(date,"%Y%m%d")-timedelta(days=11)).date().strftime("%Y-%m-%d")
    end_time = (datetime.strptime(date,"%Y%m%d")-timedelta(days=1)).date().strftime("%Y-%m-%d")
    my_log.logger.info(f"从历史访客等级数据库拉取的时间为 {start_time} 到 {end_time} ，地点为 {location} 的数据")
    columns=[]
    columns_type=[]
    data=[]

    try:
        my_log.logger.info("正在尝试连接数据库中。。。")
        my_log.logger.info(f"数据库的主机地址为：{host}")
        my_log.logger.info(f"数据库的用户名为：{user}")
        my_log.logger.info(f"连接数据库中所要调用的数据库为：{database}")
        
    except Exception  as e:
        my_log.logger.error(f"在连接数据库时出现：{e}！！！")
    mydb = mysql.connector.connect(
                host=host,       # 数据库主机地址
                user=user,    # 数据库用户名
                passwd=passwd,   # 数据库密码
                database=database, # 调用的数据库
                port=3306
            )
    mycursor=mydb.cursor()
    # mycursor.execute("SHOW DATABASES")
    mycursor.execute("USE visitors_rank")
    my_log.logger.info("正在从数据库中获取指定需求的数据中。。。")
    mycursor.execute(f"""
                     select * from `{database}` 
                     WHERE `{date_field}` > '{start_time}' AND 
                     `{date_field}` <= '{end_time}' AND 
                     `{location_field}` = '{location}';
                     """)
    for x in mycursor:
        data.append([res for res in x])

    mycursor.execute("describe visitors_rank")
    for x in mycursor:
        columns.append(x[0])
        columns_type.append(x[1])

    df=pd.DataFrame(data,columns=columns)
    my_log.logger.info(f"已经从数据中拉取指定数据完成，部分数据框数据为：\n{df}")

    return df



if __name__=="__main__":
    my_log.setup_log("app.log","/data/app/logs")
    get_db_main("jiangning","20251121")
