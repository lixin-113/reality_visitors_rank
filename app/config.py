# 数据输入输出路径
INPUTPATH="/data/app/input"
OUTPUTPATH="/data/output/model_res"
HISTORYPATH="/data/history_data"
HISTORYNAME="res.csv"

# 配置
WATCHED_FOLDER = "/data/output/zhga_szwz_host_real_data"  # 监控扫描的目录
MAX_WORKERS = 1  # 线程池最大并发数


# 日志路径和名称
LOG_PATH="/data/app/logs"
LOG_NAME="app.log"
WXA_FILEPATH="/data/wxa_data"

# 设置风险等级分值
MAP_DICT={"紧急":4,"高危":3,"中危":2,"低危":1}


# 地点
# LOCATION="nantong"
# LOCATION="kunshan"
LOCATION="jiangning"

# 地域地图和地图文件名称
LOCATION_MAP_PATH="/data/app/data"
MAP_NAME="派出所边界.shp"


# 地区编码映射
# LOCATION_CODE_MAPPING={
#     '102513':'江宁',
#     '10513':'南通',
#     '10255':'昆山'
# }

TYPE_SET=set(['色情博彩引流','裸聊诈骗','招嫖诈骗','博彩诈骗','虚假服务诈骗','公检法','冒充电商客服诈骗','虚假购物诈骗','仿冒政府网站诈骗','刷单诈骗','客服系统','理财诈骗','贷款诈骗'])
RISK_TYPE_SET=set(['虚假服务诈骗','公检法','冒充电商客服诈骗','虚假购物诈骗','仿冒政府网站诈骗','刷单诈骗','客服系统','理财诈骗','贷款诈骗'])

