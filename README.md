# 涉诈网址实时访客风险等级分级模型
这是一个定时对实时访客进行风险等级分级的服务。
## 项目结构

```
app/
├── api/              
├── logs/               # 日志
├── core/               # 中间件
├── data/               # 派出所地图
├── model/              # 请求体
├── scheduler/          # 定时处理器
├── services/           # 处理函数
├── config.py           # 基本配置信息
├── docker-compose.yaml # 容器启动脚本
├── main.py             # 应用入口
├── match_station.py    # 匹配派出所
├── run.sh              # 启动脚本
└── run_test.sh         # 测试启动脚本
README.md         
```
## 运行服务
uvicorn main:app  --host 127.0.0.1 --port 8231 --workers 1

## 定时任务
每5分钟处理指定文件夹下的访客数据，对其进行风险等级分级。

## 匹配派出所
根据提供的实时访客为“紧急”的部分位置信息，进行匹配相应的派出所信息
