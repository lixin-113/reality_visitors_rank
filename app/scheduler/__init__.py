
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime, timedelta
 
scheduler = BackgroundScheduler()

from .everyFiveMinute import everyFiveMinute
# 每分钟
"""
--------------cron------------
year (int|str) – 4-digit year

month (int|str) – month (1-12)

day (int|str) – day of month (1-31)

week (int|str) – ISO week (1-53)

day_of_week (int|str) – number or name of weekday (0-6 or mon,tue,wed,thu,fri,sat,sun)

hour (int|str) – hour (0-23)

minute (int|str) – minute (0-59)

second (int|str) – second (0-59)

start_date - 指定开始时间 (datetime|str) – starting point for the interval calculation

"""
# 注册定时任务
scheduler.add_job(
        func=everyFiveMinute,
        # trigger=IntervalTrigger(minutes=5),

        # for test
        trigger=IntervalTrigger(seconds=5),
        id='scan_job',
        name='每5分钟扫描文件夹',
        replace_existing=True
        )


