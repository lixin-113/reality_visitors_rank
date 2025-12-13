import pandas as pd
from collections import Counter
from core.global_logger import my_log


def process_data(data):
    # 访问时间 手机号码 访问网址 网址类型 网址风险等级 地市 区县（有""） 访问日期 访问时长 二次研判网址类型 
    
    data.columns=["时间","手机号","网址","网址类型","网址风险等级","地市","区县","访问日期","访问时长","二次研判网址类型"]
    data['区县']=data['区县'].fillna("未知区县编码")
    data['地市']=data['地市'].fillna("未知地市编码")
    data['访问时长']=data['访问时长'].astype(int)
    data['时间'] = pd.to_datetime(data['时间'],format="%Y-%m-%d %H:%M:%S", errors='coerce')
    data['网址类型']=data['网址类型'].fillna("未知类型")
    data['二次研判网址类型']=data['二次研判网址类型'].fillna("未知类型")
    data['结合网址类型']=data.apply(lambda row: row['二次研判网址类型'] if row['二次研判网址类型']!="未知类型" else row['网址类型'], axis=1)
    my_log.logger.info(f"部分预处理完后的数据为：\n{data[:5]}")
    return data


def get_main_type(host_type_list):
    counter=Counter(host_type_list)
    main_type=counter.most_common(1)[0][0]
    return pd.Series([main_type, counter[main_type],counter])



def divide_time_period(visitor_time):
    visitor_hour=visitor_time.hour
    if visitor_hour<0 or visitor_hour>=20:
        return '夜间' 
    else:
        if 0>=visitor_hour or visitor_hour<=4:
            return '深夜'
        else:
            return '白天'

def comb_time(data):
    grouped=data.groupby('手机号').size().reset_index(name='访问网址次数')
    type_visitor_durtion=data.groupby(['手机号','结合网址类型'])['访问时长'].sum().reset_index()
    visitor_durtion=type_visitor_durtion.groupby('手机号')[['结合网址类型','访问时长']].apply(lambda x:dict(zip(x['访问时长'],x['结合网址类型']))).reset_index(name="网址类型总访问时长字典")
    merged=data.groupby('手机号').agg(
                访问网址类型列表=("结合网址类型",list),
                访问网址类型集合=("结合网址类型",set),
            ).reset_index()
    merged[['访问网址主要类型','访问网址主要类型次数','访问网址类型计数表']]=merged['访问网址类型列表'].apply(get_main_type)
    merged=merged.merge(grouped, on='手机号', how='left')
    merged=merged.merge(visitor_durtion,on='手机号',how='left')
    phone_location=data.drop_duplicates(subset="手机号", keep="first", ignore_index=True).copy()
    phone_location['时段']=phone_location['时间'].apply(divide_time_period)
    merged=merged.merge(phone_location[['手机号','地市','区县','时段']],on='手机号',how='left')
    merged['访问网址类型计数表']=merged['访问网址类型计数表'].fillna("")
    merged['网址类型总访问时长字典']=merged['网址类型总访问时长字典'].fillna("")
    return merged

def feature_extraction_main(data):
    data_copy=data.copy()
    data_copy=process_data(data_copy)
    data_copy=comb_time(data_copy)
    return data_copy


if __name__=="__main__":
    data=pd.read_csv("/data/app/test/reality_test.csv",sep="\t",dtype=str)
    data=feature_extraction_main(data)
    print(data)
    data.to_csv("/data/app/test/feature_extraction_result.csv",sep="\t",index=False)
