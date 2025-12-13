import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from core.global_logger import my_log
from config import LOCATION_MAP_PATH,MAP_NAME
import os

def fix_encoding(text):
    """
    尝试修复由于编码错误导致的乱码字符串。
    """
    if pd.isna(text):
        return text
    try:
        byte_repr = text.encode('latin1', errors='ignore')
        return byte_repr.decode('utf-8')
    except Exception:
        return text

def process_jiangning_map_data(data):

    # 修复字符串列的编码
    string_columns = data.select_dtypes(include=['object']).columns
    for col in string_columns:
        data[col] = data[col].astype(str).apply(fix_encoding)

    # 修复列名编码
    data.columns = [fix_encoding(col) for col in data.columns]
    print(f"江宁 原地域数据为的特征为：\n{data.columns}")
    data.columns = ['DWBM', 'DWMC', '所属分局', 'DJD', 'DJDBH', 'geometry']
    if data.crs != "EPSG:4326":
        data=data.to_crs("EPSG:4326")

    data.rename(columns={'DWMC': '派出所'}, inplace=True)
    data['派出所'] = data['派出所'].replace({
        '淳化派出所': '大学城派出所',
        '土桥派出所': '淳化派出所',
        '周岗派出所': '龙都派出所',
        '东善桥派出所': '秣陵派出所',
        '机场派出所': '禄口派出所'
    }) 
    data['country_code']="1025023"
    print("江宁 Shapefile 加载和编码错误 已经完成。。。")
    print(f"江宁 预处理后的部分地域数据为：\n{data[:5]}")


    return data


def process_nantong_map_data(data):

    # 修复字符串列的编码
    string_columns = data.select_dtypes(include=['object']).columns
    for col in string_columns:
        data[col] = data[col].astype(str).apply(fix_encoding)

    # 修复列名编码
    data.columns = [fix_encoding(col) for col in data.columns]
    print(f"南通 原地域数据为的特征为：\n{data.columns}")
    data.columns = ['DWBM', 'DWMC', '所属分局', 'DJD', 'DJDBH', 'geometry']
    if data.crs != "EPSG:4326":
        data=data.to_crs("EPSG:4326")

    data.rename(columns={'DWMC': '派出所'}, inplace=True)
    # data['派出所'] = data['派出所'].replace({
    #     '淳化派出所': '大学城派出所',
    #     '土桥派出所': '淳化派出所',
    #     '周岗派出所': '龙都派出所',
    #     '东善桥派出所': '秣陵派出所',
    #     '机场派出所': '禄口派出所'
    # }) 
    data['country_code']="1000513"
    print("南通 Shapefile 加载和编码错误 已经完成。。。")
    print(f"南通 预处理后的部分地域数据为：\n{data[:5]}")


    return data


def process_kunshan_map_data(data):

    # 修复字符串列的编码
    string_columns = data.select_dtypes(include=['object']).columns
    for col in string_columns:
        data[col] = data[col].astype(str).apply(fix_encoding)

    # 修复列名编码
    data.columns = [fix_encoding(col) for col in data.columns]
    print(f"昆山 原地域数据为的特征为：\n{data.columns}")
    data.columns = ['DWBM', 'DWMC', '所属分局', 'DJD', 'DJDBH', 'geometry']
    if data.crs != "EPSG:4326":
        data=data.to_crs("EPSG:4326")
    
    data.rename(columns={'DWMC': '派出所'}, inplace=True)
    # data['派出所'] = data['派出所'].replace({
    #     '淳化派出所': '大学城派出所',
    #     '土桥派出所': '淳化派出所',
    #     '周岗派出所': '龙都派出所',
    #     '东善桥派出所': '秣陵派出所',
    #     '机场派出所': '禄口派出所'
    # }) 
    data['country_code']="1051203"
    print("昆山 Shapefile 加载和编码错误 已经完成。。。")
    print(f"昆山 预处理后的部分地域数据为：\n{data[:5]}")


    return data


def process_jiangyin_map_data(data):

    # 修复字符串列的编码
    string_columns = data.select_dtypes(include=['object']).columns
    for col in string_columns:
        data[col] = data[col].astype(str).apply(fix_encoding)

    # 修复列名编码
    data.columns = [fix_encoding(col) for col in data.columns]
    print(f"江阴 原地域数据为的特征为：\n{data.columns}")
    data.columns = ['DWBM', 'DWMC', '所属分局', 'DJD', 'DJDBH', 'geometry']
    if data.crs != "EPSG:4326":
        data=data.to_crs("EPSG:4326")
    
    data.rename(columns={'DWMC': '派出所'}, inplace=True)
    # data['派出所'] = data['派出所'].replace({
    #     '淳化派出所': '大学城派出所',
    #     '土桥派出所': '淳化派出所',
    #     '周岗派出所': '龙都派出所',
    #     '东善桥派出所': '秣陵派出所',
    #     '机场派出所': '禄口派出所'
    # }) 
    data['country_code']="1051048"
    print("江阴 Shapefile 加载和编码错误 已经完成。。。")
    print(f"江阴 预处理后的部分地域数据为：\n{data[:5]}")


    return data




def match_points(track_data,lng_fea,lat_fea,gd_data):


    # 匹配出所有点所在的位置是否在指定位置中
    geometry=[Point(xy) for xy in zip(track_data[lng_fea],track_data[lat_fea])]

    track_geodata=gpd.GeoDataFrame(track_data,geometry=geometry,crs="EPSG:4326")
    my_log.logger.info(f"访客经纬度数据转化为gdf的部分数据为：\n{track_geodata}")
    gd_data_copy=gd_data[gd_data['country_code']=="1025023"].copy()
    gd_data_copy=gd_data_copy[['派出所','geometry']]
    merged=gpd.sjoin(track_geodata,gd_data_copy,how='left',predicate="within")
    my_log.logger.info(f"根据访客经纬度匹配出派出所的位置的部分数据为：\n{merged[:5]}")
    
    res=merged.drop(columns=merged.geometry.name).copy()
    res=res[['msisdn','city_code','res','remark','派出所']].copy()
    res['派出所']=res['派出所'].fillna("")
    my_log.logger.info(res)
    my_log.logger.info(f"访客常驻匹配出的派出所位置的部分数据为：\n{res[:5]}")

    return res
 

def load_map_information():
    try:
        print("正在读取 江宁 地图中。。。")
        jiangning_map_path=os.path.join(LOCATION_MAP_PATH,"jiangning")
        jiangning_map_path=os.path.join(jiangning_map_path,MAP_NAME)
        map_jiangning_data=gpd.read_file(jiangning_map_path,encoding="latin1")
        print(f"读取 江宁 地图路径为：{jiangning_map_path}")
        print(f"部分 江宁 派出所地图数据为：\n{map_jiangning_data[:5]}")
    except Exception as e:
        print(f"在读取 江宁 地图时出现：{e}！！！")

    try:
        print("正在对 江宁 派出所地域数据进行预处理中。。。")
        jiangning_map=process_jiangning_map_data(map_jiangning_data)
        print("江宁 派出所地域数据预处理完成。。。")
    except Exception as e:
        print(f"在对 江宁 派出所地域数据进行预处理时出现：{e}！！！","error")
        jiangning_map=gpd.GeoDataFrame(columns=['DWBM', '派出所', '所属分局', 'DJD', 'DJDBH', 'geometry','country_code'])



    try:
        print("正在读取 南通 地图中。。。")
        nantong_map_path=os.path.join(LOCATION_MAP_PATH,"nantong")
        nantong_map_path=os.path.join(nantong_map_path,MAP_NAME)
        map_nantong_data=gpd.read_file(nantong_map_path,encoding="latin1")
        print(f"读取 南通 地图路径为：{nantong_map_path}")
        print(f"部分 南通 派出所地图数据为：\n{map_nantong_data[:5]}")
    except Exception as e:
        print(f"在读取 南通 地图时出现：{e}！！！")

    try:
        print("正在对 南通 派出所地域数据进行预处理中。。。")
        nantong_map=process_nantong_map_data(map_nantong_data)
        print("南通 派出所地域数据预处理完成。。。")
    except Exception as e:
        print(f"在对 南通 派出所地域数据进行预处理时出现：{e}！！！","error")
        nantong_map=gpd.GeoDataFrame(columns=['DWBM', '派出所', '所属分局', 'DJD', 'DJDBH', 'geometry','country_code'])


    try:
        print("正在读取 昆山 地图中。。。")
        kunshan_map_path=os.path.join(LOCATION_MAP_PATH,"kunshan")
        kunshan_map_path=os.path.join(kunshan_map_path,MAP_NAME)
        map_kunshan_data=gpd.read_file(kunshan_map_path,encoding="latin1")
        print(f"读取 昆山 地图路径为：{kunshan_map_path}")
        print(f"部分 昆山 派出所地图数据为：\n{map_kunshan_data[:5]}")
    except Exception as e:
        print(f"在读取 昆山 地图时出现：{e}！！！")

    try:
        print("正在对 昆山 派出所地域数据进行预处理中。。。")
        kunshan_map=process_kunshan_map_data(map_kunshan_data)
        print("昆山 派出所地域数据预处理完成。。。")
    except Exception as e:
        print(f"在对 昆山 派出所地域数据进行预处理时出现：{e}！！！","error")
        kunshan_map=gpd.GeoDataFrame(columns=['DWBM', '派出所', '所属分局', 'DJD', 'DJDBH', 'geometry','country_code'])

    try:
        print("正在读取 江阴 地图中。。。")
        jiangyin_map_path=os.path.join(LOCATION_MAP_PATH,"jiangyin")
        jiangyin_map_path=os.path.join(jiangyin_map_path,MAP_NAME)
        map_jiangyin_data=gpd.read_file(jiangyin_map_path,encoding="latin1")
        print(f"读取 江阴 地图路径为：{jiangyin_map_path}")
        print(f"部分 江阴 派出所地图数据为：\n{map_jiangyin_data[:5]}")
    except Exception as e:
        print(f"在读取 江阴 地图时出现：{e}！！！")

    try:
        print("正在对 江阴 派出所地域数据进行预处理中。。。")
        jiangyin_map=process_jiangyin_map_data(map_jiangyin_data)
        print("江阴 派出所地域数据预处理完成。。。")
    except Exception as e:
        print(f"在对 江阴 派出所地域数据进行预处理时出现：{e}！！！","error")
        jiangyin_map=gpd.GeoDataFrame(columns=['DWBM', '派出所', '所属分局', 'DJD', 'DJDBH', 'geometry','country_code'])

    map_data=pd.concat([jiangning_map,nantong_map,kunshan_map,jiangyin_map],axis=0,ignore_index=True)
    return map_data
# 根据访客经纬度去判断所处的派出所
def match_station_main(track_data,output_path,map_data):

    try:
        my_log.logger.info("正在根据访客经纬度判断处于哪一个派出所中。。。")
        res=match_points(track_data,"lng","lat",map_data)
        my_log.logger.info("判断访客所处派出所已经完成。。。")
    except Exception as e:
        my_log.logger.info(f"在根据访客经纬度判断处于哪一个派出所时出现：{e}！！！","error")
    
    try:
        my_log.logger.info("正在导出访客所处派出所的文件中。。。")
        res.to_csv(output_path,index=None)
        my_log.logger.info(f"文件导出的路径为：{output_path}")
    except Exception as e:
        my_log.logger.info(f"在导出访客所处派出所的文件时出现：{e}！！！")

    return 


if __name__=="__main__":
    # track_data=pd.read_csv(r"D:\python\vscode\learn_geopandas\data\test.csv",sep="\t")
    track_data=pd.read_csv(r"D:\python\vscode\learn_geopandas\data\reality_test.csv")
    print(track_data)
    res=match_station_main(track_data,r"D:\python\vscode\learn_geopandas\data\reality_test.csv")


# 输入数据字段 手机号码  地点 风险等级 备注 地点编码（1025023 江宁） lac lng 最终备注

# 输出数据地段 手机号码 地点 风险等级 派出所 最终备注
