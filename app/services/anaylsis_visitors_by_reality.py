import pandas as pd
from config import TYPE_SET,RISK_TYPE_SET
from .get_history_data import adjust_res_by_history
from core.global_logger import my_log

def rule_model(row,df_anj_dic, df_ai_dic, df_gw_dic):
    if (row['访问网址类型计数表'] =="") | (row['网址类型总访问时长字典']==""):
        my_log.logger.info(f"数据可能存在问题，手机号为：{row['手机号']}")
        return "低危，无"


    remark=""
    is_risk=0
    if row['手机号'] in df_ai_dic:
        remark+=f"【{df_ai_dic[row['手机号']]}】和疑似欺诈号码有通话"
        is_risk=1


    if row['手机号'] in df_gw_dic:
        if remark != "":
            remark+=" "
        remark+=f"【{df_gw_dic[row['手机号']]}】和高危号码有通话"
        is_risk=1


    if row['手机号'] in df_anj_dic:
        if remark != "":
            remark+=" "
        remark+=f"【{df_anj_dic[row['手机号']]}】和涉诈号码有通话"
        is_risk=1


    if row['访问网址次数'] >= 15:
        if remark != "":
            return f"紧急，【{row['手机号']}】关联的手机在短时间内多次浏览【{row['访问网址主要类型']}】类型涉诈网站 其访问此网址类型的访问次数为：{row['访问网址主要类型次数']} 且{remark}。"    
        return f"紧急，【{row['手机号']}】关联的手机在短时间内多次浏览【{row['访问网址主要类型']}】类型涉诈网站 其访问此网址类型的访问次数为：{row['访问网址主要类型次数']}。"    
    


    if row['访问网址类型计数表']['虚假服务诈骗'] > 2:
        if remark != "":
            return f"紧急，【{row['手机号']}】关联的手机短时间内多次访问【虚假服务诈骗】网址 其访问网址次数为：【{row['访问网址类型计数表']['虚假服务诈骗']}】 且{remark}。"
        return f"紧急，【{row['手机号']}】关联的手机短时间内多次访问【虚假服务诈骗】网址 其访问网址次数为：【{row['访问网址类型计数表']['虚假服务诈骗']}】。"


    if row['访问网址类型计数表']['冒充电商客服诈骗'] > 2:
        if remark != "":
            return f"紧急，【{row['手机号']}】关联的手机短时间内多次访问【冒充电商客服诈骗】网址,其访问网址次数为：【{row['访问网址类型计数表']['冒充电商客服诈骗']}】 且{remark}。"
        return f"紧急，【{row['手机号']}】关联的手机短时间内多次访问【冒充电商客服诈骗】网址,其访问网址次数为：【{row['访问网址类型计数表']['冒充电商客服诈骗']}】"


    if row['访问网址类型计数表']['虚假购物诈骗'] > 2:
        if remark != "":
            return f"紧急，【{row['手机号']}】关联的手机短时间内多次访问【虚假购物诈骗】网址,其访问网址次数为：【{row['访问网址类型计数表']['虚假购物诈骗']}】 且{remark}。"
        return f"紧急，【{row['手机号']}】关联的手机短时间内多次访问【虚假购物诈骗】网址,其访问网址次数为：【{row['访问网址类型计数表']['虚假购物诈骗']}】。"


    if row['访问网址类型计数表']['刷单诈骗'] > 2:
        if remark != "":
            return f"紧急，【{row['手机号']}】关联的手机短时间内多次访问【刷单诈骗】网址,其访问网址次数为：【{row['访问网址类型计数表']['刷单诈骗']}】 且{remark}。"
        return f"紧急，【{row['手机号']}】关联的手机短时间内多次访问【刷单诈骗】网址,其访问网址次数为：【{row['访问网址类型计数表']['刷单诈骗']}】。"

    if row['访问网址类型计数表']['客服系统'] > 2:
        if remark != "":
            return f"紧急，【{row['手机号']}】关联的手机短时间内多次访问【客服系统】网址,其访问网址次数为：【{row['访问网址类型计数表']['客服系统']}】 且{remark}。"
        return f"紧急，【{row['手机号']}】关联的手机短时间内多次访问【客服系统】网址,其访问网址次数为：【{row['访问网址类型计数表']['客服系统']}】。"


    if row['访问网址类型计数表']['理财诈骗'] > 2:
        if remark != "":
            return f"紧急，【{row['手机号']}】关联的手机短时间内多次访问【理财诈骗】网址 其访问网址次数为：【{row['访问网址类型计数表']['理财诈骗']}】 且{remark}。"
        return f"紧急，【{row['手机号']}】关联的手机短时间内多次访问【理财诈骗】网址 其访问网址次数为：【{row['访问网址类型计数表']['理财诈骗']}】。"


    if row['访问网址类型计数表']['贷款诈骗'] > 2:
        if remark != "":
            return f"紧急，【{row['手机号']}】关联的手机短时间内多次访问【贷款诈骗】网址 其访问网址次数为：【{row['访问网址类型计数表']['贷款诈骗']}】 且{remark}。"
        return f"紧急，【{row['手机号']}】关联的手机短时间内多次访问【贷款诈骗】网址 其访问网址次数为：【{row['访问网址类型计数表']['贷款诈骗']}】。"

    if max(row['网址类型总访问时长字典']) >=200:
        long_duration_types = {game_type for duration,game_type in row['网址类型总访问时长字典'].items() if duration >= 200}
        
        if not long_duration_types.isdisjoint(RISK_TYPE_SET):
            comment_type=RISK_TYPE_SET & long_duration_types
            result = {val:key for key, val in row['网址类型总访问时长字典'].items() if val in comment_type}
            print(result)
            max_type=max(comment_type,key=result.get)
            print(max_type)
            if remark !="":
                return f"紧急，【{row['手机号']}】关联的手机短时间内长时间访问【{max_type}】网址 其访问时长为：【{result[max_type]}】 其访问该类型网址次数为：【{row['访问网址类型计数表'][max_type]}】 且{remark}。"
            return f"紧急，【{row['手机号']}】关联的手机短时间内长时间访问【{max_type}】网址 其访问时长为：【{result[max_type]}】 其访问该类型网址次数为：【{row['访问网址类型计数表'][max_type]}】。"
        
        
        result = {val:key for key, val in row['网址类型总访问时长字典'].items() if val in long_duration_types}
        max_type=max(long_duration_types,key=result.get)
        if remark !="":
            return f"紧急，【{row['手机号']}】关联的手机短时间内长时间访问【{max_type}】网址 其访问时长为：【{result[max_type]}】 其访问该类型网址次数为：【{row['访问网址类型计数表'][max_type]}】 且{remark}。"
        return f"高危，【{row['手机号']}】关联的手机短时间内长时间访问【{max_type}】网址 其访问时长为：【{result[max_type]}】 其访问该类型网址次数为：【{row['访问网址类型计数表'][max_type]}】。"

    if 5 <= row['访问网址次数'] < 15:
        
        if not row['访问网址类型集合'].isdisjoint({"虚假服务诈骗","公检法","冒充电商客服诈骗","虚假购物诈骗","仿冒政府网站诈骗","刷单诈骗","客服系统","理财诈骗","贷款诈骗"}):
            if remark != "":
                return f"紧急，访问的网址中含有高危网址类型 且【{row['手机号']}】关联的手机在短时间内多次浏览【{row['访问网址主要类型']}】类型涉诈网站 其访问此网址类型的访问次数为：{row['访问网址主要类型次数']} 且{row['访问网址主要类型次数']} 且{remark}。"
            
            return f"紧急，访问的网址中含有高危网址类型 且【{row['手机号']}】关联的手机在短时间内多次浏览【{row['访问网址主要类型']}】类型涉诈网站 其访问此网址类型的访问次数为：{row['访问网址主要类型次数']}。"
        
        if remark != "":
            return f"紧急，【{row['手机号']}】关联的手机在短时间内多次浏览【{row['访问网址主要类型']}】类型涉诈网站 其访问此网址类型的访问次数为：{row['访问网址主要类型次数']} 且{remark}。"
        
        return f"高危，【{row['手机号']}】关联的手机在短时间内多次浏览【{row['访问网址主要类型']}】类型涉诈网站 其访问此网址类型的访问次数为：{row['访问网址主要类型次数']}。"
    
    
    if max(row['网址类型总访问时长字典']) >= 100:
        long_duration_types = {game_type for  duration,game_type in row['网址类型总访问时长字典'].items() if duration >= 100}
        
        if not long_duration_types.isdisjoint(RISK_TYPE_SET):
            comment_type=RISK_TYPE_SET & long_duration_types
            result = {val:key for key, val in row['网址类型总访问时长字典'].items() if val in comment_type}
            max_type=max(comment_type,key=result.get)
            if remark !="":
                return f"紧急，【{row['手机号']}】关联的手机短时间内长时间访问【{max_type}】网址 其访问时长为：【{result[max_type]}】 其访问该类型网址次数为：【{row['访问网址类型计数表'][max_type]}】 且{remark}。"
            return f"高危，【{row['手机号']}】关联的手机短时间内长时间访问【{max_type}】网址 其访问时长为：【{result[max_type]}】 其访问该类型网址次数为：【{row['访问网址类型计数表'][max_type]}】。"
        
        
        result = {val:key for key, val in row['网址类型总访问时长字典'].items() if val in long_duration_types}
        max_type=max(long_duration_types,key=result.get)
        if remark !="":
            return f"紧急，【{row['手机号']}】关联的手机短时间内长时间访问【{max_type}】网址 其访问时长为：【{result[max_type]}】 其访问该类型网址次数为：【{row['访问网址类型计数表'][max_type]}】 且{remark}。"
        return f"中危，【{row['手机号']}】关联的手机短时间内长时间访问【{max_type}】网址 其访问时长为：【{result[max_type]}】 其访问该类型网址次数为：【{row['访问网址类型计数表'][max_type]}】。"



    
    if 2 <= row['访问网址次数'] < 5:
        if remark != "":
            return f"紧急，【{row['手机号']}】关联的手机短时间内访问网址次数较少 其访问网址的主要类型为：【{row['访问网址主要类型']}】 但{remark}。"

        return f"中危，【{row['手机号']}】关联的手机短时间内访问网址次数较少 其访问网址的主要类型为：【{row['访问网址主要类型']}】。"


    if is_risk == 1:
        return f"紧急，{remark}。"
    
    return f"低危，【{row['手机号']}】关联的手机短时间内访问网址次数较少。"


def split_model_res(data,model_res_feature):

    # 使用str.split方法拆分列
    new_cols = data[model_res_feature].str.split('，', expand=True)
    # print(new_cols)
    
    new_cols.columns = [f"{model_res_feature}_res", f"{model_res_feature}_reason"]

    # 将新拆分的列添加到原始数据框
    data = pd.concat([data, new_cols], axis=1)

    data = data.drop(model_res_feature, axis=1)
    # print(data.columns)

    return data

def anaylsis_reality_main(data,df_anj_dic, df_ai_dic, df_gw_dic,history_data,date):
    data_copy=data.copy()
    try:
        my_log.logger.info(f"正在根据规则对实时访客数据进行风险等级分级中。。。")
        data_copy['规则模型结果']=data_copy.apply(lambda row: rule_model(row,df_anj_dic, df_ai_dic, df_gw_dic), axis=1)
        my_log.logger.info(f"根据规则对实时访客数据进行风险等级分级完成，其部分数据为：\n{data_copy['规则模型结果'][:5]}")
    except Exception as e:
        my_log.logger.error(f"在根据规则对实时访客数据进行风险等级分级时出现：{e}！！！")

    data_copy=split_model_res(data_copy,'规则模型结果')
    
    try:
        my_log.logger.info("正在根据历史数据对访客风险等级调整中。。。")
        history_res=adjust_res_by_history(data_copy,history_data,date)
        my_log.logger.info(f"根据历史数据对访客风险等级调整完成，其部分数据为：\n{history_res[:5]}")
    except Exception as e:
        my_log.logger.error(f"在根据历史数据对访客风险等级调整时出现：{e}！！！")
        history_res=pd.DataFrame({"手机号":["error"],"new_res":["error，error"]})

    data_copy=data_copy.merge(history_res,on='手机号',how='left')
    data_copy['历史数据分级']=data_copy['new_res'].fillna("低危，【无历史数据】")    
    data_copy=split_model_res(data_copy,'历史数据分级')
    data_copy['res']=data_copy['历史数据分级_res']


    data_copy['备注']=data_copy['规则模型结果_reason']
    data_copy['风险等级']=data_copy['res']
    res=data_copy[['手机号','地市','区县','风险等级','备注']].copy()
    my_log.logger.info(f"涉诈网址访客风险等级分级完成，其部分数据为：\n{res[:5]}")
    return res


if __name__=="__main__":
    data = pd.read_csv("/data/app/test/reality_test.csv",sep="\t")
    from feature_extraction import feature_extraction_main
    data= feature_extraction_main(data)
    df_anj_dic={}
    df_ai_dic={}
    df_gw_dic={}
    res=anaylsis_reality_main(data,df_anj_dic,df_ai_dic,df_gw_dic)
    print(res)

# 输入字段 手机号 网址类型 二次研判网址类型（可能为空）
# 结果字段 手机号 地点 风险等级 备注 