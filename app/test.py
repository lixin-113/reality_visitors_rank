import pandas as pd



data=pd.read_csv("/data/app/input/test.csv",sep="\t",dtype={"phone":str,"lng":str})
data['new']=data['phone']+"+"+data['lng']
print(data['new'])