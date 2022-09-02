import blpapi
import pandas as pd
import numpy as np

session = blpapi.Session()
# define the session (use the default seesion in blpapi library)
try:
    session.start()
except:
    print("session failed")

session.openService('//blp/refdata')
service = session.getService('//blp/refdata')
request = service.createRequest("HistoricalDataRequest")

#这一部分开始需要用户自定义修改

ar = []  #ARray for efficient data processing
security_data = {}  #dictionary to hold single line of data

ticker_list = ['7203 JP Equity','IBM US Equity']
for ticker in ticker_list:
    request.getElement("securities").appendValue(ticker)

flds_list = ['PX_LAST','PX_VOLUME','PE_RATIO']
for flds in flds_list:
    request.getElement("fields").appendValue(flds)

request.set("startDate","20200301")
request.set("endDate","20200401")

#添加数据栏目的栏目改写field overrides, 请参考FLDS中相应字段的overrides设置
overrides = request.getElement("overrides")

override1 = overrides.appendElement()
overrideField1 = "FUND_PER"
overrideValue1 = "Y"

override1.setElement("fieldId", overrideField1)
override1.setElement("value", overrideValue1) 


#这一部分是历史数据请求的固有参数Parameters，主要用来调整数据的频率，改写货币等。完整的参数列表请参考API Core Developer Guide文档92页

request.set("nonTradingDayFillOption","ALL_CALENDAR_DAYS")
request.set("calendarCodeOverride","5D")
request.set("nonTradingDayFillMethod","NIL_VALUE")
request.set("periodicityAdjustment","ACTUAL")
request.set("periodicitySelection","DAILY")


session.sendRequest(request)


while(True):
    ev = session.nextEvent()
    if ev.eventType() == blpapi.Event.RESPONSE or ev.eventType() ==blpapi.Event.PARTIAL_RESPONSE:
        for msg in ev:  
            securityData = msg.getElement("securityData")
            security = securityData.getElement("security").getValue()

            fieldData=securityData.getElement("fieldData")
            for i in range(0,fieldData.numValues()):
                security_data  = {element.toString().split()[0] :element.getValue() for element in fieldData.getValue(i).elements()}
                security_data['ID'] = security
                ar.append(security_data) 
                
                
    if ev.eventType() == blpapi.Event.RESPONSE:
        break

session.stop()
df=pd.DataFrame(ar).set_index(['ID','date'])
df
