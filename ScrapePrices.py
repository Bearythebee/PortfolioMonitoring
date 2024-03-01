import requests
import json
import pprint
from misc.CustomLogging import Logger, log_fuction
from datetime import datetime
import pytz

# Set logging infomation
current_date = datetime.now(pytz.timezone('Asia/Singapore')).strftime('%Y%m%d')
logfilename = f"logfiles/ScrapePrices/{current_date}.log"
loggerobj  = Logger('ScrapePrices',stream='',file=[logfilename])


@log_fuction(logger=loggerobj)
def get_prices_full(security_id):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{security_id}?interval=1d&range=30y"
    headers = {
        "Access-Control-Allow-Origin":"https://sg.finance.yahoo.com",
        "Content-Type":"application/json;charset=utf-8",
        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"\
                    " AppleWebKit/537.36 (KHTML, like Gecko)"\
                    " Chrome/121.0.0.0 Safari/537.36"
        }
    response = requests.get(url,headers=headers)
    try:
        data = json.loads(response.text)['chart']['result'][0]
        return data
    except:
        print(f"NO data for {security_id}")

@log_fuction(logger=loggerobj)
def get_prices_daily(security_id):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{security_id}?interval=1d&range=5d"
    headers = {
        "Access-Control-Allow-Origin":"https://sg.finance.yahoo.com",
        "Content-Type":"application/json;charset=utf-8",
        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"\
                    " AppleWebKit/537.36 (KHTML, like Gecko)"\
                    " Chrome/121.0.0.0 Safari/537.36"
        }
    response = requests.get(url,headers=headers)
    try:
        data = json.loads(response.text)['chart']['result'][0]
        return data
    except:
        print(f"NO data for {security_id}")
# # url = 'https://api.investing.com/api/financialdata/38423/historical/chart/?period=MAX&interval=PT1M'
# url = "https://api.investing.com/api/financialdata/historical/38423?start-date=2024-01-02&end-date=2024-02-13&time-frame=Daily&add-missing-rows=false"
# headers = {
#     "Access-Control-Allow-Origin":"https://www.investing.com",
#     "Content-Type":"application/json;charset=utf-8",
#     "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"\
#                 " AppleWebKit/537.36 (KHTML, like Gecko)"\
#                 " Chrome/121.0.0.0 Safari/537.36"}

# cookies = {
#     "__cf_bm" : "8QyVpHFRhelr5CCKWk.rIMzFX8qligmknBIQ7r_FY08-1707809324-1-Ae++WcnMvFgnnj5SRQFuxtlVjfmfOaPAHQ2cCZwwXl3j74QB4ujJqSFc83poSVc0QzaVgcADmkUrkeqaA0JjMPkWZqywtB0622Jx8/ydDpNh; path=/; expires=Tue, 13-Feb-24 07:58:44 GMT; domain=.investing.com; HttpOnly; Secure; SameSite=None",
#     "__cflb" : "02DiuEaBtsFfH7bEbN4qQwLpwTUxNYEGyKxXU35xEkSex; SameSite=None; Secure; path=/; expires=Wed, 14-Feb-24 06:28:44 GMT; HttpOnly"
# }

# response = requests.get(url,headers=headers,cookies=cookies)
# print(response.text)
# # try:
# #     data = json.loads(response.text)['chart']['result'][0]
# #     return data
# # except:
# #     print(f"NO data for {security_id}")