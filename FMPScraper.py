from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
import requests
import pandas as pd
import time
from datetime import datetime
from sqlalchemy import create_engine
import os 

from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from webdriver_manager.firefox import GeckoDriverManager

driver = webdriver.Firefox(service=Service(GeckoDriverManager().install()))

apis = [os.environ.get('KEYS')]
# Requests Company Ratios from FMP and returns DataFrame 
def get_ratios(ticker,api):    
    r_df = pd.DataFrame(columns=['currentRatio','quickRatio','grossProfitMargin','pretaxProfitMargin','returnOnEquity',
                                'longTermDebtToCapitalization','priceEarningsRatio'])
    r_url = "https://financialmodelingprep.com/api/v3/ratios/{}?limit=40&apikey={}"
    r_data = requests.get(r_url.format(ticker, api)).json()
    time.sleep(2)
    for i in r_data:
        cr = i['currentRatio']
        qr = i['quickRatio']
        gpm = i['grossProfitMargin']
        ppm = i['pretaxProfitMargin']
        roe = i['returnOnEquity']
        ltdptc = i['longTermDebtToCapitalization']
        per = i['priceEarningsRatio']
        pr = i['payoutRatio']
        r_df = r_df.append({'currentRatio':cr,'quickRatio':qr,'grossProfitMargin':gpm,
                            'pretaxProfitMargin':ppm,'returnOnEquity':roe,'longTermDebtToCapitalization': ltdptc,
                            'priceEarningsRatio':per,'payoutRatio':pr},ignore_index=True)
    return(r_df)

# Requests Income Statements from FMP and returns DataFrame        
def get_income_statement(ticker,api):
    is_df = pd.DataFrame(columns= ['symbol','date','revenue','epsdiluted'])
    is_url = 'https://financialmodelingprep.com/api/v3/income-statement/{}?limit=120&apikey={}'
    is_data = requests.get(is_url.format(ticker,api)).json()
    time.sleep(2) 
    for i in is_data:
        sym = i['symbol']
        date = i['date']
        rev = i['revenue']
        eps = i['eps']
        eps_dil= i['epsdiluted']
        is_df = is_df.append({'symbol':sym,'date':date,'revenue':rev,'eps': eps,'epsdiluted':eps_dil},ignore_index=True)
    return(is_df)

#Web Scrapes EPS Growth from CNBC and returns Dataframe
def get_scrape(ticker):    
    try:
        driver = webdriver.Firefox(executable_path='/home/novascott/Documents/Finance-Project/Gecko Driver/geckodriver')
        base_url = 'https://apps.cnbc.com/view.asp?symbol={}&uid=stocks/earnings'
        WebDriverWait(driver,50)
        url = base_url.format(ticker)
        driver.get(url)
        html = driver.page_source
        tables = pd.read_html(html)
        driver.close()
        tab = tables[1]
        Epsg = tab.loc[0,'Next 3-5 yrs EPSGrowth Rate']
        Epsvalue = (Epsg.replace('%',''))
        data = {'NextThreeToFiveYrsEPSGrowthRate':[float(Epsvalue),0.0,0.0,0.0,0.0]}
        df_scrape = pd.DataFrame(data)
        return(df_scrape)

    except (ImportError, IndexError, KeyError, ValueError):
        data = {'NextThreeToFiveYrsEPSGrowthRate':[0.0,0.0,0.0,0.0,0.0]}
        df_scrape = pd.DataFrame(data)
        driver.quit()
        return(df_scrape)

#Compiles All Requests into Single DataFrame
def get_report(ticker,api):
    inc = get_income_statement(ticker,api)
    rat = get_ratios(ticker,api)
    scrape = get_scrape(ticker)
    frames = (inc,rat,scrape)
    df = pd.concat(frames,axis=1)
    df = df.fillna(0)
    return df

#Score Criteria
def get_score(df):
    rev0 = df.at[0,'revenue']
    rev1 = df.at[1,'revenue']
    rev2 = df.at[2,'revenue']
    rev3 = df.at[3,'revenue']
    rev4 = df.at[4,'revenue']

    epsy0 = df.at[0,'eps']
    epsy1 = df.at[1,'eps']
    epsy2 = df.at[2,'eps']
    epsy3 = df.at[3,'eps']
    epsy4 = df.at[4,'eps']

    pry0 = df.at[0,'payoutRatio']
    pry1 = df.at[1,'payoutRatio']
    pry2 = df.at[2,'payoutRatio']
    pry3 = df.at[3,'payoutRatio']
    pry4 = df.at[4,'payoutRatio']

    dpsy0 = epsy0 * pry0
    dpsy1 = epsy1 * pry1
    dpsy2 = epsy2 * pry2
    dpsy3 = epsy3 * pry3
    dpsy4 = epsy4 * pry4  

    depsy0 = df.at[0,'epsdiluted']
    depsy1 = df.at[1,'epsdiluted']
    depsy2 = df.at[2,'epsdiluted']
    depsy3 = df.at[3,'epsdiluted']
    depsy4 = df.at[4,'epsdiluted']

    ptm = df.at[0,'pretaxProfitMargin']

    pm0 = df.at[0,'grossProfitMargin']
    pm1 = df.at[1,'grossProfitMargin']
    pm2 = df.at[2,'grossProfitMargin']

    ltdc = df.at[0,'longTermDebtToCapitalization']

    roe0 = df.at[0,'returnOnEquity']
    roe1 = df.at[1,'returnOnEquity']
    roe2 = df.at[2,'returnOnEquity']

    cr = df.at[0,'currentRatio']

    qr = df.at[0,'quickRatio']

    pe0 = df.at[0,'priceEarningsRatio']
    pe1 = df.at[1,'priceEarningsRatio']
    pe2 = df.at[2,'priceEarningsRatio']
    pe3 = df.at[3,'priceEarningsRatio']
    pe4 = df.at[4,'priceEarningsRatio']
    peAvg = (pe0 + pe1 + pe2 + pe3 + pe4)/5

    epsval = df.at[0,'NextThreeToFiveYrsEPSGrowthRate']

    score = 0

    if (rev0 >= rev1 >= rev2 >= rev3 >= rev4): 
        score+=1

    if (rev0 >= 2 * rev4): 
        score+=1

    if(depsy0 >= depsy1 >= depsy2 >= depsy3 >= depsy4): 
        score+=1

    if (2 * depsy4 <= depsy0):
        score+=1

    if (dpsy0 + dpsy1 + dpsy2 + dpsy3 + dpsy4) > 0:
        score+=1

    if (dpsy0 >= dpsy1 >= dpsy2 >= dpsy3 >= dpsy4): 
        score+=1

    if ptm > .15:
        score+=1

    if (pm0 >= pm1 >= pm2): 
        score+=1

    if (ltdc < 1/3): 
        score+=1

    if (roe0 >= roe1 >= roe2): 
        score+=1

    if (roe0 >= .15): 
        score+=1

    if(cr > 2): 
        score+=1

    if(qr > 1): 
        score+=1

    if (pe0 < peAvg): 
        score+=1

    if(epsval >= 15): 
        score+=1
    return(score)

#Engine Connection to DB
url = os.environ.get("DB_URL")
path = os.environ.get("Path")
engine = create_engine(url)
tickers = pd.read_csv(path)

def populate_stocksinfo(tickers,api):
    try:
        for i in tickers.index:
            if tickers['Score'].at[i] == 'x':
                ticker = tickers['Symbol'].at[i]
                df = get_report(ticker,api)
                df.to_sql('stocksReports',engine, if_exists='append', index=False)
                today = datetime.now()
                current_date = today.strftime("%Y-%d-%m")
                tickers['Date Analyzed'].at[i] = current_date
                try:
                    tickers['Score'].at[i] = get_score(df)
                except KeyError:
                    tickers['Score'].at[i] = 0
                print('Finished : ',ticker)
                os.remove(path)
                tickers.to_csv(path_or_buf=path)
            else:
                pass
    except TypeError:
        print("Calls Remaining : 0")

if __name__ == "__main__":
     for api in apis:
        populate_stocksinfo(tickers,api)
        tickers.to_sql('wb_calc_stockscores',engine, if_exists='replace', index=False)