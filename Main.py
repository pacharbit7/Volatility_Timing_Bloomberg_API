import blpapi
import pandas as pd
from datetime import datetime
from pandas.tseries.offsets import MonthEnd
from datetime import datetime, timedelta
import yfinance as yf
from sqlalchemy import create_engine

import os
directory = "C:\\Users\\paul-\\OneDrive\\Documents\\M2 Dauphine\\M2\\Bloom API\\Final"
os.chdir(directory)
#os.getcwd()

from backtest import Backtest
from Strategy import Strategy, Range_Based_Vol_Timing
from Performance_Metrics import PerformanceMetrics
from Data_Loader import DataLoader




class DataLoader:
    def __init__(self):
        self.session = blpapi.Session()
        if not self.session.start():
            print("Failed to start session.")
            return

        if not self.session.openService("//blp/refdata"):
            print("Failed to open //blp/refdata")
            return

        self.refDataSvc = self.session.getService('//blp/refdata')
        print('Session open')

    def bds(self, strSecurity, strFields, **overrides):
        request = self.refDataSvc.createRequest('ReferenceDataRequest')

        if isinstance(strFields, str):
            strFields = [strFields]

        if isinstance(strSecurity, str):
            strSecurity = [strSecurity]

        for strD in strFields:
            request.append('fields', strD)

        for strS in strSecurity:
            request.append('securities', strS)

        for overrideField, overrideValue in overrides.items():
            o = request.getElement('overrides').appendElement()
            o.setElement('fieldId', overrideField)
            o.setElement('value', overrideValue)

        self.session.sendRequest(request)

        print("Sending request")

        dict_Security_Fields = {field: {} for field in strFields}

        while True:
            event = self.session.nextEvent()

            if event.eventType() != blpapi.Event.RESPONSE and event.eventType() != blpapi.Event.PARTIAL_RESPONSE:
                continue

            for msg in event:
                securityDataArray = msg.getElement('securityData')

                for sec_data in securityDataArray.values():
                    ticker = sec_data.getElement('security').getValue()
                    fieldDataArray = sec_data.getElement('fieldData')

                    for field in strFields:
                        if fieldDataArray.hasElement(field):
                            field_data = fieldDataArray.getElement(field)
                            dict_Security_Fields[field][ticker] = [field_data.getValue(i) for i in range(field_data.numValues())]

            if event.eventType() == blpapi.Event.RESPONSE:
                break

        return dict_Security_Fields

    def get_historical_index_members(self, index_ticker, date):
        strFields = "INDX_MWEIGHT"
        result = self.bds(index_ticker, strFields, END_DATE_OVERRIDE=date)
        index_members = []

        for ticker, elements in result.get('INDX_MWEIGHT', {}).items():
            for element in elements:
                index_members.append(element.getElementAsString("Member Ticker and Exchange Code"))
        modified_tickers = [ticker[:-2] + ' US Equity' for ticker in index_members]

        return modified_tickers

    def bdh(self, strSecurity, strFields, startdate, enddate, per='DAILY', perAdj='CALENDAR',
            days='NON_TRADING_WEEKDAYS', fill='PREVIOUS_VALUE', curr='USD'):
        request = self.refDataSvc.createRequest('HistoricalDataRequest')

        if isinstance(strFields, str):
            strFields = [strFields]

        if isinstance(strSecurity, str):
            strSecurity = [strSecurity]

        for strF in strFields:
            request.append('fields', strF)

        for strS in strSecurity:
            request.append('securities', strS)

        request.set('startDate', startdate.strftime('%Y%m%d'))
        request.set('endDate', enddate.strftime('%Y%m%d'))
        request.set('periodicitySelection', per)
        request.set('periodicityAdjustment', perAdj)
        request.set('currency', curr)
        request.set('nonTradingDayFillOption', days)
        request.set('nonTradingDayFillMethod', fill)
        requestID = self.session.sendRequest(request)

        print("Sending request")

        dict_Security_Fields = {security: pd.DataFrame(columns=['date'] + strFields) for security in strSecurity}

        while True:
            event = self.session.nextEvent()

            if event.eventType() != blpapi.Event.RESPONSE and event.eventType() != blpapi.Event.PARTIAL_RESPONSE:
                continue

            for msg in event:
                securityData = msg.getElement('securityData')
                ticker = securityData.getElement('security').getValue()
                fieldDataArray = securityData.getElement('fieldData')

                for fieldData in fieldDataArray.values():
                    date = fieldData.getElement('date').getValue()
                    data = {'date': date}

                    for field in strFields:
                        if fieldData.hasElement(field):
                            data[field] = fieldData.getElement(field).getValue()

                    #dict_Security_Fields[ticker] = dict_Security_Fields[ticker].append(data, ignore_index=True)
                    data_df = pd.DataFrame([data])
                    dict_Security_Fields[ticker] = pd.concat([dict_Security_Fields[ticker], data_df], ignore_index=True)

            if event.eventType() == blpapi.Event.RESPONSE:
                break

        return dict_Security_Fields

    def get_historical_data_for_index_members(self, ticker, startdate, enddate, fields):
        historical_data = self.bdh(ticker, fields, startdate, enddate)
        return historical_data

    def closeSession(self):
        print("Session closed")
        self.session.stop()


start_date = datetime(2015, 1, 1)
end_date = datetime(2023, 12, 31)
business_days = pd.date_range(start=start_date, end=end_date, freq='B')

blp = DataLoader()

members_data_dict = {}

for date in business_days:
    date_str = date.strftime("%Y%m%d")
    members = blp.get_historical_index_members("SPX Index", date_str)
    members_data_dict[date] = members
    
members_data_set = set()
for array in members_data_dict.values():
    for asset in array:
        members_data_set.add(asset)
        
historical_data_dict = {}


for asset in members_data_set:
    historical_data = blp.get_historical_data_for_index_members(asset, start_date, end_date, ["PX_LOW", "PX_HIGH", "PX_LAST"])
    
    # Rename columns for each DataFrame and do not use the date as the index
    for ticker, df in historical_data.items():
        if 'PX_LAST' in df.columns:
            df.rename(columns={'PX_LAST': 'Close'}, inplace=True)
        if 'PX_HIGH' in df.columns:
            df.rename(columns={'PX_HIGH': 'High'}, inplace=True)
        if 'PX_LOW' in df.columns:
            df.rename(columns={'PX_LOW': 'Low'}, inplace=True)
        
        # Add the DataFrame to the historical data dictionary
        historical_data_dict[ticker] = df
        
blp.closeSession()

pd.DataFrame(members_data_dict).to_csv("C:/Users/paul-/OneDrive/Documents/M2 Dauphine/M2/Bloom API/Final/members_data.csv")

# save data in a local database
engine = create_engine('sqlite:///dataframes.db')

for asset, df in historical_data_dict.items():
    df.to_sql(asset, engine, if_exists='replace', index=False)

#####################################

#load memebrs_dict
members_data_dict_ = pd.read_csv("C:/Users/paul-/OneDrive/Documents/M2 Dauphine/M2/Bloom API/Final/members_data.csv").iloc[:,1:].to_dict(orient='list')
members_data_dict_ = {pd.Timestamp(date): values for date, values in members_data_dict_.items()}

members_data_set_ = set()
for array in members_data_dict_.values():
    for asset in array:
        members_data_set_.add(asset)
        
#retrieve the data
loaded_dataframes = {}

for asset in members_data_set_:
    loaded_dataframes[asset] = pd.read_sql(asset, engine)
    dates = loaded_dataframes[asset][['date']]
    loaded_dataframes[asset].drop(columns=['date'], inplace=True)
    loaded_dataframes[asset] = loaded_dataframes[asset].apply(pd.to_numeric, errors='coerce').fillna(0)
    
    
Ptf_idx = dates.groupby([dates['date'].dt.year, dates['date'].dt.month]).last()
spaced_dates = dates[::21]


strat = Range_Based_Vol_Timing()
bk_tester = Backtest(loaded_dataframes) 

weights, V_t, Tr_costs, volume = bk_tester.run_backtest(strat.get_allocation, members_data_dict_, dates)
Ptf = pd.DataFrame({'Value':V_t[1:]}, index = spaced_dates.values.flatten()[1:])

PerformanceMetrics.stat_dashboard(Ptf)

