# -*- coding: utf-8 -*-
"""
Created on Sat Jun 15 16:13:24 2024

@author: paul-
"""
import blpapi
import pandas as pd

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