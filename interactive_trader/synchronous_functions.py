
from interactive_trader.ibkr_app import ibkr_app
import threading
import time
from datetime import datetime
import statsmodels as sm
import pandas as pd
import numpy as np
import math
# If you want different default values, configure it here.
default_hostname = '127.0.0.1'
default_port = 7497
default_client_id = 10645 # can set and use your Master Client ID
timeout_sec = 5

def fetch_managed_accounts(hostname=default_hostname, port=default_port,
                           client_id=default_client_id):

    app = ibkr_app()
    app.connect(hostname, int(port), int(client_id))

    start_time = datetime.now()
    while not app.isConnected():
        time.sleep(0.01)
        if (datetime.now() - start_time).seconds > timeout_sec:
            app.disconnect()
            raise Exception(
                "fetch_managed_accounts",
                "timeout",
                "couldn't connect to IBKR"
            )

    def run_loop():
        app.run()

    api_thread = threading.Thread(target=run_loop, daemon=True)
    api_thread.start()
    while app.next_valid_id is None:
        time.sleep(0.01)
    app.disconnect()
    return app.managed_accounts

def fetch_current_time(hostname=default_hostname,
                       port=default_port, client_id=default_client_id):
    app = ibkr_app()
    app.connect(hostname, int(port), int(client_id))
    start_time = datetime.now()
    while not app.isConnected():
        time.sleep(0.01)
        if (datetime.now() - start_time).seconds > timeout_sec:
            app.disconnect()
            raise Exception(
                "fetch_current_time",
                "timeout",
                "couldn't connect to IBKR"
            )

    def run_loop():
        app.run()

    api_thread = threading.Thread(target=run_loop, daemon=True)
    api_thread.start()
    start_time = datetime.now()
    while app.next_valid_id is None:
        time.sleep(0.01)
        if (datetime.now() - start_time).seconds > timeout_sec:
            app.disconnect()
            raise Exception(
                "fetch_current_time",
                "timeout",
                "next_valid_id not received"
            )

    app.reqCurrentTime()
    start_time = datetime.now()
    while app.current_time is None:
        time.sleep(0.01)
        if (datetime.now() - start_time).seconds > timeout_sec:
            app.disconnect()
            raise Exception(
                "fetch_current_time",
                "timeout",
                "current_time not received"
            )
    app.disconnect()
    return app.current_time


def fetch_historical_data(contract, endDateTime='', durationStr='30 D',
                          barSizeSetting='1 hour', whatToShow='MIDPOINT',
                          useRTH=True, hostname=default_hostname,
                          port=default_port, client_id=default_client_id):
    app = ibkr_app()
    app.connect(hostname, int(port), int(client_id))
    start_time = datetime.now()
    while not app.isConnected():
        time.sleep(0.01)
    if (datetime.now() - start_time).seconds > timeout_sec:
        app.disconnect()
        raise Exception(
            "fetch_historical_data",
            "timeout",
            "couldn't connect to IBKR"
        )

    def run_loop():
        app.run()
    api_thread = threading.Thread(target=run_loop, daemon=True)
    api_thread.start()
    start_time = datetime.now()
    while app.next_valid_id is None:
        time.sleep(0.01)
        if (datetime.now() - start_time).seconds > timeout_sec:
            app.disconnect()
            raise Exception(
                "fetch_historical_data",
                "timeout",
                "next_valid_id not received"
            )
    tickerId = app.next_valid_id
    app.reqHistoricalData(
        tickerId, contract, endDateTime, durationStr, barSizeSetting,
        whatToShow, useRTH, formatDate=1, keepUpToDate=False, chartOptions=[])
    start_time = datetime.now()
    while app.historical_data_end != tickerId:
        time.sleep(0.01)
        if (datetime.now() - start_time).seconds > timeout_sec:
            app.disconnect()
            raise Exception(
                "fetch_historical_data",
                "timeout",
                "historical_data not received"
            )
    app.disconnect()
    return app.historical_data

def fetch_contract_details(contract, hostname=default_hostname,
                           port=default_port, client_id=default_client_id):
    app = ibkr_app()
    app.connect(hostname, int(port), int(client_id))
    start_time = datetime.now()
    while not app.isConnected():
        time.sleep(0.01)
        if (datetime.now() - start_time).seconds > timeout_sec:
            app.disconnect()
            raise Exception(
                "fetch_contract_details",
                "timeout",
                "couldn't connect to IBKR"
            )

    def run_loop():
        app.run()

    api_thread = threading.Thread(target=run_loop, daemon=True)
    api_thread.start()
    start_time = datetime.now()
    while app.next_valid_id is None:
        time.sleep(0.01)
        if (datetime.now() - start_time).seconds > timeout_sec:
            app.disconnect()
            raise Exception(
                "fetch_contract_details",
                "timeout",
                "next_valid_id not received"
            )

    tickerId = app.next_valid_id
    app.reqContractDetails(tickerId, contract)

    start_time = datetime.now()
    while app.contract_details_end != tickerId:
        time.sleep(0.01)
        if (datetime.now() - start_time).seconds > timeout_sec:
            app.disconnect()
            raise Exception(
                "fetch_contract_details",
                "timeout",
                "contract_details not received"
            )

    app.disconnect()

    return app.contract_details

def fetch_matching_symbols(pattern, hostname=default_hostname,
                           port=default_port, client_id=default_client_id):
    app = ibkr_app()
    app.connect(hostname, int(port), int(client_id))
    start_time = datetime.now()
    while not app.isConnected():
        time.sleep(0.01)
        if (datetime.now() - start_time).seconds > timeout_sec:
            app.disconnect()
            raise Exception(
                "fetch_contract_details",
                "timeout",
                "couldn't connect to IBKR"
            )

    def run_loop():
        app.run()

    api_thread = threading.Thread(target=run_loop, daemon=True)
    api_thread.start()
    start_time = datetime.now()
    while app.next_valid_id is None:
        time.sleep(0.01)
        if (datetime.now() - start_time).seconds > timeout_sec:
            app.disconnect()
            raise Exception(
                "fetch_contract_details",
                "timeout",
                "next_valid_id not received"
            )

    req_id = app.next_valid_id
    app.reqMatchingSymbols(req_id, pattern)

    start_time = datetime.now()
    while app.matching_symbols is None:
        time.sleep(0.01)
        if (datetime.now() - start_time).seconds > timeout_sec:
            app.disconnect()
            raise Exception(
                "fetch_contract_details",
                "timeout",
                "contract_details not received"
            )

    app.disconnect()

    return app.matching_symbols

def place_order(contract, order, hostname=default_hostname,
                           port=default_port, client_id=default_client_id):

    app = ibkr_app()
    app.connect(hostname, port, client_id)
    while not app.isConnected():
        time.sleep(0.01)

    def run_loop():
        app.run()

    api_thread = threading.Thread(target=run_loop, daemon=True)
    api_thread.start()

    while app.next_valid_id is None:
        time.sleep(0.01)

    app.placeOrder(app.next_valid_id, contract, order)
    while not ('Submitted' in set(app.order_status['status'])):
        time.sleep(0.25)

    app.disconnect()

    return app.order_status

def get_log_return(stock): #takes in pandas dataframe
    stock['Log_Price'] = stock['Close'].apply(lambda x: math.log(x))
    stock['Log_Diff'] = stock['Log_Price'].diff()
    return stock

def entry_signal(stock1, stock2, stock1_symbl, stock2_symbl, position_taken, threshold, allocation): # takes in pd dataframe # position taken is a boolean yes or no
    stock1_window = stock1['Log_Price'][-91:-1]
    stock2_window = stock2['Log_Price'][-91:-1]
    stock1_window = sm.add_constant(stock1_window) # need to import statsmodels as sm
    model = sm.OLS(stock2_window, stock1_window) #OLS(y,x)
    results = model.fit()
    sigma = math.sqrt(results.mse_resid)  # standard deviation of the residual
    slope = results.params[1]
    intercept = results.params[0]
    res = results.resid  # regression residual mean of res =0 by definition
    zscore_1_2 = res / sigma
    position_taken = position_taken
    spread = pd.concat([res, zscore_1_2], axis=1)
    spread = pd.concat([stock2_window, spread], axis=1)
    spread = pd.concat([stock1_window, spread], axis=1)
    spread.columns = ['const', 'stock1_log', 'stock2_log', 'residual', 'zscore']
    spread['date'] = stock1['Date']

    stock1_wt = slope/(1+slope)
    stock2_wt =  1/(1+slope)
    status = 'Filled'
    if position_taken == 0 and spread['zscore'].values[-1] > sigma *threshold and spread['zscore'].values[-2] < sigma * threshold:
        size1 = np.round((stock1_wt * allocation * stock1['Open'].values[-1]))
        size2 = np.round((stock2_wt * allocation *  stock2['Open'].values[-1]))
        stock1_order = [stock1['Date'][-1],status,'ENTRY',stock1_symbl,stock1['Open'].values[-1],'BUY',size1 ] # order = signal (buy or sell) , (# of shares)
        stock2_order = [stock1['Date'][-1],status,'ENTRY',stock2_symbl,stock2['Open'].values[-1],'SELL', size2]
        position_taken = 1 # Long Spread
        df = pd.DataFrame(stock1_order)
        df = df.append(stock2_order)

    elif position_taken == 0 and spread['zscore'].values[-1] < -sigma *threshold and spread['zscore'].values[-2] > -sigma * threshold:
        size1 = np.round((stock1_wt * allocation * stock1['Open'].values[-1]))
        size2 = np.round((stock2_wt * allocation * stock2['Open'].values[-1]))
        stock1_order = [stock1['Date'][-1],status,'ENTRY',stock1_symbl,stock1['Open'].values[-1],'SELL', size1]
        stock2_order = [stock1['Date'][-1],status,'ENTRY',stock2_symbl,stock2['Open'].values[-1],'BUY', size2]
        position_taken = -1 # Short Spread
        df = pd.DataFrame(stock1_order)
        df = df.append(stock2_order)
    else:
        df = 0
        position_taken = 0 # represents that there is no position taken


    return [df, position_taken, spread]


def exit_signal(stock1,stock2,stock1_symbl,stock2_symbl,position_taken,blotter,spread):
    status = 'FILLED'
    position_taken = position_taken
    if position_taken == 1 and spread['zscore'].values[-1] < 0:
        size1 = blotter['Size'][-2]
        size2 = blotter['Size'][-1]
        stock1_order = [stock1['Date'][-1], status, 'EXIT', stock1_symbl, stock1['Open'].values[-1], 'BUY', size1]
        stock2_order = [stock1['Date'][-1], status, 'EXIT', stock2_symbl, stock2['Open'].values[-1], 'SELL', size2]
        df = pd.DataFrame(stock1_order)
        df = df.append(stock2_order)
        position_taken = 0


    elif position_taken == -1 and spread['zscore'].values[-1] > 0:
        size1 = blotter['Size'][-2]
        size2 = blotter['Size'][-1]
        stock1_order = [stock1['Date'][-1], status, 'EXIT', stock1_symbl, stock1['Open'].values[-1], 'BUY', size1]
        stock2_order = [stock1['Date'][-1], status, 'EXIT', stock2_symbl, stock2['Open'].values[-1], 'SELL', size2]
        df = pd.DataFrame(stock1_order)
        df = df.append(stock2_order)
        position_taken = 0
    else:
        df = 0
    return [df,position_taken]




def backtest(csv1,csv2, allocation, stock1_symbl, stock2_symbl):
    s1 = get_log_return(pd.read_csv(csv1))
    s2 = get_log_return(pd.read_csv(csv2))
    blotter = pd.DataFrame(columns = ['Date','Status','Trip','Symbol','Price','Action','Size'])
    position_taken = 0
    threshold = 1
    N = len(s1)
    spread = 0
    for i in range(90,N+1):
        stock1 = s1[0:i]
        stock2 = s2[0:i]
        output = entry_signal(stock1, stock2, stock1_symbl, stock2_symbl, position_taken, threshold, allocation)
        spread = output[2]
        if output[0] != 0:
            blotter = blotter.append(output[0])
            position_taken = output[1]

        if position_taken != 0:
            output = exit_signal(stock1,stock2,stock1_symbl,stock2_symbl,position_taken,blotter,spread)
            if output[0] != 0:
                blotter = blotter.append(output[0])
                position_taken = output[1]

    return [blotter]





