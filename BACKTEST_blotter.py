

from interactive_trader.synchronous_functions import *

#%% RUN Backtest
from interactive_trader.synchronous_functions import backtest

blotter = backtest('GS.csv','MS.csv',100000,'GS','MS')