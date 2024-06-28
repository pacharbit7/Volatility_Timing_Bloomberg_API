# -*- coding: utf-8 -*-
"""
Created on Wed May  8 13:04:48 2024

@author: paul-
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
import numpy.typing as npt
from datetime import datetime


class Backtest:
    
    def __init__(self, 
                 Datas : Dict[str, pd.DataFrame] = None) -> None:
        self.Datas = Datas
        
    def transaction_cost_fun(self, volume_t: float, p_1: float, p_2: float) -> float:
        """
        Args:
            volume_t (float): Trading volume
            p_1 (float): proportionality factor p1 (in percent)
            p_2 (float): minimum fee p2 (in monetary units)
    
        Returns:
            float: The charged cost for the volume at t
            """
        return max(volume_t * p_1, p_2) * (volume_t > 0)
        
        
    def run_backtest(self,
                     allocation_function: Callable[[Dict[str, pd.DataFrame]],
                     List[float]],
                     members_data: Dict[datetime, List[str]],
                     dates,
                     p1: float = 0.0,
                     p2: float = 0.0,
                     universe_dataframe: Optional[pd.DataFrame] = None
                     ) -> Tuple[
                         pd.DataFrame,
                         npt.NDArray[np.float64],
                         npt.NDArray[np.float64]]:
                             
                             quantities = []
                             volumes = []
                             transaction_costs = []
                             transaction_account = []
                             transaction_account_qty = []
                             Ptf = []
                             
                             close_df = pd.DataFrame({key:value['Close'] for key, value in self.Datas.items()}) 
                             
                             
                             for idx, row in close_df.iloc[21:].iterrows():
                                 row_values = np.nan_to_num(row.to_numpy(), nan=0.0)
                                 
                                 if idx % 21 == 0 or idx == close_df.index[-1]:
                                     
                                     datas_period = {key:value[idx-21:idx] for key, value in self.Datas.items()}
                                     
                                     if idx != close_df.index[-1]:
                                         
                                         
                                         quantities.append(allocation_function(datas_period, members_data[dates.iloc[idx][0]]))
                                     else:
                                         quantities.append([0] * len(row))
                                     
                                         
                                     #    
                                     if idx-21 == 0:
                                         
                                         volumes.append(np.array(tuple(map(abs, quantities[-1]))) @ row_values)
                                         
                                     elif idx == close_df.index[-1]:
                                         volumes.append(np.array(tuple(map(abs, quantities[-2]))) @ row_values)
                                         
                                     else:
                                         volumes.append(
                                             np.array(tuple(map(
                                                 abs, np.array(quantities[-1]) - np.array(quantities[-2])))) 
                                             @ row_values
                                             )
                                         
                                     
                                     transaction_costs.append(self.transaction_cost_fun(volumes[-1], p_1 = p1, p_2 = p2))
                                     #
                                     
                                     if (idx > 21 and idx != close_df.index[-1]):
                                         try:
                                             transaction_account.append(
                                                 (np.array(quantities[-1]) - np.array(quantities[-2])) @ row_values)
                                         except:
                                             transaction_account.append(
                                                 (np.array(quantities[-1] - 0) @ row_values))
                                             
                                     else:
                                         transaction_account.append(0)
                                         
                                     if idx == 21:
                                         transaction_account_qty.append(-transaction_costs[-1])
                                         
                                     elif idx == close_df.index[-1]:
                                         net_revenue = (
                                             np.array(quantities[-2]) @ row_values) -transaction_costs[-1]
                                         transaction_account_qty.append(transaction_account_qty[-1] + net_revenue)
                                         
                                     else:
                                         transaction_account_qty.append(
                                             transaction_account_qty[-1]
                                             - transaction_account[-1]
                                             - transaction_costs[-1]
                                             )
                                         
                                     Ptf.append(
                                         np.array(quantities[-1]) @ row_values
                                         + 1 *transaction_account_qty[-1]
                                         )
                                     
                                     self.quantities = quantities
                                     
                             return (
                                 pd.DataFrame(quantities, columns = [f"{asset}" for asset in self.Datas.keys()]),
                                 np.array(Ptf),
                                 np.array(transaction_account),
                                 volumes)
                         
                         
                            
                            

                                         
                                     
                                         
                                         
                                         
                                         
                                         
                                     
                                    
                                         
                                     
                                    
                                     
                             
                             
                                                 
                             
                             
            
    
    
    
   
        
        

            
        
                                
        
        
    
    
    