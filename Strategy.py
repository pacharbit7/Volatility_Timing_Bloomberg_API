import pandas as pd
import numpy as np
import math
from scipy.stats import norm
from typing import Dict, List, Optional
from abc import ABC, abstractmethod

class Strategy(ABC):
    
    def __init__(self, scaling_factor: int = 100, target: float = 0.10, alpha: float = 0.2) -> None:
        self.scaling_factor = scaling_factor
        self.target = target  
        self.alpha = alpha
    
    @abstractmethod
    def get_allocation(self, data: Dict[str, pd.DataFrame], *args, **kwargs) -> List[float]:
        pass

class Range_Based_Vol_Timing(Strategy):
    
    def __init__(self, scaling_factor: int = 100, target: float = 0.10, alpha: float = 0.2) -> None:
        super().__init__(scaling_factor=scaling_factor, target=target, alpha=alpha)
        
    def get_allocation(self, datas: Dict[str, pd.DataFrame], members_data, vol_proxy: str = 'range', *args, **kwargs) -> List[float]:
        """Returns the asset allocation for the next iteration

        Args:
            datas (Dict[str, pd.DataFrame]): The dict of the current assets and prices
            vol_proxy (str): The type of volatility proxy to use ('range' or 'returns')

        Returns:
            List[float]: The new quantity to hold for the next iteration
        """
        vol_dict = {}
        
        for asset_name, df in datas.items():
            if asset_name in members_data:
            
                if vol_proxy == 'range':
                    vol = self.compute_range_based_vol(df) 
                    if vol is not None:
                        vol_dict[asset_name] = vol
                elif vol_proxy == 'returns':
                    vol = self.compute_returns_based_vol(df)
                    if vol is not None:
                        vol_dict[asset_name] = vol
                else:
                    raise ValueError("Invalid vol_proxy value. Use 'range' or 'returns'.")
            
        vols = pd.DataFrame([vol_dict], index=['volatility']).T
        vols.sort_values(by='volatility', inplace=True)
        
        lower_quantile = vols[vols['volatility'] <= vols['volatility'].quantile(0.2)].index.tolist()
        upper_quantile = vols[vols['volatility'] >= vols['volatility'].quantile(0.8)].index.tolist()
        
        if self.Bull_Market(datas, lower_quantile, upper_quantile):
            final_dict = {asset_name: self.scaling_factor * self.target / vol_dict[asset_name] if asset_name in upper_quantile else 0 for asset_name in datas.keys()}
            return [qty for qty in final_dict.values()]
        else:
            final_dict = {asset_name: self.scaling_factor * self.target / vol_dict[asset_name] if asset_name in lower_quantile else 0 for asset_name in datas.keys()}
            return [qty for qty in final_dict.values()]
        
    def compute_range_based_vol(self, df: pd.DataFrame) -> float:
        """Calculate range-based volatility for a given DataFrame"""
        try:
            return (1 / (4 * np.log(2))) * (1 / 21) * sum((np.log(df['High']) - np.log(df['Low']))**2)
        except: 
            return None
            
    
    def compute_returns_based_vol(self, df: pd.DataFrame) -> float:
        """Calculate returns-based volatility for a given DataFrame"""
        try:
            returns = np.log(df['Close']).diff().dropna()
            return np.sqrt((returns**2).mean())
        except:
            return None
    
    def Bull_Market(self, datas: Dict[str, pd.DataFrame], vols_low: List[str], vols_high: List[str]) -> bool:
        rets_low = self.Compute_Ptf_ts(datas, vols_low)
        rets_high = self.Compute_Ptf_ts(datas, vols_high)
        
        slope = rets_high - rets_low
        t_stat = np.mean(slope) / np.std(slope)
        
        if t_stat > norm.ppf(1 - self.alpha):
            return True
        else:
            return False
 
    def Compute_Ptf_ts(self, datas: Dict[str, pd.DataFrame], vol_list: List[str]) -> np.ndarray:
        """Calculate the low/high Ptf returns for the 21 days"""
        ptf = np.zeros(21)
        for asset in vol_list:
            if asset in datas:
                ptf += datas[asset]['Close'].values
        return np.diff(np.log(ptf))
