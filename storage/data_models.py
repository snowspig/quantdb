"""
数据模型模块，定义各种金融数据的结构和处理方法
"""
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, List, Optional
from datetime import datetime

# 基础数据模型
@dataclass
class BaseModel:
    """基础数据模型，所有数据模型的基类"""
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {k: v for k, v in asdict(self).items() if v is not None}


@dataclass
class StockBasic(BaseModel):
    """股票基本信息模型"""
    ts_code: str             # TS代码
    symbol: str              # 股票代码
    name: str                # 股票名称
    list_date: Optional[str] = None   # 上市日期
    exchange: Optional[str] = None    # 交易所


@dataclass
class DailyQuote(BaseModel):
    """日线行情模型"""
    ts_code: str             # TS代码
    trade_date: str          # 交易日期
    open: Optional[float] = None      # 开盘价
    high: Optional[float] = None      # 最高价
    low: Optional[float] = None       # 最低价
    close: Optional[float] = None     # 收盘价
    pre_close: Optional[float] = None # 昨收价
    change: Optional[float] = None    # 涨跌额
    pct_chg: Optional[float] = None   # 涨跌幅
    vol: Optional[float] = None       # 成交量 (手)
    amount: Optional[float] = None    # 成交额 (千元)


@dataclass
class MinuteQuote(BaseModel):
    """分钟行情模型"""
    ts_code: str             # TS代码
    trade_time: str          # 交易时间 (yyyy-mm-dd HH:MM)
    trade_date: str          # 交易日期
    open: Optional[float] = None      # 开盘价
    high: Optional[float] = None      # 最高价
    low: Optional[float] = None       # 最低价
    close: Optional[float] = None     # 收盘价
    vol: Optional[float] = None       # 成交量 (手)
    amount: Optional[float] = None    # 成交额 (千元)
    freq: Optional[str] = None        # 频率


@dataclass
class FinancialStatement(BaseModel):
    """财务报表基础模型"""
    ts_code: str             # TS代码
    end_date: str            # 报告期
    ann_date: Optional[str] = None    # 公告日期
    f_ann_date: Optional[str] = None  # 实际公告日期
    report_type: Optional[str] = None # 报告类型
    comp_type: Optional[str] = None   # 公司类型
    update_flag: Optional[str] = None # 更新标志


@dataclass
class IncomeStatement(FinancialStatement):
    """利润表模型"""
    total_revenue: Optional[float] = None     # 营业总收入
    revenue: Optional[float] = None           # 营业收入
    cogs: Optional[float] = None              # 营业成本
    operate_profit: Optional[float] = None    # 营业利润
    total_profit: Optional[float] = None      # 利润总额
    income_tax: Optional[float] = None        # 所得税
    n_income: Optional[float] = None          # 净利润
    n_income_attr_p: Optional[float] = None   # 归属母公司净利润


@dataclass
class BalanceSheet(FinancialStatement):
    """资产负债表模型"""
    total_assets: Optional[float] = None      # 总资产
    total_cur_assets: Optional[float] = None  # 流动资产合计
    total_non_cur_assets: Optional[float] = None  # 非流动资产合计
    total_liab: Optional[float] = None        # 负债合计
    total_cur_liab: Optional[float] = None    # 流动负债合计
    total_non_cur_liab: Optional[float] = None  # 非流动负债合计
    total_hldr_eqy_inc_min_int: Optional[float] = None  # 股东权益合计
    total_assets_minus_liab: Optional[float] = None     # 资产减负债


@dataclass
class CashFlow(FinancialStatement):
    """现金流量表模型"""
    net_cash_flows_oper_act: Optional[float] = None  # 经营活动现金流量净额
    net_cash_flows_inv_act: Optional[float] = None   # 投资活动现金流量净额
    net_cash_flows_fin_act: Optional[float] = None   # 筹资活动现金流量净额
    net_incr_cash_cash_equ: Optional[float] = None   # 现金净增加额
    cash_cash_equ_beg_period: Optional[float] = None # 期初现金及现金等价物
    cash_cash_equ_end_period: Optional[float] = None # 期末现金及现金等价物


@dataclass
class IndexQuote(BaseModel):
    """指数行情模型"""
    ts_code: str             # TS代码
    trade_date: str          # 交易日期
    open: Optional[float] = None      # 开盘价
    high: Optional[float] = None      # 最高价
    low: Optional[float] = None       # 最低价
    close: Optional[float] = None     # 收盘价
    pre_close: Optional[float] = None # 昨收价
    change: Optional[float] = None    # 涨跌额
    pct_chg: Optional[float] = None   # 涨跌幅
    vol: Optional[float] = None       # 成交量 (手)
    amount: Optional[float] = None    # 成交额 (千元)


# 工厂函数 - 根据API名称创建适当的数据类
def create_model_from_api(api_name: str, data: Dict[str, Any]) -> BaseModel:
    """
    根据API名称创建适当的数据模型实例
    
    Args:
        api_name: API名称
        data: 数据字典
        
    Returns:
        适当类型的数据模型实例
    """
    model_map = {
        'stock_basic': StockBasic,
        'daily': DailyQuote,
        'stk_mins': MinuteQuote,
        'income': IncomeStatement,
        'balancesheet': BalanceSheet,
        'cashflow': CashFlow,
        'index_daily': IndexQuote
    }
    
    model_class = model_map.get(api_name, BaseModel)
    
    # 过滤掉None值和空字符串
    filtered_data = {k: v for k, v in data.items() if v is not None and v != ''}
    
    return model_class(**filtered_data) 