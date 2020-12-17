# -*- coding: utf-8 -*-


# 数据库配置

_HOST = 'localhost'
_PORT = '3306'
_USERNAME = 'username'
_PASSWORD = 'password'
_DATABASE = 'database'
DB_CONNECT_URL = 'mysql+pymysql://{2}:{3}@{0}:{1}/{4}'.format(
    _HOST,
    _PORT,
    _USERNAME,
    _PASSWORD,
    _DATABASE
)


# 金融市场相关

STOCK_MARKET_CLOSING_TIME = '15:00:00'
FUTURE_MARKET_CLOSING_TIME = '15:00:00'

# 期货市场相关

# FUTURE_CONTRACT_SYMBOL = [
#     # 农产品
#     'C',  # 玉米
#     # 油脂油料
#     'A',  # 豆一
#     'M',  # 豆粕
#     'RM',  # 菜粕
#     'Y',  # 豆油
#     'OI',  # 菜油
#     'P',  # 棕榈油
#     # 软商品
#     'SR',  # 白糖
#     'CF',  # 棉花
#     # 农副产品
#     'JD',  # 鸡蛋
#     'AP',  # 苹果
#     # 煤炭
#     'ZC',  # 动力煤
#     'JM',  # 焦煤
#     'J',  # 焦炭
#     # 黑色金属
#     'I',  # 铁矿石
#     'SM',  # 锰硅
#     'SF',  # 硅铁
#     'RB',  # 螺纹钢
#     'HC',  # 热轧卷板
#     'SS',  # 不锈钢
#     # 有色金属
#     'NI',  # 镍
#     'CU',  # 铜
#     'AL',  # 铝
#     'ZN',  # 锌
#     # 贵金属
#     'AU',  # 金
#     'AG',  # 银
#     # 原油
#     'SC',  # 原油
#     'BU',  # 沥青
#     'FU',  # 燃料油
#     'LU',  # 低硫燃料油
#     'PG',  # 液化气
#     # 化工
#     'MA',  # 甲醇
#     'EG',  # 乙二醇
#     'TA',  # PTA
#     'PF',  # 短纤
#     'EB',  # 苯乙烯
#     'L',  # 聚乙烯
#     'V',  # 聚氯乙烯
#     'PP',  # 聚丙烯
#     'RU',  # 橡胶
#     'SA',  # 纯碱
#     # 轻工
#     'FG',  # 玻璃
#     'SP',  # 纸浆
#     # 股指
#     # 'IH',  # 上证50指数
#     # 'IF',  # 沪深300指数
#     # 'IC',  # 中证500指数
#     # 国债
#     # 'TS',  # 二债
#     # 'TF',  # 五债
#     # 'T',  # 十债
# ]
