# -*- coding: utf-8 -*-

import datetime
import json
import requests
import rqdatac
import pandas
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column
from sqlalchemy import Date
from sqlalchemy import DateTime
from sqlalchemy import DECIMAL
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import UniqueConstraint
from sqlalchemy import Index
from sqlalchemy import and_
from sqlalchemy import asc
from sqlalchemy import desc
from constants import DB_CONNECT_URL


_engine = create_engine(
    DB_CONNECT_URL,
    pool_pre_ping=True,
    pool_size=20,
    pool_recycle=3600,
    pool_timeout=15
)
_session_factory = sessionmaker(bind=_engine)

_baseObject = declarative_base()


# 基础数据

class BasisCalendar(_baseObject):

    '''
        CREATE TABLE t_basic_calendar
          (
             date_id    INT(10) NOT NULL AUTO_INCREMENT,
             date_name  VARCHAR(10) NOT NULL,
             year_name  VARCHAR(4) NOT NULL,
             month_name VARCHAR(2) NOT NULL,
             is_trading VARCHAR(1) NOT NULL DEFAULT 'N',
             CONSTRAINT pk_basic_calendar PRIMARY KEY (date_id),
             CONSTRAINT unq_basic_calendar UNIQUE (date_name),
             INDEX idx_basic_calendar_is_trading (is_trading)
          )
        ENGINE=innodb DEFAULT CHARSET=utf8;
    '''

    __tablename__ = 't_basic_calendar'

    date_id = Column(Integer, nullable=False, autoincrement=True)
    date_name = Column(String(10), nullable=False)
    year_name = Column(String(4), nullable=False)
    month_name = Column(String(2), nullable=False)
    is_trading = Column(String(1), nullable=False, default='N')

    __table_args__ = (
        PrimaryKeyConstraint(
            'date_id',
            name='pk_basic_calendar'
        ),
        UniqueConstraint(
            'date_name',
            name='unq_basic_calendar'
        ),
        Index(
            'idx_basic_calendar_is_trading',
            'is_trading'
        ),
    )


class BasisCalendarManager(object):

    @staticmethod
    def store(date_name, is_trading):
        session = scoped_session(_session_factory)
        try:
            obj = session.query(BasisCalendar).filter(
                BasisCalendar.date_name == date_name
            ).first()
            if obj is None:
                obj = BasisCalendar(
                    date_name=date_name,
                    year_name=date_name[0:4],
                    month_name=date_name[5:7],
                    is_trading=is_trading
                )
            else:
                obj.is_trading = is_trading
            session.merge(obj)
            session.commit()
        except BaseException:
            session.rollback()
            raise
        finally:
            session.close()


class BasisTradingDateView(_baseObject):

    '''
        CREATE view v_basic_trading_date
        AS
          SELECT date_name,
                 year_name,
                 month_name
          FROM   t_basic_calendar
          WHERE  is_trading = 'Y'
          ORDER  BY date_name;
    '''

    __tablename__ = 'v_basic_trading_date'

    date_name = Column(String(10), nullable=False)
    year_name = Column(String(4), nullable=False)
    month_name = Column(String(2), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint(
            'date_name',
            name='pk_v_basic_trading_date'
        ),
    )


class BasisTradingDateViewManager(object):

    @staticmethod
    def get_trading_dates(start_date, end_date):
        trading_dates = []

        session = scoped_session(_session_factory)
        try:
            objs = session.query(BasisTradingDateView).filter(
                and_(
                    BasisTradingDateView.date_name >= start_date,
                    BasisTradingDateView.date_name <= end_date
                )
            ).order_by(
                asc(BasisTradingDateView.date_name)
            ).all()
            for obj in objs:
                trading_dates.append(obj.date_name)
        finally:
            session.close()

        return trading_dates

    @staticmethod
    def get_previous_trading_date(date, n):
        previous_trading_date = None

        session = scoped_session(_session_factory)
        try:
            objs = session.query(BasisTradingDateView).filter(
                BasisTradingDateView.date_name < date
            ).order_by(
                desc(BasisTradingDateView.date_name)
            ).limit(n).all()
            if objs:
                previous_trading_date = objs[-1].date_name
        finally:
            session.close()

        return previous_trading_date

    @staticmethod
    def get_next_trading_date(date, n):
        next_trading_date = None

        session = scoped_session(_session_factory)
        try:
            objs = session.query(BasisTradingDateView).filter(
                BasisTradingDateView.date_name > date
            ).order_by(
                asc(BasisTradingDateView.date_name)
            ).limit(n).all()
            if objs:
                next_trading_date = objs[-1].date_name
        finally:
            session.close()

        return next_trading_date

    @staticmethod
    def get_latest_trading_date():
        latest_trading_date = None

        session = scoped_session(_session_factory)
        try:
            today = datetime.date.today().strftime('%Y-%m-%d')
            obj = session.query(BasisTradingDateView).filter(
                BasisTradingDateView.date_name <= today
            ).order_by(
                desc(BasisTradingDateView.date_name)
            ).limit(1).first()
            if obj is not None:
                latest_trading_date = obj.date_name
        finally:
            session.close()

        return latest_trading_date


# 基金数据

class FundScale(_baseObject):

    '''
        CREATE TABLE t_fund_scale
          (
             fund_code    VARCHAR(10) NOT NULL,
             trading_date DATE NOT NULL,
             total_units  DECIMAL(20, 2) NOT NULL,
             CONSTRAINT pk_fund_scale PRIMARY KEY (fund_code, trading_date)
          )
        ENGINE=innodb DEFAULT CHARSET=utf8;
    '''

    __tablename__ = 't_fund_scale'

    fund_code = Column(String(10), nullable=False)
    trading_date = Column(Date, nullable=False)
    total_units = Column(DECIMAL(20, 2), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint(
            'fund_code',
            'trading_date',
            name='pk_fund_scale'
        ),
    )


class FundScaleManager(object):

    @staticmethod
    def fetch_sse_fund_scale(code, date):
        headers = {
            'Referer': 'http://www.sse.com.cn/assortment/fund/list/etfinfo/scale/index.shtml?FUNDID=' + code,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0'}
        params = {
            'isPagination': 'false',
            'sqlId': 'COMMON_SSE_ZQPZ_ETFZL_ETFJBXX_JJGM_SEARCH_L',
            'SEC_CODE': code,
            'STAT_DATE': date
        }
        url = 'http://query.sse.com.cn/commonQuery.do'
        r = requests.get(url, params=params, headers=headers)
        if r.status_code == 200:
            j = json.loads(r.text)
            return j['result']

    @staticmethod
    def store(code, date, units):
        session = scoped_session(_session_factory)
        try:
            session.merge(
                FundScale(
                    fund_code=code,
                    trading_date=date,
                    total_units=units
                )
            )
            session.commit()
        except BaseException:
            session.rollback()
            raise
        finally:
            session.close()


# 期货数据

class FutureContractSymbol(_baseObject):

    '''
        CREATE TABLE t_future_contract_symbol
          (
             contract_symbol VARCHAR(10) NOT NULL,
             symbol_name     VARCHAR(10) NOT NULL,
             tick_size       VARCHAR(20) NOT NULL,
             trading_units   VARCHAR(20) NOT NULL,
             display_order   INT(10) NOT NULL,
             CONSTRAINT pk_future_contract_symbol PRIMARY KEY (contract_symbol)
          )
        ENGINE=innodb DEFAULT CHARSET=utf8;
    '''

    __tablename__ = 't_future_contract_symbol'

    contract_symbol = Column(String(10), nullable=False)
    symbol_name = Column(String(10), nullable=False)
    tick_size = Column(String(20), nullable=False)
    trading_units = Column(String(20), nullable=False)
    display_order = Column(Integer, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint(
            'contract_symbol',
            name='pk_future_contract_symbol'
        ),
    )


class FutureContractSymbolManager(object):

    @staticmethod
    def get_contract_symbols():
        contract_symbols = []

        session = scoped_session(_session_factory)
        try:
            objs = session.query(FutureContractSymbol).order_by(
                asc(FutureContractSymbol.display_order)
            ).all()
            for obj in objs:
                contract_symbols.append(obj.contract_symbol)
        finally:
            session.close()

        return contract_symbols


class FutureMainContract(_baseObject):

    '''
        CREATE TABLE t_future_main_contract
          (
             contract_code   VARCHAR(10) NOT NULL,
             contract_symbol VARCHAR(10) NOT NULL,
             start_date      DATE NOT NULL,
             end_date        DATE NOT NULL,
             CONSTRAINT pk_future_main_contract PRIMARY KEY (contract_code),
             INDEX idx_future_main_contract (start_date, end_date)
          )
        ENGINE=InnoDB DEFAULT CHARSET=utf8;
    '''

    __tablename__ = 't_future_main_contract'

    contract_code = Column(String(10), nullable=False)
    contract_symbol = Column(String(10), nullable=False)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint(
            'contract_code',
            name='pk_future_main_contract'
        ),
        Index(
            'idx_future_main_contract',
            'contract_symbol',
            'start_date',
            'end_date'
        ),
    )


class FutureMainContractManager(object):

    @staticmethod
    def store(contract_code, contract_symbol, trading_date):
        session = scoped_session(_session_factory)
        try:
            obj = session.query(FutureMainContract).filter(
                FutureMainContract.contract_code == contract_code
            ).first()
            if obj is None:
                obj = FutureMainContract(
                    contract_code=contract_code,
                    contract_symbol=contract_symbol,
                    start_date=trading_date,
                    end_date=trading_date
                )
            else:
                obj.end_date = trading_date
            session.merge(obj)
            session.commit()
        except BaseException:
            session.rollback()
            raise
        finally:
            session.close()

    @staticmethod
    def get_main_contracts_by_trading_date(trading_date):
        main_contracts = []

        session = scoped_session(_session_factory)
        try:
            objs = session.query(FutureMainContract).filter(
                and_(
                    trading_date >= FutureMainContract.start_date,
                    trading_date <= FutureMainContract.end_date
                )
            ).all()
            for obj in objs:
                main_contracts.append(obj.contract_code)
        finally:
            session.close()

        return main_contracts

    @staticmethod
    def get_main_contracts_by_switch_date(trading_date):
        main_contracts = []

        session = scoped_session(_session_factory)
        try:
            objs = session.query(FutureMainContract).filter(
                FutureMainContract.start_date == trading_date
            ).all()
            for obj in objs:
                main_contracts.append(obj.contract_code)
        finally:
            session.close()

        return main_contracts


class FutureLatestMainContractView(_baseObject):

    '''
        CREATE view v_future_latest_main_contract
        AS
          SELECT t1.contract_symbol,
                 t1.symbol_name,
                 t3.contract_code,
                 t1.tick_size,
                 t1.trading_units,
                 t3.start_date,
                 t3.end_date,
                 t1.display_order
          FROM   t_future_contract_symbol t1
                 LEFT JOIN (SELECT contract_symbol,
                                   Max(start_date) AS max_start_date
                            FROM   t_future_main_contract
                            GROUP  BY contract_symbol) t2
                        ON t1.contract_symbol = t2.contract_symbol
                 LEFT JOIN t_future_main_contract t3
                        ON t2.contract_symbol = t3.contract_symbol
                           AND t2.max_start_date = t3.start_date
          ORDER  BY t1.display_order;
    '''

    __tablename__ = 'v_future_latest_main_contract'

    contract_symbol = Column(String(10), nullable=False)
    symbol_name = Column(String(10), nullable=False)
    contract_code = Column(String(10), nullable=False)
    tick_size = Column(String(20), nullable=False)
    trading_units = Column(String(20), nullable=False)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)
    display_order = Column(Integer, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint(
            'contract_symbol',
            name='pk_v_future_latest_main_contract'
        ),
    )


class FutureLatestMainContractViewManager(object):

    @staticmethod
    def get_latest_main_contracts():
        latest_main_contracts = []

        session = scoped_session(_session_factory)
        try:
            objs = session.query(FutureLatestMainContractView).order_by(
                asc(FutureLatestMainContractView.display_order)
            ).all()
            for obj in objs:
                if obj.contract_code is not None:
                    latest_main_contracts.append(obj.contract_code)
        finally:
            session.close()

        return latest_main_contracts


class FutureKline1d(_baseObject):

    '''
        CREATE TABLE t_future_k_line_1d
          (
             contract_code VARCHAR(10) NOT NULL,
             actual_time   DATETIME NOT NULL,
             trading_date  DATE NOT NULL,
             open          DECIMAL(20, 5) NOT NULL,
             close         DECIMAL(20, 5) NOT NULL,
             high          DECIMAL(20, 5) NOT NULL,
             low           DECIMAL(20, 5) NOT NULL,
             volume        DECIMAL(20, 5) NOT NULL,
             open_interest DECIMAL(20, 5) NOT NULL,
             CONSTRAINT pk_future_k_line_1d PRIMARY KEY (contract_code, actual_time),
             INDEX idx_future_k_line_1d (trading_date)
          )
        ENGINE=innodb DEFAULT CHARSET=utf8;
    '''

    __tablename__ = 't_future_k_line_1d'

    contract_code = Column(String(10), nullable=False)
    actual_time = Column(DateTime, nullable=False)
    trading_date = Column(Date, nullable=False)
    open = Column(DECIMAL(20, 5), nullable=False)
    close = Column(DECIMAL(20, 5), nullable=False)
    high = Column(DECIMAL(20, 5), nullable=False)
    low = Column(DECIMAL(20, 5), nullable=False)
    volume = Column(DECIMAL(20, 5), nullable=False)
    open_interest = Column(DECIMAL(20, 5), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint(
            'contract_code',
            'actual_time',
            name='pk_future_k_line_1d'
        ),
        Index(
            'idx_future_k_line_1d',
            'trading_date'
        ),
    )


class FutureKline1dManager(object):

    @staticmethod
    def fetch_k_line_data(contract_codes, start_date, end_date):
        k_line_data = rqdatac.get_price(
            order_book_ids=contract_codes,
            start_date=start_date,
            end_date=end_date,
            frequency='1d',
            fields=[
                'open',         # 开盘价
                'close',        # 收盘价
                'high',         # 最高价
                'low',          # 最低价
                'volume',       # 成交量
                'open_interest'  # 持仓量
            ],
            expect_df=True
        )
        if k_line_data is not None:
            k_line_data.reset_index(inplace=True)
            k_line_data.rename(
                columns={
                    'order_book_id': 'contract_code',
                    'date': 'actual_time'
                },
                inplace=True
            )
            k_line_data['trading_date'] = k_line_data['actual_time']

        return k_line_data

    @staticmethod
    def store(dataframe):
        session = scoped_session(_session_factory)
        try:
            for row in dataframe.itertuples():
                session.merge(
                    FutureKline1d(
                        contract_code=getattr(
                            row, 'contract_code'
                        ),
                        actual_time=getattr(
                            row, 'actual_time'
                        ).strftime('%Y-%m-%d %H:%M:%S'),
                        trading_date=getattr(
                            row, 'trading_date'
                        ).strftime('%Y-%m-%d'),
                        open=getattr(
                            row, 'open'
                        ),
                        close=getattr(
                            row, 'close'
                        ),
                        high=getattr(
                            row, 'high'
                        ),
                        low=getattr(
                            row, 'low'
                        ),
                        volume=getattr(
                            row, 'volume'
                        ),
                        open_interest=getattr(
                            row, 'open_interest'
                        )
                    )
                )
            session.commit()
        except BaseException:
            session.rollback()
            raise
        finally:
            session.close()

    @staticmethod
    def get_contract_k_line_data_by_trading_date(
            contract_code, start_date, end_date):
        k_line_data = None

        session = scoped_session(_session_factory)
        try:
            objs = session.query(FutureKline1d).filter(
                and_(
                    FutureKline1d.contract_code == contract_code,
                    FutureKline1d.trading_date >= start_date,
                    FutureKline1d.trading_date <= end_date
                )
            ).order_by(
                asc(FutureKline1d.contract_code),
                asc(FutureKline1d.actual_time)
            ).all()
            if objs:
                index_list = []
                contract_code_list = []
                actual_time_list = []
                trading_date_list = []
                open_list = []
                close_list = []
                high_list = []
                low_list = []
                volume_list = []
                open_interest_list = []
                for i in range(len(objs)):
                    index_list.append(i)
                    contract_code_list.append(objs[i].contract_code)
                    actual_time_list.append(objs[i].actual_time)
                    trading_date_list.append(objs[i].trading_date)
                    open_list.append(objs[i].open)
                    close_list.append(objs[i].close)
                    high_list.append(objs[i].high)
                    low_list.append(objs[i].low)
                    volume_list.append(objs[i].volume)
                    open_interest_list.append(objs[i].open_interest)
                k_line_data_dict = {
                    'contract_code': contract_code_list,
                    'actual_time': actual_time_list,
                    'trading_date': trading_date_list,
                    'open': open_list,
                    'close': close_list,
                    'high': high_list,
                    'low': low_list,
                    'volume': volume_list,
                    'open_interest': open_interest_list,
                }
                k_line_data = pandas.DataFrame(
                    data=k_line_data_dict,
                    index=index_list
                )
        finally:
            session.close()

        return k_line_data


class FutureKline1m(_baseObject):

    '''
        CREATE TABLE t_future_k_line_1m
          (
             contract_code VARCHAR(10) NOT NULL,
             actual_time   DATETIME NOT NULL,
             trading_date  DATE NOT NULL,
             open          DECIMAL(20, 5) NOT NULL,
             close         DECIMAL(20, 5) NOT NULL,
             high          DECIMAL(20, 5) NOT NULL,
             low           DECIMAL(20, 5) NOT NULL,
             volume        DECIMAL(20, 5) NOT NULL,
             open_interest DECIMAL(20, 5) NOT NULL,
             CONSTRAINT pk_future_k_line_1m PRIMARY KEY (contract_code, actual_time),
             INDEX idx_future_k_line_1m (trading_date)
          )
        ENGINE=innodb DEFAULT CHARSET=utf8;

    '''

    __tablename__ = 't_future_k_line_1m'

    contract_code = Column(String(10), nullable=False)
    actual_time = Column(DateTime, nullable=False)
    trading_date = Column(Date, nullable=False)
    open = Column(DECIMAL(20, 5), nullable=False)
    close = Column(DECIMAL(20, 5), nullable=False)
    high = Column(DECIMAL(20, 5), nullable=False)
    low = Column(DECIMAL(20, 5), nullable=False)
    volume = Column(DECIMAL(20, 5), nullable=False)
    open_interest = Column(DECIMAL(20, 5), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint(
            'contract_code',
            'actual_time',
            name='pk_future_k_line_1m'
        ),
        Index(
            'idx_future_k_line_1m',
            'trading_date'
        ),
    )


class FutureKline1mManager(object):

    @staticmethod
    def fetch_k_line_data(contract_codes, start_date, end_date):
        k_line_data = rqdatac.get_price(
            order_book_ids=contract_codes,
            start_date=start_date,
            end_date=end_date,
            frequency='1m',
            fields=[
                'trading_date',  # 交易日期
                'open',         # 开盘价
                'close',        # 收盘价
                'high',         # 最高价
                'low',          # 最低价
                'volume',       # 成交量
                'open_interest'  # 持仓量
            ],
            expect_df=True
        )
        if k_line_data is not None:
            k_line_data.reset_index(inplace=True)
            k_line_data.rename(
                columns={
                    'order_book_id': 'contract_code',
                    'datetime': 'actual_time'
                },
                inplace=True
            )

        return k_line_data

    @staticmethod
    def fetch_current_minute(contract_codes):
        k_line_data = rqdatac.current_minute(
            order_book_ids=contract_codes,
            fields=[
                'open',     # 开盘价
                'close',    # 收盘价
                'high',     # 最高价
                'low',      # 最低价
                'volume'    # 成交量
            ]
        )
        if k_line_data is not None:
            k_line_data.reset_index(inplace=True)
            k_line_data.rename(
                columns={
                    'order_book_id': 'contract_code',
                    'datetime': 'actual_time'
                },
                inplace=True
            )
            k_line_data['trading_date'] = datetime.datetime.strptime(
                '1970-01-01', '%Y-%m-%d'
            )
            k_line_data['open_interest'] = 0.0

        return k_line_data

    @staticmethod
    def store(dataframe):
        session = scoped_session(_session_factory)
        try:
            for row in dataframe.itertuples():
                session.merge(
                    FutureKline1m(
                        contract_code=getattr(
                            row, 'contract_code'
                        ),
                        actual_time=getattr(
                            row, 'actual_time'
                        ).strftime('%Y-%m-%d %H:%M:%S'),
                        trading_date=getattr(
                            row, 'trading_date'
                        ).strftime('%Y-%m-%d'),
                        open=getattr(
                            row, 'open'
                        ),
                        close=getattr(
                            row, 'close'
                        ),
                        high=getattr(
                            row, 'high'
                        ),
                        low=getattr(
                            row, 'low'
                        ),
                        volume=getattr(
                            row, 'volume'
                        ),
                        open_interest=getattr(
                            row, 'open_interest'
                        )
                    )
                )
            session.commit()
        except BaseException:
            session.rollback()
            raise
        finally:
            session.close()

    @staticmethod
    def get_contract_k_line_data_by_trading_date(
            contract_code, start_date, end_date):
        k_line_data = None

        session = scoped_session(_session_factory)
        try:
            objs = session.query(FutureKline1m).filter(
                and_(
                    FutureKline1m.contract_code == contract_code,
                    FutureKline1m.trading_date >= start_date,
                    FutureKline1m.trading_date <= end_date
                )
            ).order_by(
                asc(FutureKline1m.contract_code),
                asc(FutureKline1m.actual_time)
            ).all()
            if objs:
                index_list = []
                contract_code_list = []
                actual_time_list = []
                trading_date_list = []
                open_list = []
                close_list = []
                high_list = []
                low_list = []
                volume_list = []
                open_interest_list = []
                for i in range(len(objs)):
                    index_list.append(i)
                    contract_code_list.append(objs[i].contract_code)
                    actual_time_list.append(objs[i].actual_time)
                    trading_date_list.append(objs[i].trading_date)
                    open_list.append(objs[i].open)
                    close_list.append(objs[i].close)
                    high_list.append(objs[i].high)
                    low_list.append(objs[i].low)
                    volume_list.append(objs[i].volume)
                    open_interest_list.append(objs[i].open_interest)
                k_line_data_dict = {
                    'contract_code': contract_code_list,
                    'actual_time': actual_time_list,
                    'trading_date': trading_date_list,
                    'open': open_list,
                    'close': close_list,
                    'high': high_list,
                    'low': low_list,
                    'volume': volume_list,
                    'open_interest': open_interest_list,
                }
                k_line_data = pandas.DataFrame(
                    data=k_line_data_dict,
                    index=index_list
                )
        finally:
            session.close()

        return k_line_data


class FutureKline3m(_baseObject):

    '''
        CREATE TABLE t_future_k_line_3m
          (
             contract_code VARCHAR(10) NOT NULL,
             actual_time   DATETIME NOT NULL,
             trading_date  DATE NOT NULL,
             open          DECIMAL(20, 5) NOT NULL,
             close         DECIMAL(20, 5) NOT NULL,
             high          DECIMAL(20, 5) NOT NULL,
             low           DECIMAL(20, 5) NOT NULL,
             volume        DECIMAL(20, 5) NOT NULL,
             open_interest DECIMAL(20, 5) NOT NULL,
             CONSTRAINT pk_future_k_line_3m PRIMARY KEY (contract_code, actual_time),
             INDEX idx_future_k_line_3m (trading_date)
          )
        ENGINE=innodb DEFAULT CHARSET=utf8;

    '''

    __tablename__ = 't_future_k_line_3m'

    contract_code = Column(String(10), nullable=False)
    actual_time = Column(DateTime, nullable=False)
    trading_date = Column(Date, nullable=False)
    open = Column(DECIMAL(20, 5), nullable=False)
    close = Column(DECIMAL(20, 5), nullable=False)
    high = Column(DECIMAL(20, 5), nullable=False)
    low = Column(DECIMAL(20, 5), nullable=False)
    volume = Column(DECIMAL(20, 5), nullable=False)
    open_interest = Column(DECIMAL(20, 5), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint(
            'contract_code',
            'actual_time',
            name='pk_future_k_line_3m'
        ),
        Index(
            'idx_future_k_line_3m',
            'trading_date'
        ),
    )


class FutureKline3mManager(object):

    @staticmethod
    def generate_k_line_data_from_1m(dataframe):
        k1m_df = dataframe.copy(deep=True)
        k1m_df.reset_index(inplace=True)
        k1m_df.set_index(
            'actual_time',
            inplace=True
        )
        k3m_df = k1m_df.resample(
            '3min', label='right', closed='right'
        ).first()
        k3m_df['open'] = k1m_df['open'].resample(
            '3min', label='right', closed='right'
        ).first()
        k3m_df['close'] = k1m_df['close'].resample(
            '3min', label='right', closed='right'
        ).last()
        k3m_df['high'] = k1m_df['high'].resample(
            '3min', label='right', closed='right'
        ).max()
        k3m_df['low'] = k1m_df['low'].resample(
            '3min', label='right', closed='right'
        ).min()
        k3m_df['volume'] = k1m_df['volume'].resample(
            '3min', label='right', closed='right'
        ).sum()
        k3m_df['open_interest'] = k1m_df['open_interest'].resample(
            '3min', label='right', closed='right'
        ).last()
        k3m_df.drop(
            k3m_df[k3m_df.isnull().values].index,
            inplace=True
        )
        k3m_df.reset_index(inplace=True)

        return k3m_df

    @staticmethod
    def store(dataframe):
        session = scoped_session(_session_factory)
        try:
            for row in dataframe.itertuples():
                session.merge(
                    FutureKline3m(
                        contract_code=getattr(
                            row, 'contract_code'
                        ),
                        actual_time=getattr(
                            row, 'actual_time'
                        ).strftime('%Y-%m-%d %H:%M:%S'),
                        trading_date=getattr(
                            row, 'trading_date'
                        ).strftime('%Y-%m-%d'),
                        open=getattr(
                            row, 'open'
                        ),
                        close=getattr(
                            row, 'close'
                        ),
                        high=getattr(
                            row, 'high'
                        ),
                        low=getattr(
                            row, 'low'
                        ),
                        volume=getattr(
                            row, 'volume'
                        ),
                        open_interest=getattr(
                            row, 'open_interest'
                        )
                    )
                )
            session.commit()
        except BaseException:
            session.rollback()
            raise
        finally:
            session.close()

    @staticmethod
    def get_contract_k_line_data_by_trading_date(
            contract_code, start_date, end_date):
        k_line_data = None

        session = scoped_session(_session_factory)
        try:
            objs = session.query(FutureKline3m).filter(
                and_(
                    FutureKline3m.contract_code == contract_code,
                    FutureKline3m.trading_date >= start_date,
                    FutureKline3m.trading_date <= end_date
                )
            ).order_by(
                asc(FutureKline3m.contract_code),
                asc(FutureKline3m.actual_time)
            ).all()
            if objs:
                index_list = []
                contract_code_list = []
                actual_time_list = []
                trading_date_list = []
                open_list = []
                close_list = []
                high_list = []
                low_list = []
                volume_list = []
                open_interest_list = []
                for i in range(len(objs)):
                    index_list.append(i)
                    contract_code_list.append(objs[i].contract_code)
                    actual_time_list.append(objs[i].actual_time)
                    trading_date_list.append(objs[i].trading_date)
                    open_list.append(objs[i].open)
                    close_list.append(objs[i].close)
                    high_list.append(objs[i].high)
                    low_list.append(objs[i].low)
                    volume_list.append(objs[i].volume)
                    open_interest_list.append(objs[i].open_interest)
                k_line_data_dict = {
                    'contract_code': contract_code_list,
                    'actual_time': actual_time_list,
                    'trading_date': trading_date_list,
                    'open': open_list,
                    'close': close_list,
                    'high': high_list,
                    'low': low_list,
                    'volume': volume_list,
                    'open_interest': open_interest_list,
                }
                k_line_data = pandas.DataFrame(
                    data=k_line_data_dict,
                    index=index_list
                )
        finally:
            session.close()

        return k_line_data


class FutureKline5m(_baseObject):

    '''
        CREATE TABLE t_future_k_line_5m
          (
             contract_code VARCHAR(10) NOT NULL,
             actual_time   DATETIME NOT NULL,
             trading_date  DATE NOT NULL,
             open          DECIMAL(20, 5) NOT NULL,
             close         DECIMAL(20, 5) NOT NULL,
             high          DECIMAL(20, 5) NOT NULL,
             low           DECIMAL(20, 5) NOT NULL,
             volume        DECIMAL(20, 5) NOT NULL,
             open_interest DECIMAL(20, 5) NOT NULL,
             CONSTRAINT pk_future_k_line_5m PRIMARY KEY (contract_code, actual_time),
             INDEX idx_future_k_line_5m (trading_date)
          )
        ENGINE=innodb DEFAULT CHARSET=utf8;

    '''

    __tablename__ = 't_future_k_line_5m'

    contract_code = Column(String(10), nullable=False)
    actual_time = Column(DateTime, nullable=False)
    trading_date = Column(Date, nullable=False)
    open = Column(DECIMAL(20, 5), nullable=False)
    close = Column(DECIMAL(20, 5), nullable=False)
    high = Column(DECIMAL(20, 5), nullable=False)
    low = Column(DECIMAL(20, 5), nullable=False)
    volume = Column(DECIMAL(20, 5), nullable=False)
    open_interest = Column(DECIMAL(20, 5), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint(
            'contract_code',
            'actual_time',
            name='pk_future_k_line_5m'
        ),
        Index(
            'idx_future_k_line_5m',
            'trading_date'
        ),
    )


class FutureKline5mManager(object):

    @staticmethod
    def generate_k_line_data_from_1m(dataframe):
        k1m_df = dataframe.copy(deep=True)
        k1m_df.reset_index(inplace=True)
        k1m_df.set_index(
            'actual_time',
            inplace=True
        )
        k5m_df = k1m_df.resample(
            '5min', label='right', closed='right'
        ).first()
        k5m_df['open'] = k1m_df['open'].resample(
            '5min', label='right', closed='right'
        ).first()
        k5m_df['close'] = k1m_df['close'].resample(
            '5min', label='right', closed='right'
        ).last()
        k5m_df['high'] = k1m_df['high'].resample(
            '5min', label='right', closed='right'
        ).max()
        k5m_df['low'] = k1m_df['low'].resample(
            '5min', label='right', closed='right'
        ).min()
        k5m_df['volume'] = k1m_df['volume'].resample(
            '5min', label='right', closed='right'
        ).sum()
        k5m_df['open_interest'] = k1m_df['open_interest'].resample(
            '5min', label='right', closed='right'
        ).last()
        k5m_df.drop(
            k5m_df[k5m_df.isnull().values].index,
            inplace=True
        )
        k5m_df.reset_index(inplace=True)

        return k5m_df

    @staticmethod
    def store(dataframe):
        session = scoped_session(_session_factory)
        try:
            for row in dataframe.itertuples():
                session.merge(
                    FutureKline5m(
                        contract_code=getattr(
                            row, 'contract_code'
                        ),
                        actual_time=getattr(
                            row, 'actual_time'
                        ).strftime('%Y-%m-%d %H:%M:%S'),
                        trading_date=getattr(
                            row, 'trading_date'
                        ).strftime('%Y-%m-%d'),
                        open=getattr(
                            row, 'open'
                        ),
                        close=getattr(
                            row, 'close'
                        ),
                        high=getattr(
                            row, 'high'
                        ),
                        low=getattr(
                            row, 'low'
                        ),
                        volume=getattr(
                            row, 'volume'
                        ),
                        open_interest=getattr(
                            row, 'open_interest'
                        )
                    )
                )
            session.commit()
        except BaseException:
            session.rollback()
            raise
        finally:
            session.close()

    @staticmethod
    def get_contract_k_line_data_by_trading_date(
            contract_code, start_date, end_date):
        k_line_data = None

        session = scoped_session(_session_factory)
        try:
            objs = session.query(FutureKline5m).filter(
                and_(
                    FutureKline5m.contract_code == contract_code,
                    FutureKline5m.trading_date >= start_date,
                    FutureKline5m.trading_date <= end_date
                )
            ).order_by(
                asc(FutureKline5m.contract_code),
                asc(FutureKline5m.actual_time)
            ).all()
            if objs:
                index_list = []
                contract_code_list = []
                actual_time_list = []
                trading_date_list = []
                open_list = []
                close_list = []
                high_list = []
                low_list = []
                volume_list = []
                open_interest_list = []
                for i in range(len(objs)):
                    index_list.append(i)
                    contract_code_list.append(objs[i].contract_code)
                    actual_time_list.append(objs[i].actual_time)
                    trading_date_list.append(objs[i].trading_date)
                    open_list.append(objs[i].open)
                    close_list.append(objs[i].close)
                    high_list.append(objs[i].high)
                    low_list.append(objs[i].low)
                    volume_list.append(objs[i].volume)
                    open_interest_list.append(objs[i].open_interest)
                k_line_data_dict = {
                    'contract_code': contract_code_list,
                    'actual_time': actual_time_list,
                    'trading_date': trading_date_list,
                    'open': open_list,
                    'close': close_list,
                    'high': high_list,
                    'low': low_list,
                    'volume': volume_list,
                    'open_interest': open_interest_list,
                }
                k_line_data = pandas.DataFrame(
                    data=k_line_data_dict,
                    index=index_list
                )
        finally:
            session.close()

        return k_line_data


class FutureKline15m(_baseObject):

    '''
        CREATE TABLE t_future_k_line_15m
          (
             contract_code VARCHAR(10) NOT NULL,
             actual_time   DATETIME NOT NULL,
             trading_date  DATE NOT NULL,
             open          DECIMAL(20, 5) NOT NULL,
             close         DECIMAL(20, 5) NOT NULL,
             high          DECIMAL(20, 5) NOT NULL,
             low           DECIMAL(20, 5) NOT NULL,
             volume        DECIMAL(20, 5) NOT NULL,
             open_interest DECIMAL(20, 5) NOT NULL,
             CONSTRAINT pk_future_k_line_15m PRIMARY KEY (contract_code, actual_time),
             INDEX idx_future_k_line_15m (trading_date)
          )
        ENGINE=innodb DEFAULT CHARSET=utf8;
    '''

    __tablename__ = 't_future_k_line_15m'

    contract_code = Column(String(10), nullable=False)
    actual_time = Column(DateTime, nullable=False)
    trading_date = Column(Date, nullable=False)
    open = Column(DECIMAL(20, 5), nullable=False)
    close = Column(DECIMAL(20, 5), nullable=False)
    high = Column(DECIMAL(20, 5), nullable=False)
    low = Column(DECIMAL(20, 5), nullable=False)
    volume = Column(DECIMAL(20, 5), nullable=False)
    open_interest = Column(DECIMAL(20, 5), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint(
            'contract_code',
            'actual_time',
            name='pk_future_k_line_15m'
        ),
        Index(
            'idx_future_k_line_15m',
            'trading_date'
        ),
    )


class FutureKline15mManager(object):

    @staticmethod
    def generate_k_line_data_from_1m(dataframe):
        k1m_df = dataframe.copy(deep=True)
        k1m_df.reset_index(inplace=True)
        k1m_df.set_index(
            'actual_time',
            inplace=True
        )
        k15m_df = k1m_df.resample(
            '15min', label='right', closed='right'
        ).first()
        k15m_df['open'] = k1m_df['open'].resample(
            '15min', label='right', closed='right'
        ).first()
        k15m_df['close'] = k1m_df['close'].resample(
            '15min', label='right', closed='right'
        ).last()
        k15m_df['high'] = k1m_df['high'].resample(
            '15min', label='right', closed='right'
        ).max()
        k15m_df['low'] = k1m_df['low'].resample(
            '15min', label='right', closed='right'
        ).min()
        k15m_df['volume'] = k1m_df['volume'].resample(
            '15min', label='right', closed='right'
        ).sum()
        k15m_df['open_interest'] = k1m_df['open_interest'].resample(
            '15min', label='right', closed='right'
        ).last()
        k15m_df.drop(
            k15m_df[k15m_df.isnull().values].index,
            inplace=True
        )
        k15m_df.reset_index(inplace=True)

        return k15m_df

    @staticmethod
    def store(dataframe):
        session = scoped_session(_session_factory)
        try:
            for row in dataframe.itertuples():
                session.merge(
                    FutureKline15m(
                        contract_code=getattr(
                            row, 'contract_code'
                        ),
                        actual_time=getattr(
                            row, 'actual_time'
                        ).strftime('%Y-%m-%d %H:%M:%S'),
                        trading_date=getattr(
                            row, 'trading_date'
                        ).strftime('%Y-%m-%d'),
                        open=getattr(
                            row, 'open'
                        ),
                        close=getattr(
                            row, 'close'
                        ),
                        high=getattr(
                            row, 'high'
                        ),
                        low=getattr(
                            row, 'low'
                        ),
                        volume=getattr(
                            row, 'volume'
                        ),
                        open_interest=getattr(
                            row, 'open_interest'
                        )
                    )
                )
            session.commit()
        except BaseException:
            session.rollback()
            raise
        finally:
            session.close()

    @staticmethod
    def get_contract_k_line_data_by_trading_date(
            contract_code, start_date, end_date):
        k_line_data = None

        session = scoped_session(_session_factory)
        try:
            objs = session.query(FutureKline15m).filter(
                and_(
                    FutureKline15m.contract_code == contract_code,
                    FutureKline15m.trading_date >= start_date,
                    FutureKline15m.trading_date <= end_date
                )
            ).order_by(
                asc(FutureKline15m.contract_code),
                asc(FutureKline15m.actual_time)
            ).all()
            if objs:
                index_list = []
                contract_code_list = []
                actual_time_list = []
                trading_date_list = []
                open_list = []
                close_list = []
                high_list = []
                low_list = []
                volume_list = []
                open_interest_list = []
                for i in range(len(objs)):
                    index_list.append(i)
                    contract_code_list.append(objs[i].contract_code)
                    actual_time_list.append(objs[i].actual_time)
                    trading_date_list.append(objs[i].trading_date)
                    open_list.append(objs[i].open)
                    close_list.append(objs[i].close)
                    high_list.append(objs[i].high)
                    low_list.append(objs[i].low)
                    volume_list.append(objs[i].volume)
                    open_interest_list.append(objs[i].open_interest)
                k_line_data_dict = {
                    'contract_code': contract_code_list,
                    'actual_time': actual_time_list,
                    'trading_date': trading_date_list,
                    'open': open_list,
                    'close': close_list,
                    'high': high_list,
                    'low': low_list,
                    'volume': volume_list,
                    'open_interest': open_interest_list,
                }
                k_line_data = pandas.DataFrame(
                    data=k_line_data_dict,
                    index=index_list
                )
        finally:
            session.close()

        return k_line_data


class FutureMaStrategy(_baseObject):

    '''
        CREATE TABLE t_future_ma_strategy
          (
             contract_code VARCHAR(10) NOT NULL,
             k_line_type   VARCHAR(10) NOT NULL,
             transaction   VARCHAR(10) NOT NULL DEFAULT 'UNKNOWN',
             short_term_ma VARCHAR(1) NOT NULL DEFAULT 'N',
             long_term_ma  VARCHAR(1) NOT NULL DEFAULT 'N',
             ma60          VARCHAR(1) NOT NULL DEFAULT 'N',
             ma120         VARCHAR(1) NOT NULL DEFAULT 'N',
             ma250         VARCHAR(1) NOT NULL DEFAULT 'N',
             update_time   DATETIME NOT NULL DEFAULT Now(),
             CONSTRAINT pk_future_ma_strategy PRIMARY KEY (contract_code, k_line_type)
          )
        ENGINE=innodb DEFAULT CHARSET=utf8;
    '''

    __tablename__ = 't_future_ma_strategy'

    contract_code = Column(String(10), nullable=False)
    k_line_type = Column(String(10), nullable=False)
    transaction = Column(String(10), nullable=False, default='UNKNOWN')
    short_term_ma = Column(String(1), nullable=False, default='N')
    long_term_ma = Column(String(1), nullable=False, default='N')
    ma60 = Column(String(1), nullable=False, default='N')
    ma120 = Column(String(1), nullable=False, default='N')
    ma250 = Column(String(1), nullable=False, default='N')
    update_time = Column(
        DateTime,
        nullable=False,
        default=datetime.datetime.now()
    )

    __table_args__ = (
        PrimaryKeyConstraint(
            'contract_code',
            'k_line_type',
            name='pk_future_ma_strategy'
        ),
    )


class FutureMaStrategyManager(object):

    @staticmethod
    def store(
            contract_code,
            k_line_type,
            transaction,
            short_term_ma,
            long_term_ma,
            ma60,
            ma120,
            ma250):
        session = scoped_session(_session_factory)
        try:
            obj = session.query(FutureMaStrategy).filter(
                and_(
                    FutureMaStrategy.contract_code == contract_code,
                    FutureMaStrategy.k_line_type == k_line_type
                )
            ).first()
            if obj is None:
                obj = FutureMaStrategy(
                    contract_code=contract_code,
                    k_line_type=k_line_type
                )
            obj.transaction = transaction
            obj.short_term_ma = short_term_ma
            obj.long_term_ma = long_term_ma
            obj.ma60 = ma60
            obj.ma120 = ma120
            obj.ma250 = ma250
            obj.update_time = datetime.datetime.now()
            session.merge(obj)
            session.commit()
        except BaseException:
            session.rollback()
            raise
        finally:
            session.close()
