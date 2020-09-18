# -*- coding: utf-8 -*-

import datetime
import time
import logging
import logging.config
import yaml
import os
import rqdatac
from enum import Enum
from entries import BasisTradingDateViewManager
from entries import FutureKline1dManager
from entries import FutureKline1mManager
from entries import FutureKline3mManager
from entries import FutureKline5mManager
from entries import FutureKline15mManager
from entries import FutureMaStrategyManager


file_path = os.path.dirname(os.path.realpath(__file__))
logging_yaml_path = file_path + os.sep + 'logging.yaml'
with open(logging_yaml_path, 'r') as file:
    logging.config.dictConfig(
        yaml.safe_load(file.read())
    )
logger = logging.getLogger('strategies')


class BaseStrategy(object):

    # 交易日期
    trading_date = None
    # 主力合约列表
    main_contracts = None

    def __init__(self, trading_date, main_contracts):
        self.trading_date = trading_date
        self.main_contracts = main_contracts

    def batch(self):
        try:
            logger.info('[BaseStrategy] batch start.')

            for main_contract in self.main_contracts:
                self.strategy_for_k1d(main_contract)
                self.strategy_for_k15m(main_contract)
                self.strategy_for_k5m(main_contract)
                self.strategy_for_k3m(main_contract)
                self.strategy_for_k1m(main_contract)

            logger.info('[BaseStrategy] batch end.')
        except BaseException:
            logger.error('[BaseStrategy] batch failed.')

    def fix_k_line_data(self):
        try:
            logger.info('[BaseStrategy] fix k line data.')

            k_line_data = FutureKline1mManager.fetch_k_line_data(
                self.main_contracts, self.trading_date, self.trading_date
            )
            if k_line_data is not None:
                FutureKline1mManager.store(k_line_data)
                for main_contract in self.main_contracts:
                    k1m = FutureKline1mManager.get_contract_k_line_data_by_trading_date(
                        main_contract, self.trading_date, self.trading_date)
                    if k1m is not None:
                        k3m = FutureKline3mManager.generate_k_line_data_from_1m(
                            k1m)
                        FutureKline3mManager.store(k3m)
                        k5m = FutureKline5mManager.generate_k_line_data_from_1m(
                            k1m)
                        FutureKline5mManager.store(k5m)
                        k15m = FutureKline15mManager.generate_k_line_data_from_1m(
                            k1m)
                        FutureKline15mManager.store(k15m)

            logger.info('[BaseStrategy] fix k line data finish.')
        except BaseException:
            logger.error('[BaseStrategy] fix k line data failed.')

    def fetch_current_minute(self):
        now = datetime.datetime.now()
        try:
            logger.info('[BaseStrategy] fetch current minute k line data.')

            k_line_data = FutureKline1mManager.fetch_current_minute(
                self.main_contracts
            )
            if k_line_data is not None:
                k_line_data['trading_date'] = datetime.datetime.strptime(
                    self.trading_date, '%Y-%m-%d'
                )
                k_line_data.drop(
                    k_line_data[
                        k_line_data.actual_time < now.strftime('%Y-%m-%d %H:%M:00')
                    ].index,
                    inplace=True
                )
                FutureKline1mManager.store(k_line_data)

            logger.info('[BaseStrategy] fetch current minute k line finish.')
        except BaseException:
            logger.error('[BaseStrategy] fetch current minute k line failed.')

    def realtime_for_k15m(self):
        minute_number = datetime.datetime.now().minute
        try:
            logger.info('[BaseStrategy] realtime for k15m start.')

            if minute_number % 15 == 0:
                for main_contract in self.main_contracts:
                    k1m = FutureKline1mManager.get_contract_k_line_data_by_trading_date(
                        main_contract, self.trading_date, self.trading_date)
                    if k1m is not None:
                        k15m = FutureKline15mManager.generate_k_line_data_from_1m(
                            k1m)
                        FutureKline15mManager.store(k15m)

                        self.strategy_for_k15m(main_contract)

            logger.info('[BaseStrategy] realtime for k15m end.')
        except BaseException:
            logger.error('[BaseStrategy] realtime for k15m failed.')

    def realtime_for_k5m(self):
        minute_number = datetime.datetime.now().minute
        try:
            logger.info('[BaseStrategy] realtime for k5m start.')

            if minute_number % 5 == 0:
                for main_contract in self.main_contracts:
                    k1m = FutureKline1mManager.get_contract_k_line_data_by_trading_date(
                        main_contract, self.trading_date, self.trading_date)
                    if k1m is not None:
                        k5m = FutureKline5mManager.generate_k_line_data_from_1m(
                            k1m)
                        FutureKline5mManager.store(k5m)

                        self.strategy_for_k5m(main_contract)

            logger.info('[BaseStrategy] realtime for k5m end.')
        except BaseException:
            logger.error('[BaseStrategy] realtime for k5m failed.')

    def realtime_for_k3m(self):
        minute_number = datetime.datetime.now().minute
        try:
            logger.info('[BaseStrategy] realtime for k3m start.')

            if minute_number % 3 == 0:
                for main_contract in self.main_contracts:
                    k1m = FutureKline1mManager.get_contract_k_line_data_by_trading_date(
                        main_contract, self.trading_date, self.trading_date)
                    if k1m is not None:
                        k3m = FutureKline3mManager.generate_k_line_data_from_1m(
                            k1m)
                        FutureKline3mManager.store(k3m)

                        self.strategy_for_k3m(main_contract)

            logger.info('[BaseStrategy] realtime for k3m end.')
        except BaseException:
            logger.error('[BaseStrategy] realtime for k3m failed.')

    def realtime_for_k1m(self):
        try:
            logger.info('[BaseStrategy] realtime for k1m start.')

            for main_contract in self.main_contracts:
                self.strategy_for_k1m(main_contract)

            logger.info('[BaseStrategy] realtime for k1m end.')
        except BaseException:
            logger.error('[BaseStrategy] realtime for k1m failed.')

    def strategy_for_k1d(self, main_contract):
        pass

    def strategy_for_k15m(self, main_contract):
        pass

    def strategy_for_k5m(self, main_contract):
        pass

    def strategy_for_k3m(self, main_contract):
        pass

    def strategy_for_k1m(self, main_contract):
        pass


class TransactionType(Enum):

    BUY = 'BUY'
    SELL = 'SELL'
    UNKNOWN = 'UNKNOWN'


class YesOrNo(Enum):

    YES = 'Y'
    NO = 'N'


# 均线策略

class MaType(Enum):

    MA5 = 5
    MA10 = 10
    MA20 = 20
    MA60 = 60
    MA120 = 120
    MA250 = 250


class MaStrategy(BaseStrategy):

    SHORT_TERM_MA = MaType.MA5
    LONG_TERM_MA = MaType.MA20

    _realtime_status = True

    def __init__(self, trading_date, main_contracts):
        super().__init__(trading_date, main_contracts)

    def realtime_status(self):
        return self._realtime_status

    def realtime_stop(self):
        self._realtime_status = False

    def strategy_for_k1d(self, contract_code):
        start_date = BasisTradingDateViewManager.get_previous_trading_date(
            self.trading_date, 61
        )
        k1d = FutureKline1dManager.get_contract_k_line_data_by_trading_date(
            contract_code, start_date, self.trading_date
        )
        if k1d is not None:
            # 计算短期均线
            k1d[MaStrategy.SHORT_TERM_MA.name] = k1d['close'].rolling(
                MaStrategy.SHORT_TERM_MA.value
            ).mean().round(decimals=5)
            # 计算长期均线
            k1d[MaStrategy.LONG_TERM_MA.name] = k1d['close'].rolling(
                MaStrategy.LONG_TERM_MA.value
            ).mean().round(decimals=5)
            # 计算MA60
            k1d[MaType.MA60.name] = k1d['close'].rolling(
                MaType.MA60.value
            ).mean().round(decimals=5)

            # 获取均线数据
            index_latest = k1d.index[-1]
            index_before_last = k1d.index[-2]
            short_term_ma_latest = getattr(
                k1d, MaStrategy.SHORT_TERM_MA.name
            )[index_latest]
            short_term_ma_before_last = getattr(
                k1d, MaStrategy.SHORT_TERM_MA.name
            )[index_before_last]
            long_term_ma_latest = getattr(
                k1d, MaStrategy.LONG_TERM_MA.name
            )[index_latest]
            long_term_ma_before_last = getattr(
                k1d, MaStrategy.LONG_TERM_MA.name
            )[index_before_last]
            ma60_latest = k1d[MaType.MA60.name][index_latest]
            ma60_before_last = k1d[MaType.MA60.name][index_before_last]

            # 判断交易方向
            transaction = TransactionType.UNKNOWN.value
            # 短期均线上穿长期均线，MA60向上，方向：多
            if ((short_term_ma_latest > long_term_ma_latest)
                    and (ma60_latest > ma60_before_last)):
                transaction = TransactionType.BUY.value
            # 短期均线上穿长期均线，短期均线向上，长期均线向上，方向：多
            if ((short_term_ma_latest > long_term_ma_latest)
                and (short_term_ma_latest > short_term_ma_before_last)
                    and (long_term_ma_latest > long_term_ma_before_last)):
                transaction = TransactionType.BUY.value
            # 短期均线下穿长期均线，MA60向下，方向：空
            if ((short_term_ma_latest < long_term_ma_latest)
                    and (ma60_latest < ma60_before_last)):
                transaction = TransactionType.SELL.value
            # 短期均线下穿长期均线，短期均线向下，长期均线向下，方向：空
            if ((short_term_ma_latest < long_term_ma_latest)
                and (short_term_ma_latest < short_term_ma_before_last)
                    and (long_term_ma_latest < long_term_ma_before_last)):
                transaction = TransactionType.SELL.value

            # 判断短期均线方向
            short_term_ma = YesOrNo.NO.value
            # 如果方向为多，且短期均线向上
            if ((transaction == TransactionType.BUY.value)
                    and (short_term_ma_latest > short_term_ma_before_last)):
                short_term_ma = YesOrNo.YES.value
            # 如果方向为空，且短期均线向下
            if ((transaction == TransactionType.SELL.value)
                    and (short_term_ma_latest < short_term_ma_before_last)):
                short_term_ma = YesOrNo.YES.value

            # 判断长期均线方向
            long_term_ma = YesOrNo.NO.value
            # 如果方向为多，且长期均线向上
            if ((transaction == TransactionType.BUY.value)
                    and (long_term_ma_latest > long_term_ma_before_last)):
                long_term_ma = YesOrNo.YES.value
            # 如果方向为空，且长期均线向下
            if ((transaction == TransactionType.SELL.value)
                    and (long_term_ma_latest < long_term_ma_before_last)):
                long_term_ma = YesOrNo.YES.value

            # 判断MA60方向
            ma60 = YesOrNo.NO.value
            # 如果方向为多，且MA60向上
            if ((transaction == TransactionType.BUY.value)
                    and (ma60_latest > ma60_before_last)):
                ma60 = YesOrNo.YES.value
            # 如果方向为空，且MA60向下
            if ((transaction == TransactionType.SELL.value)
                    and (ma60_latest < ma60_before_last)):
                ma60 = YesOrNo.YES.value

            FutureMaStrategyManager.store(
                contract_code,
                'k1d',
                transaction,
                short_term_ma,
                long_term_ma,
                ma60,
                'X',
                'X'
            )

    def strategy_for_k15m(self, contract_code):
        start_date = BasisTradingDateViewManager.get_previous_trading_date(
            self.trading_date, 20
        )
        k15m = FutureKline15mManager.get_contract_k_line_data_by_trading_date(
            contract_code, start_date, self.trading_date
        )
        if k15m is not None:
            # 计算短期均线
            k15m[MaStrategy.SHORT_TERM_MA.name] = k15m['close'].rolling(
                MaStrategy.SHORT_TERM_MA.value
            ).mean().round(decimals=5)
            # 计算长期均线
            k15m[MaStrategy.LONG_TERM_MA.name] = k15m['close'].rolling(
                MaStrategy.LONG_TERM_MA.value
            ).mean().round(decimals=5)
            # 计算MA60
            k15m[MaType.MA60.name] = k15m['close'].rolling(
                MaType.MA60.value
            ).mean().round(decimals=5)
            # 计算MA120
            k15m[MaType.MA120.name] = k15m['close'].rolling(
                MaType.MA120.value
            ).mean().round(decimals=5)
            # 计算MA250
            k15m[MaType.MA250.name] = k15m['close'].rolling(
                MaType.MA250.value
            ).mean().round(decimals=5)

            # 获取均线数据
            index_latest = k15m.index[-1]
            index_before_last = k15m.index[-2]
            short_term_ma_latest = getattr(
                k15m, MaStrategy.SHORT_TERM_MA.name
            )[index_latest]
            short_term_ma_before_last = getattr(
                k15m, MaStrategy.SHORT_TERM_MA.name
            )[index_before_last]
            long_term_ma_latest = getattr(
                k15m, MaStrategy.LONG_TERM_MA.name
            )[index_latest]
            long_term_ma_before_last = getattr(
                k15m, MaStrategy.LONG_TERM_MA.name
            )[index_before_last]
            ma60_latest = k15m[MaType.MA60.name][index_latest]
            ma60_before_last = k15m[MaType.MA60.name][index_before_last]
            ma120_latest = k15m[MaType.MA120.name][index_latest]
            ma120_before_last = k15m[MaType.MA120.name][index_before_last]
            ma250_latest = k15m[MaType.MA250.name][index_latest]
            ma250_before_last = k15m[MaType.MA250.name][index_before_last]

            # 判断交易方向
            transaction = TransactionType.UNKNOWN.value
            # 短期均线上穿长期均线，MA60向上，方向：多
            if ((short_term_ma_latest > long_term_ma_latest)
                    and (ma60_latest > ma60_before_last)):
                transaction = TransactionType.BUY.value
            # 短期均线上穿长期均线，短期均线向上，长期均线向上，方向：多
            if ((short_term_ma_latest > long_term_ma_latest)
                and (short_term_ma_latest > short_term_ma_before_last)
                    and (long_term_ma_latest > long_term_ma_before_last)):
                transaction = TransactionType.BUY.value
            # 短期均线下穿长期均线，MA60向下，方向：空
            if ((short_term_ma_latest < long_term_ma_latest)
                    and (ma60_latest < ma60_before_last)):
                transaction = TransactionType.SELL.value
            # 短期均线下穿长期均线，短期均线向下，长期均线向下，方向：空
            if ((short_term_ma_latest < long_term_ma_latest)
                and (short_term_ma_latest < short_term_ma_before_last)
                    and (long_term_ma_latest < long_term_ma_before_last)):
                transaction = TransactionType.SELL.value

            # 判断短期均线方向
            short_term_ma = YesOrNo.NO.value
            # 如果方向为多，且短期均线向上
            if ((transaction == TransactionType.BUY.value)
                    and (short_term_ma_latest > short_term_ma_before_last)):
                short_term_ma = YesOrNo.YES.value
            # 如果方向为空，且短期均线向下
            if ((transaction == TransactionType.SELL.value)
                    and (short_term_ma_latest < short_term_ma_before_last)):
                short_term_ma = YesOrNo.YES.value

            # 判断长期均线方向
            long_term_ma = YesOrNo.NO.value
            # 如果方向为多，且长期均线向上
            if ((transaction == TransactionType.BUY.value)
                    and (long_term_ma_latest > long_term_ma_before_last)):
                long_term_ma = YesOrNo.YES.value
            # 如果方向为空，且长期均线向下
            if ((transaction == TransactionType.SELL.value)
                    and (long_term_ma_latest < long_term_ma_before_last)):
                long_term_ma = YesOrNo.YES.value

            # 判断MA60方向
            ma60 = YesOrNo.NO.value
            # 如果方向为多，且MA60向上
            if ((transaction == TransactionType.BUY.value)
                    and (ma60_latest > ma60_before_last)):
                ma60 = YesOrNo.YES.value
            # 如果方向为空，且MA60向下
            if ((transaction == TransactionType.SELL.value)
                    and (ma60_latest < ma60_before_last)):
                ma60 = YesOrNo.YES.value

            # 判断MA120方向
            ma120 = YesOrNo.NO.value
            # 如果方向为多，且MA120向上
            if ((transaction == TransactionType.BUY.value)
                    and (ma120_latest > ma120_before_last)):
                ma120 = YesOrNo.YES.value
            # 如果方向为空，且MA120向下
            if ((transaction == TransactionType.SELL.value)
                    and (ma120_latest < ma120_before_last)):
                ma120 = YesOrNo.YES.value

            # 判断MA250方向
            ma250 = YesOrNo.NO.value
            # 如果方向为多，且MA250向上
            if ((transaction == TransactionType.BUY.value)
                    and (ma250_latest > ma250_before_last)):
                ma250 = YesOrNo.YES.value
            # 如果方向为空，且MA250向下
            if ((transaction == TransactionType.SELL.value)
                    and (ma250_latest < ma250_before_last)):
                ma250 = YesOrNo.YES.value

            FutureMaStrategyManager.store(
                contract_code,
                'k15m',
                transaction,
                short_term_ma,
                long_term_ma,
                ma60,
                ma120,
                ma250
            )

    def strategy_for_k5m(self, contract_code):
        start_date = BasisTradingDateViewManager.get_previous_trading_date(
            self.trading_date, 7
        )
        k5m = FutureKline5mManager.get_contract_k_line_data_by_trading_date(
            contract_code, start_date, self.trading_date
        )
        if k5m is not None:
            # 计算短期均线
            k5m[MaStrategy.SHORT_TERM_MA.name] = k5m['close'].rolling(
                MaStrategy.SHORT_TERM_MA.value
            ).mean().round(decimals=5)
            # 计算长期均线
            k5m[MaStrategy.LONG_TERM_MA.name] = k5m['close'].rolling(
                MaStrategy.LONG_TERM_MA.value
            ).mean().round(decimals=5)
            # 计算MA60
            k5m[MaType.MA60.name] = k5m['close'].rolling(
                MaType.MA60.value
            ).mean().round(decimals=5)
            # 计算MA120
            k5m[MaType.MA120.name] = k5m['close'].rolling(
                MaType.MA120.value
            ).mean().round(decimals=5)
            # 计算MA250
            k5m[MaType.MA250.name] = k5m['close'].rolling(
                MaType.MA250.value
            ).mean().round(decimals=5)

            # 获取均线数据
            index_latest = k5m.index[-1]
            index_before_last = k5m.index[-2]
            short_term_ma_latest = getattr(
                k5m, MaStrategy.SHORT_TERM_MA.name
            )[index_latest]
            short_term_ma_before_last = getattr(
                k5m, MaStrategy.SHORT_TERM_MA.name
            )[index_before_last]
            long_term_ma_latest = getattr(
                k5m, MaStrategy.LONG_TERM_MA.name
            )[index_latest]
            long_term_ma_before_last = getattr(
                k5m, MaStrategy.LONG_TERM_MA.name
            )[index_before_last]
            ma60_latest = k5m[MaType.MA60.name][index_latest]
            ma60_before_last = k5m[MaType.MA60.name][index_before_last]
            ma120_latest = k5m[MaType.MA120.name][index_latest]
            ma120_before_last = k5m[MaType.MA120.name][index_before_last]
            ma250_latest = k5m[MaType.MA250.name][index_latest]
            ma250_before_last = k5m[MaType.MA250.name][index_before_last]

            # 判断交易方向
            transaction = TransactionType.UNKNOWN.value
            # 短期均线上穿长期均线，MA60向上，方向：多
            if ((short_term_ma_latest > long_term_ma_latest)
                    and (ma60_latest > ma60_before_last)):
                transaction = TransactionType.BUY.value
            # 短期均线上穿长期均线，短期均线向上，长期均线向上，方向：多
            if ((short_term_ma_latest > long_term_ma_latest)
                and (short_term_ma_latest > short_term_ma_before_last)
                    and (long_term_ma_latest > long_term_ma_before_last)):
                transaction = TransactionType.BUY.value
            # 短期均线下穿长期均线，MA60向下，方向：空
            if ((short_term_ma_latest < long_term_ma_latest)
                    and (ma60_latest < ma60_before_last)):
                transaction = TransactionType.SELL.value
            # 短期均线下穿长期均线，短期均线向下，长期均线向下，方向：空
            if ((short_term_ma_latest < long_term_ma_latest)
                and (short_term_ma_latest < short_term_ma_before_last)
                    and (long_term_ma_latest < long_term_ma_before_last)):
                transaction = TransactionType.SELL.value

            # 判断短期均线方向
            short_term_ma = YesOrNo.NO.value
            # 如果方向为多，且短期均线向上
            if ((transaction == TransactionType.BUY.value)
                    and (short_term_ma_latest > short_term_ma_before_last)):
                short_term_ma = YesOrNo.YES.value
            # 如果方向为空，且短期均线向下
            if ((transaction == TransactionType.SELL.value)
                    and (short_term_ma_latest < short_term_ma_before_last)):
                short_term_ma = YesOrNo.YES.value

            # 判断长期均线方向
            long_term_ma = YesOrNo.NO.value
            # 如果方向为多，且长期均线向上
            if ((transaction == TransactionType.BUY.value)
                    and (long_term_ma_latest > long_term_ma_before_last)):
                long_term_ma = YesOrNo.YES.value
            # 如果方向为空，且长期均线向下
            if ((transaction == TransactionType.SELL.value)
                    and (long_term_ma_latest < long_term_ma_before_last)):
                long_term_ma = YesOrNo.YES.value

            # 判断MA60方向
            ma60 = YesOrNo.NO.value
            # 如果方向为多，且MA60向上
            if ((transaction == TransactionType.BUY.value)
                    and (ma60_latest > ma60_before_last)):
                ma60 = YesOrNo.YES.value
            # 如果方向为空，且MA60向下
            if ((transaction == TransactionType.SELL.value)
                    and (ma60_latest < ma60_before_last)):
                ma60 = YesOrNo.YES.value

            # 判断MA120方向
            ma120 = YesOrNo.NO.value
            # 如果方向为多，且MA120向上
            if ((transaction == TransactionType.BUY.value)
                    and (ma120_latest > ma120_before_last)):
                ma120 = YesOrNo.YES.value
            # 如果方向为空，且MA120向下
            if ((transaction == TransactionType.SELL.value)
                    and (ma120_latest < ma120_before_last)):
                ma120 = YesOrNo.YES.value

            # 判断MA250方向
            ma250 = YesOrNo.NO.value
            # 如果方向为多，且MA250向上
            if ((transaction == TransactionType.BUY.value)
                    and (ma250_latest > ma250_before_last)):
                ma250 = YesOrNo.YES.value
            # 如果方向为空，且MA250向下
            if ((transaction == TransactionType.SELL.value)
                    and (ma250_latest < ma250_before_last)):
                ma250 = YesOrNo.YES.value

            FutureMaStrategyManager.store(
                contract_code,
                'k5m',
                transaction,
                short_term_ma,
                long_term_ma,
                ma60,
                ma120,
                ma250
            )

    def strategy_for_k3m(self, contract_code):
        start_date = BasisTradingDateViewManager.get_previous_trading_date(
            self.trading_date, 5
        )
        k3m = FutureKline5mManager.get_contract_k_line_data_by_trading_date(
            contract_code, start_date, self.trading_date
        )
        if k3m is not None:
            # 计算短期均线
            k3m[MaStrategy.SHORT_TERM_MA.name] = k3m['close'].rolling(
                MaStrategy.SHORT_TERM_MA.value
            ).mean().round(decimals=5)
            # 计算长期均线
            k3m[MaStrategy.LONG_TERM_MA.name] = k3m['close'].rolling(
                MaStrategy.LONG_TERM_MA.value
            ).mean().round(decimals=5)
            # 计算MA60
            k3m[MaType.MA60.name] = k3m['close'].rolling(
                MaType.MA60.value
            ).mean().round(decimals=5)
            # 计算MA120
            k3m[MaType.MA120.name] = k3m['close'].rolling(
                MaType.MA120.value
            ).mean().round(decimals=5)
            # 计算MA250
            k3m[MaType.MA250.name] = k3m['close'].rolling(
                MaType.MA250.value
            ).mean().round(decimals=5)

            # 获取均线数据
            index_latest = k3m.index[-1]
            index_before_last = k3m.index[-2]
            short_term_ma_latest = getattr(
                k3m, MaStrategy.SHORT_TERM_MA.name
            )[index_latest]
            short_term_ma_before_last = getattr(
                k3m, MaStrategy.SHORT_TERM_MA.name
            )[index_before_last]
            long_term_ma_latest = getattr(
                k3m, MaStrategy.LONG_TERM_MA.name
            )[index_latest]
            long_term_ma_before_last = getattr(
                k3m, MaStrategy.LONG_TERM_MA.name
            )[index_before_last]
            ma60_latest = k3m[MaType.MA60.name][index_latest]
            ma60_before_last = k3m[MaType.MA60.name][index_before_last]
            ma120_latest = k3m[MaType.MA120.name][index_latest]
            ma120_before_last = k3m[MaType.MA120.name][index_before_last]
            ma250_latest = k3m[MaType.MA250.name][index_latest]
            ma250_before_last = k3m[MaType.MA250.name][index_before_last]

            # 判断交易方向
            transaction = TransactionType.UNKNOWN.value
            # 短期均线上穿长期均线，MA60向上，方向：多
            if ((short_term_ma_latest > long_term_ma_latest)
                    and (ma60_latest > ma60_before_last)):
                transaction = TransactionType.BUY.value
            # 短期均线上穿长期均线，短期均线向上，长期均线向上，方向：多
            if ((short_term_ma_latest > long_term_ma_latest)
                and (short_term_ma_latest > short_term_ma_before_last)
                    and (long_term_ma_latest > long_term_ma_before_last)):
                transaction = TransactionType.BUY.value
            # 短期均线下穿长期均线，MA60向下，方向：空
            if ((short_term_ma_latest < long_term_ma_latest)
                    and (ma60_latest < ma60_before_last)):
                transaction = TransactionType.SELL.value
            # 短期均线下穿长期均线，短期均线向下，长期均线向下，方向：空
            if ((short_term_ma_latest < long_term_ma_latest)
                and (short_term_ma_latest < short_term_ma_before_last)
                    and (long_term_ma_latest < long_term_ma_before_last)):
                transaction = TransactionType.SELL.value

            # 判断短期均线方向
            short_term_ma = YesOrNo.NO.value
            # 如果方向为多，且短期均线向上
            if ((transaction == TransactionType.BUY.value)
                    and (short_term_ma_latest > short_term_ma_before_last)):
                short_term_ma = YesOrNo.YES.value
            # 如果方向为空，且短期均线向下
            if ((transaction == TransactionType.SELL.value)
                    and (short_term_ma_latest < short_term_ma_before_last)):
                short_term_ma = YesOrNo.YES.value

            # 判断长期均线方向
            long_term_ma = YesOrNo.NO.value
            # 如果方向为多，且长期均线向上
            if ((transaction == TransactionType.BUY.value)
                    and (long_term_ma_latest > long_term_ma_before_last)):
                long_term_ma = YesOrNo.YES.value
            # 如果方向为空，且长期均线向下
            if ((transaction == TransactionType.SELL.value)
                    and (long_term_ma_latest < long_term_ma_before_last)):
                long_term_ma = YesOrNo.YES.value

            # 判断MA60方向
            ma60 = YesOrNo.NO.value
            # 如果方向为多，且MA60向上
            if ((transaction == TransactionType.BUY.value)
                    and (ma60_latest > ma60_before_last)):
                ma60 = YesOrNo.YES.value
            # 如果方向为空，且MA60向下
            if ((transaction == TransactionType.SELL.value)
                    and (ma60_latest < ma60_before_last)):
                ma60 = YesOrNo.YES.value

            # 判断MA120方向
            ma120 = YesOrNo.NO.value
            # 如果方向为多，且MA120向上
            if ((transaction == TransactionType.BUY.value)
                    and (ma120_latest > ma120_before_last)):
                ma120 = YesOrNo.YES.value
            # 如果方向为空，且MA120向下
            if ((transaction == TransactionType.SELL.value)
                    and (ma120_latest < ma120_before_last)):
                ma120 = YesOrNo.YES.value

            # 判断MA250方向
            ma250 = YesOrNo.NO.value
            # 如果方向为多，且MA250向上
            if ((transaction == TransactionType.BUY.value)
                    and (ma250_latest > ma250_before_last)):
                ma250 = YesOrNo.YES.value
            # 如果方向为空，且MA250向下
            if ((transaction == TransactionType.SELL.value)
                    and (ma250_latest < ma250_before_last)):
                ma250 = YesOrNo.YES.value

            FutureMaStrategyManager.store(
                contract_code,
                'k3m',
                transaction,
                short_term_ma,
                long_term_ma,
                ma60,
                ma120,
                ma250
            )

    def strategy_for_k1m(self, contract_code):
        start_date = BasisTradingDateViewManager.get_previous_trading_date(
            self.trading_date, 2
        )
        k1m = FutureKline5mManager.get_contract_k_line_data_by_trading_date(
            contract_code, start_date, self.trading_date
        )
        if k1m is not None:
            # 计算短期均线
            k1m[MaStrategy.SHORT_TERM_MA.name] = k1m['close'].rolling(
                MaStrategy.SHORT_TERM_MA.value
            ).mean().round(decimals=5)
            # 计算长期均线
            k1m[MaStrategy.LONG_TERM_MA.name] = k1m['close'].rolling(
                MaStrategy.LONG_TERM_MA.value
            ).mean().round(decimals=5)
            # 计算MA60
            k1m[MaType.MA60.name] = k1m['close'].rolling(
                MaType.MA60.value
            ).mean().round(decimals=5)
            # 计算MA120
            k1m[MaType.MA120.name] = k1m['close'].rolling(
                MaType.MA120.value
            ).mean().round(decimals=5)
            # 计算MA250
            k1m[MaType.MA250.name] = k1m['close'].rolling(
                MaType.MA250.value
            ).mean().round(decimals=5)

            # 获取均线数据
            index_latest = k1m.index[-1]
            index_before_last = k1m.index[-2]
            short_term_ma_latest = getattr(
                k1m, MaStrategy.SHORT_TERM_MA.name
            )[index_latest]
            short_term_ma_before_last = getattr(
                k1m, MaStrategy.SHORT_TERM_MA.name
            )[index_before_last]
            long_term_ma_latest = getattr(
                k1m, MaStrategy.LONG_TERM_MA.name
            )[index_latest]
            long_term_ma_before_last = getattr(
                k1m, MaStrategy.LONG_TERM_MA.name
            )[index_before_last]
            ma60_latest = k1m[MaType.MA60.name][index_latest]
            ma60_before_last = k1m[MaType.MA60.name][index_before_last]
            ma120_latest = k1m[MaType.MA120.name][index_latest]
            ma120_before_last = k1m[MaType.MA120.name][index_before_last]
            ma250_latest = k1m[MaType.MA250.name][index_latest]
            ma250_before_last = k1m[MaType.MA250.name][index_before_last]

            # 判断交易方向
            transaction = TransactionType.UNKNOWN.value
            # 短期均线上穿长期均线，MA60向上，方向：多
            if ((short_term_ma_latest > long_term_ma_latest)
                    and (ma60_latest > ma60_before_last)):
                transaction = TransactionType.BUY.value
            # 短期均线上穿长期均线，短期均线向上，长期均线向上，方向：多
            if ((short_term_ma_latest > long_term_ma_latest)
                and (short_term_ma_latest > short_term_ma_before_last)
                    and (long_term_ma_latest > long_term_ma_before_last)):
                transaction = TransactionType.BUY.value
            # 短期均线下穿长期均线，MA60向下，方向：空
            if ((short_term_ma_latest < long_term_ma_latest)
                    and (ma60_latest < ma60_before_last)):
                transaction = TransactionType.SELL.value
            # 短期均线下穿长期均线，短期均线向下，长期均线向下，方向：空
            if ((short_term_ma_latest < long_term_ma_latest)
                and (short_term_ma_latest < short_term_ma_before_last)
                    and (long_term_ma_latest < long_term_ma_before_last)):
                transaction = TransactionType.SELL.value

            # 判断短期均线方向
            short_term_ma = YesOrNo.NO.value
            # 如果方向为多，且短期均线向上
            if ((transaction == TransactionType.BUY.value)
                    and (short_term_ma_latest > short_term_ma_before_last)):
                short_term_ma = YesOrNo.YES.value
            # 如果方向为空，且短期均线向下
            if ((transaction == TransactionType.SELL.value)
                    and (short_term_ma_latest < short_term_ma_before_last)):
                short_term_ma = YesOrNo.YES.value

            # 判断长期均线方向
            long_term_ma = YesOrNo.NO.value
            # 如果方向为多，且长期均线向上
            if ((transaction == TransactionType.BUY.value)
                    and (long_term_ma_latest > long_term_ma_before_last)):
                long_term_ma = YesOrNo.YES.value
            # 如果方向为空，且长期均线向下
            if ((transaction == TransactionType.SELL.value)
                    and (long_term_ma_latest < long_term_ma_before_last)):
                long_term_ma = YesOrNo.YES.value

            # 判断MA60方向
            ma60 = YesOrNo.NO.value
            # 如果方向为多，且MA60向上
            if ((transaction == TransactionType.BUY.value)
                    and (ma60_latest > ma60_before_last)):
                ma60 = YesOrNo.YES.value
            # 如果方向为空，且MA60向下
            if ((transaction == TransactionType.SELL.value)
                    and (ma60_latest < ma60_before_last)):
                ma60 = YesOrNo.YES.value

            # 判断MA120方向
            ma120 = YesOrNo.NO.value
            # 如果方向为多，且MA120向上
            if ((transaction == TransactionType.BUY.value)
                    and (ma120_latest > ma120_before_last)):
                ma120 = YesOrNo.YES.value
            # 如果方向为空，且MA120向下
            if ((transaction == TransactionType.SELL.value)
                    and (ma120_latest < ma120_before_last)):
                ma120 = YesOrNo.YES.value

            # 判断MA250方向
            ma250 = YesOrNo.NO.value
            # 如果方向为多，且MA250向上
            if ((transaction == TransactionType.BUY.value)
                    and (ma250_latest > ma250_before_last)):
                ma250 = YesOrNo.YES.value
            # 如果方向为空，且MA250向下
            if ((transaction == TransactionType.SELL.value)
                    and (ma250_latest < ma250_before_last)):
                ma250 = YesOrNo.YES.value

            FutureMaStrategyManager.store(
                contract_code,
                'k1m',
                transaction,
                short_term_ma,
                long_term_ma,
                ma60,
                ma120,
                ma250
            )
