# -*- coding: utf-8 -*-

import argparse
import datetime
import time
import logging
import logging.config
import yaml
import os
import rqdatac
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.executors.pool import ProcessPoolExecutor
from constants import STOCK_MARKET_CLOSING_TIME
from constants import FUTURE_MARKET_CLOSING_TIME
from entries import BasisCalendarManager
from entries import BasisTradingDateViewManager
from entries import FundScaleManager
from entries import FutureContractSymbolManager
from entries import FutureMainContractManager
from entries import FutureLatestMainContractViewManager
from entries import FutureKline1dManager
from entries import FutureKline1mManager
from entries import FutureKline3mManager
from entries import FutureKline5mManager
from entries import FutureKline15mManager
from strategies import MaStrategy

rqdatac.init()

file_path = os.path.dirname(os.path.realpath(__file__))
logging_yaml_path = file_path + os.sep + 'logging.yaml'
with open(logging_yaml_path, 'r') as file:
    logging.config.dictConfig(
        yaml.safe_load(file.read())
    )
logger = logging.getLogger('jobs')


# 交易日数据作业
def job_calendar(args):
    logger.info('[job_calendar] job start.')

    if 'start' in args.keys() and 'end' in args.keys():
        start_date = args['start']
        end_date = args['end']
    else:
        today = datetime.date.today()
        start_date = today + datetime.timedelta(days=-30)
        end_date = today + datetime.timedelta(days=+30)

    logger.info(
        '[job_calendar] start_date = {}, end_date = {}.'.format(
            start_date, end_date
        )
    )
    try:
        trading_dates = rqdatac.get_trading_dates(
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d')
        )
        for trading_date in trading_dates:
            BasisCalendarManager.store(trading_date.strftime('%Y-%m-%d'), 'Y')
    except BaseException:
        logger.error('[job_calendar] job failed.')

    logger.info('[job_calendar] job end.')


# 基金份额数据作业
def job_fund_scale(args):
    logger.info('[job_fund_scale] job start.')

    if 'start' in args.keys() and 'end' in args.keys():
        start_date = args['start']
        end_date = args['end']
        logger.info(
            '[job_fund_scale] start_date = {}, end_date = {}.'.format(
                start_date, end_date
            )
        )
        trading_dates = BasisTradingDateViewManager.get_trading_dates(
            start_date, end_date
        )
        for trading_date in trading_dates:
            logger.info(
                '[job_fund_scale] trading_date = {}.'.format(trading_date)
            )
            _job_fund_scale(trading_date)
    else:
        trading_date = None
        today = datetime.date.today().strftime('%Y-%m-%d')
        latest_trading_date = BasisTradingDateViewManager.get_latest_trading_date()
        # 只在交易日更新数据
        if today == latest_trading_date:
            now = datetime.datetime.now().strftime('%H:%M:%S')
            # 收盘后更新当前交易日数据，未收盘则更新上一交易日数据
            if now > STOCK_MARKET_CLOSING_TIME:
                trading_date = latest_trading_date
            else:
                trading_date = BasisTradingDateViewManager.get_previous_trading_date(
                    latest_trading_date, 1)
            logger.info(
                '[job_fund_scale] trading_date = {}.'.format(trading_date)
            )
            _job_fund_scale(trading_date)

    logger.info('[job_fund_scale] job end.')


def _job_fund_scale(trading_date):
    sse_funds = ['510050', '510300', '510500']
    for code in sse_funds:
        result = FundScaleManager.fetch_sse_fund_scale(
            code, trading_date
        )
        if result:
            FundScaleManager.store(
                result[0]['SEC_CODE'],
                result[0]['STAT_DATE'],
                result[0]['TOT_VOL']
            )
        else:
            FundScaleManager.store(
                code,
                trading_date,
                '0.0'
            )


# 期货主力合约数据作业
def job_future_main_contract(args):
    logger.info('[job_future_main_contract] job start.')

    if 'start' in args.keys() and 'end' in args.keys():
        start_date = args['start']
        end_date = args['end']
        logger.info(
            '[job_future_main_contract] start_date = {}, end_date = {}.'.format(
                start_date, end_date))
        trading_dates = BasisTradingDateViewManager.get_trading_dates(
            start_date, end_date
        )
        for trading_date in trading_dates:
            logger.info(
                '[job_future_main_contract] trading_date = {}.'.format(
                    trading_date
                )
            )
            _job_future_main_contract(trading_date)
    else:
        today = datetime.date.today().strftime('%Y-%m-%d')
        latest_trading_date = BasisTradingDateViewManager.get_latest_trading_date()
        # 只在交易日更新数据
        if today == latest_trading_date:
            now = datetime.datetime.now().strftime('%H:%M:%S')
            # 收盘后更新下一交易日数据
            if now > FUTURE_MARKET_CLOSING_TIME:
                next_trading_date = BasisTradingDateViewManager.get_next_trading_date(
                    latest_trading_date, 1)
                logger.info(
                    '[job_future_main_contract] trading_date = {}.'.format(
                        next_trading_date
                    )
                )
                _job_future_main_contract(next_trading_date)

    logger.info('[job_future_main_contract] job end.')


def _job_future_main_contract(next_trading_date):
    # 更新期货主力合约切换信息
    contract_symbols = FutureContractSymbolManager.get_contract_symbols()
    for contract_symbol in contract_symbols:
        try:
            logger.info(
                'check future main contract: {}@{}'.format(
                    contract_symbol, next_trading_date
                )
            )
            # 获取下一交易日的期货主力合约
            main_contract = rqdatac.futures.get_dominant(
                underlying_symbol=contract_symbol,
                start_date=next_trading_date,
                end_date=next_trading_date
            )
            if main_contract is not None:
                contract_code = main_contract[next_trading_date]
                FutureMainContractManager.store(
                    contract_code,
                    contract_symbol,
                    next_trading_date
                )
                logger.info(
                    'update future main contract: {}@{}'.format(
                        contract_code, next_trading_date
                    )
                )
        except BaseException:
            logger.error(
                'switch future main contract failed: {}@{}'.format(
                    contract_symbol, next_trading_date
                )
            )

    # 期货主力合约切换后预加载历史K线数据
    try:
        logger.info(
            '[job_future_main_contract] preload future main contract history k line data start.'
        )
        main_contracts = FutureMainContractManager.get_main_contracts_by_switch_date(
            next_trading_date)
        if main_contracts:
            start_date = BasisTradingDateViewManager.get_previous_trading_date(
                next_trading_date, 61
            )
            end_date = BasisTradingDateViewManager.get_previous_trading_date(
                next_trading_date, 1
            )

            logger.info(
                '[job_future_main_contract] fetch history k line data, type is 1d.'
            )
            k1d = FutureKline1dManager.fetch_k_line_data(
                main_contracts, start_date, end_date
            )
            if k1d is not None:
                logger.info(
                    '[job_future_main_contract] store history k line data, type is 1d.'
                )
                FutureKline1dManager.store(k1d)

            logger.info(
                '[job_future_main_contract] fetch and store history k line minute data.'
            )
            trading_dates = BasisTradingDateViewManager.get_trading_dates(
                start_date, end_date
            )
            for trading_date in trading_dates:
                logger.info(
                    '[job_future_main_contract] trading_date = {}'.format(
                        trading_date
                    )
                )

                logger.info(
                    '[job_future_main_contract] fetch history k line data, type is 1m.'
                )
                k1m = FutureKline1mManager.fetch_k_line_data(
                    main_contracts, trading_date, trading_date
                )
                if k1m is not None:
                    logger.info(
                        '[job_future_main_contract] store history k line data, type is 1m.'
                    )
                    FutureKline1mManager.store(k1m)

                logger.info(
                    '[job_future_main_contract] generate and store other history k line data.'
                )
                for main_contract in main_contracts:
                    logger.info(
                        '[job_future_main_contract] load {} history k line data, type is 1m.'.format(
                            main_contract
                        )
                    )
                    k1m = FutureKline1mManager.get_contract_k_line_data_by_trading_date(
                        main_contract, trading_date, trading_date)
                    if k1m is not None:
                        logger.info(
                            '[job_future_main_contract] generate {} history k line data, type is 3m.'.format(
                                main_contract
                            )
                        )
                        k3m = FutureKline3mManager.generate_k_line_data_from_1m(
                            k1m)
                        logger.info(
                            '[job_future_main_contract] store {} history k line data, type is 3m.'.format(
                                main_contract
                            )
                        )
                        FutureKline3mManager.store(k3m)

                        logger.info(
                            '[job_future_main_contract] generate {} history k line data, type is 5m.'.format(
                                main_contract
                            )
                        )
                        k5m = FutureKline5mManager.generate_k_line_data_from_1m(
                            k1m)
                        logger.info(
                            '[job_future_main_contract] store {} history k line data, type is 5m.'.format(
                                main_contract
                            )
                        )
                        FutureKline5mManager.store(k5m)

                        logger.info(
                            '[job_future_main_contract] generate {} history k line data, type is 15m.'.format(
                                main_contract
                            )
                        )
                        k15m = FutureKline15mManager.generate_k_line_data_from_1m(
                            k1m)
                        logger.info(
                            '[job_future_main_contract] store {} history k line data, type is 15m.'.format(
                                main_contract
                            )
                        )
                        FutureKline15mManager.store(k15m)
        logger.info(
            '[job_future_main_contract] preload future main contract history k line data end.'
        )
    except BaseException:
        logger.error(
            '[job_future_main_contract] preload future main contract history k line data failed.'
        )


# 期货历史K线数据作业
def job_future_history_k_line_data(args):
    logger.info('[job_future_history_k_line_data] job start.')

    if 'start' in args.keys() and 'end' in args.keys():
        start_date = args['start']
        end_date = args['end']
        logger.info(
            '[job_future_history_k_line_data] start_date = {}, end_date = {}.'.format(
                start_date, end_date))
        trading_dates = BasisTradingDateViewManager.get_trading_dates(
            start_date, end_date
        )
        for trading_date in trading_dates:
            logger.info(
                '[job_future_history_k_line_data] trading_date = {}.'.format(
                    trading_date
                )
            )
            _job_future_history_k_line_data(trading_date)
    else:
        today = datetime.date.today().strftime('%Y-%m-%d')
        latest_trading_date = BasisTradingDateViewManager.get_latest_trading_date()
        # 只在交易日更新数据
        if today == latest_trading_date:
            now = datetime.datetime.now().strftime('%H:%M:%S')
            # 收盘后更新当前交易日数据
            if now > FUTURE_MARKET_CLOSING_TIME:
                logger.info(
                    '[job_future_history_k_line_data] trading_date = {}.'.format(
                        latest_trading_date
                    )
                )
                _job_future_history_k_line_data(latest_trading_date)

    logger.info('[job_future_history_k_line_data] job end.')


def _job_future_history_k_line_data(trading_date):
    try:
        logger.info(
            '[job_future_history_k_line_data] load future history k line data start. trading_date = {}.'.format(
                trading_date
            )
        )
        main_contracts = FutureMainContractManager.get_main_contracts_by_trading_date(
            trading_date)
        if main_contracts:
            logger.info(
                '[job_future_history_k_line_data] fetch history k line data, type is 1d.'
            )
            k1d = FutureKline1dManager.fetch_k_line_data(
                main_contracts, trading_date, trading_date
            )
            if k1d is not None:
                logger.info(
                    '[job_future_history_k_line_data] store history k line data, type is 1d.'
                )
                FutureKline1dManager.store(k1d)

            logger.info(
                '[job_future_history_k_line_data] fetch history k line data, type is 1m.'
            )
            k1m = FutureKline1mManager.fetch_k_line_data(
                main_contracts, trading_date, trading_date
            )
            if k1m is not None:
                logger.info(
                    '[job_future_history_k_line_data] store history k line data, type is 1m.'
                )
                FutureKline1mManager.store(k1m)

            logger.info(
                '[job_future_history_k_line_data] generate and store other history k line data.'
            )
            for main_contract in main_contracts:
                logger.info(
                    '[job_future_history_k_line_data] load {} history k line data, type is 1m.'.format(
                        main_contract
                    )
                )
                k1m = FutureKline1mManager.get_contract_k_line_data_by_trading_date(
                    main_contract, trading_date, trading_date)
                if k1m is not None:
                    logger.info(
                        '[job_future_history_k_line_data] generate {} history k line data, type is 3m.'.format(
                            main_contract
                        )
                    )
                    k3m = FutureKline3mManager.generate_k_line_data_from_1m(
                        k1m)
                    logger.info(
                        '[job_future_history_k_line_data] store {} history k line data, type is 3m.'.format(
                            main_contract
                        )
                    )
                    FutureKline3mManager.store(k3m)

                    logger.info(
                        '[job_future_history_k_line_data] generate {} history k line data, type is 5m.'.format(
                            main_contract
                        )
                    )
                    k5m = FutureKline5mManager.generate_k_line_data_from_1m(
                        k1m)
                    logger.info(
                        '[job_future_history_k_line_data] store {} history k line data, type is 5m.'.format(
                            main_contract
                        )
                    )
                    FutureKline5mManager.store(k5m)

                    logger.info(
                        '[job_future_history_k_line_data] generate {} history k line data, type is 15m.'.format(
                            main_contract
                        )
                    )
                    k15m = FutureKline15mManager.generate_k_line_data_from_1m(
                        k1m)
                    logger.info(
                        '[job_future_history_k_line_data] store {} history k line data, type is 15m.'.format(
                            main_contract
                        )
                    )
                    FutureKline15mManager.store(k15m)
        logger.info(
            '[job_future_history_k_line_data] load future history k line data end. trading_date = {}.'.format(
                trading_date
            )
        )
    except BaseException:
        logger.error(
            '[job_future_history_k_line_data] load future history k line data failed. trading_date = {}.'.format(
                trading_date
            )
        )


# 期货均线策略计算作业
def job_future_ma_strategy(args):
    logger.info('[job_future_ma_strategy] job start.')

    if 'type' in args.keys():
        types = ['realtime', 'batch']
        if args['type'] in types:
            if args['type'] == 'batch':
                trading_date = None
                if 'date' in args.keys():
                    trading_date = args['date']
                else:
                    trading_date = BasisTradingDateViewManager.get_latest_trading_date()
                main_contracts = FutureMainContractManager.get_main_contracts_by_trading_date(
                    trading_date)
                _job_future_ma_strategy_batch(trading_date, main_contracts)

            if args['type'] == 'realtime':
                trading_date = None
                today = datetime.date.today().strftime('%Y-%m-%d')
                latest_trading_date = BasisTradingDateViewManager.get_latest_trading_date()
                # 只在交易日运行实时计算作业
                if today == latest_trading_date:
                    now = datetime.datetime.now().strftime('%H:%M:%S')
                    if now > FUTURE_MARKET_CLOSING_TIME:
                        trading_date = BasisTradingDateViewManager.get_next_trading_date(
                            latest_trading_date, 1)
                    else:
                        trading_date = latest_trading_date
                    main_contracts = FutureLatestMainContractViewManager.get_latest_main_contracts()
                    _job_future_ma_strategy_realtime(
                        trading_date, main_contracts)
        else:
            logger.error('[job_future_ma_strategy] bad type specified.')
    else:
        logger.error('[job_future_ma_strategy] type is not specified.')

    logger.info('[job_future_ma_strategy] job end.')


def _job_future_ma_strategy_batch(trading_date, main_contracts):
    strategy = MaStrategy(trading_date, main_contracts)
    strategy.batch()


def _job_future_ma_strategy_realtime(trading_date, main_contracts):
    strategy = MaStrategy(trading_date, main_contracts)
    strategy.fix_k_line_data()
    strategy.batch()

    fetchScheduler = BackgroundScheduler(
        executors={
            'default': ThreadPoolExecutor(2),
            'processpool': ProcessPoolExecutor(1)
        }
    )
    otherScheduler = BackgroundScheduler()

    today = datetime.date.today().strftime('%Y-%m-%d')
    if today == trading_date:
        # 日盘
        fetchScheduler.add_job(
            func=strategy.fetch_current_minute,
            trigger='interval',
            minutes=1,
            start_date=today + ' 09:00:01',
            end_date=today + ' 15:00:01'
        )
        otherScheduler.add_job(
            func=strategy.realtime_for_k1m,
            trigger='interval',
            minutes=1,
            start_date=today + ' 09:00:03',
            end_date=today + ' 15:00:03'
        )
        otherScheduler.add_job(
            func=strategy.realtime_for_k3m,
            trigger='interval',
            minutes=3,
            start_date=today + ' 09:03:03',
            end_date=today + ' 15:00:03'
        )
        otherScheduler.add_job(
            func=strategy.realtime_for_k5m,
            trigger='interval',
            minutes=5,
            start_date=today + ' 09:05:03',
            end_date=today + ' 15:00:03'
        )
        otherScheduler.add_job(
            func=strategy.realtime_for_k15m,
            trigger='interval',
            minutes=15,
            start_date=today + ' 09:15:03',
            end_date=today + ' 15:00:03'
        )
        otherScheduler.add_job(
            func=strategy.realtime_stop,
            trigger='date',
            run_date=today + ' 15:01:00'
        )
    else:
        # 夜盘
        fetchScheduler.add_job(
            func=strategy.fetch_current_minute,
            trigger='interval',
            minutes=1,
            start_date=today + ' 21:00:01',
            end_date=today + ' 23:00:01'
        )
        otherScheduler.add_job(
            func=strategy.realtime_for_k1m,
            trigger='interval',
            minutes=1,
            start_date=today + ' 21:00:03',
            end_date=today + ' 23:00:03'
        )
        otherScheduler.add_job(
            func=strategy.realtime_for_k3m,
            trigger='interval',
            minutes=3,
            start_date=today + ' 21:03:03',
            end_date=today + ' 23:00:03'
        )
        otherScheduler.add_job(
            func=strategy.realtime_for_k5m,
            trigger='interval',
            minutes=5,
            start_date=today + ' 21:05:03',
            end_date=today + ' 23:00:03'
        )
        otherScheduler.add_job(
            func=strategy.realtime_for_k15m,
            trigger='interval',
            minutes=15,
            start_date=today + ' 21:15:03',
            end_date=today + ' 23:00:03'
        )
        otherScheduler.add_job(
            func=strategy.realtime_stop,
            trigger='date',
            run_date=today + ' 23:01:00'
        )

    try:
        fetchScheduler.start()
        otherScheduler.start()

        while strategy.realtime_status():
            time.sleep(2)
    finally:
        fetchScheduler.shutdown()
        otherScheduler.shutdown()


if __name__ == '__main__':
    jobs = [
        'job_calendar',  # 交易日数据作业
        'job_fund_scale',  # 基金份额数据作业
        'job_future_main_contract',  # 期货主力合约数据作业
        'job_future_history_k_line_data',  # 期货历史K线数据作业
        'job_future_ma_strategy',  # 期货均线策略计算作业
    ]

    parser = argparse.ArgumentParser(description='quant jobs')
    parser.add_argument('--job', required=True, choices=jobs)
    parser.add_argument('--extras', required=False)

    args = parser.parse_args()

    subargs = {}
    if args.extras:
        extras = args.extras.split(',')
        for extra in extras:
            kv = extra.split('=')
            subargs[kv[0]] = kv[1]

    globals()[args.job](subargs)
