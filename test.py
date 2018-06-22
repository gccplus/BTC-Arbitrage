# -*- coding: utf-8 -*-
from binance.client import Client as BinanceSpot
import re
import time
import numpy as np


class HuobiTest:
    def __init__(self, ):
        pass


class BinanceTest:
    def __init__(self):
        self.client = BinanceSpot('HjlK2qHg1DZ6ya9kjHkBrPq18j43Zcl5si7QoMvzvRXMwO28gBK8VIWAJfN2UOiI',
                                  'VledCYW4007QZis6x0vF4ejxtn6U8nHldzhP3ZBF6xFETTtYr8aF8LvTz4D9vIxP')

    def test_binance_order(self):
        orderId = '83956188'
        order = self.client.get_order(symbol='BTCUSDT', orderId=orderId)
        # order = self.binanceClient.get_my_trades()
        print order

    @staticmethod
    def merge_depth(depth):
        new_depth = []
        for d in depth:
            price = float(re.match('(\d+\.\d)\d*', '{:.8f}'.format(float(d[0]))).group(1))
            amount = float(d[1])
            if len(new_depth) == 0:
                new_depth.append([price, amount])
            else:
                if new_depth[-1][0] == price:
                    new_depth[-1] = [price, new_depth[-1][1] + amount]
                else:
                    new_depth.append([price, amount])
        return new_depth[:5]

    def test_binance_depth(self):
        try:
            b_depth = self.client.get_order_book(symbol='BTCUSDT')
            print b_depth
            print BinanceTest.merge_depth(b_depth['bids'])
        except Exception as e:
            print e

    def test_binance_trade(self):
        price = '1000000'
        # buy_order = self.binanceClient.order_limit_buy(symbol='BTCUSDT', quantity=0.001011,
        #                                                price=price, newOrderRespType='FULL')
        sell_order = self.client.order_limit_sell(symbol='BTCUSDT', quantity='0.01',
                                                  price=price, newOrderRespType='FULL')
        print sell_order

    def test_binance_symbols(self):
        print self.client.get_symbol_info()

    def test_cancel_order(self):
        orderId = '72532649'
        print self.client.cancel_order(symbol='BTCUSDT', orderId=orderId)

    def test_kline(self):
        """
        1月 Jan  2月 Feb  3月 Mar  4月 Apr  5月 May  6月 Jun
        7月 Jul  8月 Aug  9月 Sep  10月 Oct 11月 Nov 12月 Dec
        :return:
        """
        instervals = ['1h']
        intervers2 = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '8h', '12h', '1d']
        for interval in instervals:
            start_time = '1 Jan 2018'
            kline = self.client.get_historical_klines("BTCUSDT", interval, start_time)
            data = []
            for item in kline:
                open_time = item[0]
                open = float(item[1])
                high = float(item[2])
                low = float(item[3])
                close = float(item[4])
                close_time = item[6]
                data.append(float('%.6f' % ((close-open)/open)))
            print data



if __name__ == '__main__':
    binance_test = BinanceTest()
    binance_test.test_kline()
    # binance_test.test_binance_trade()
    # binance_test.test_binance_depth()
    # binance_test.test_binance_order()
