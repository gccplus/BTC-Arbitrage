# -*- coding: utf-8 -*-
from binance.client import Client as BinanceSpot
import  re

class HuobiTest:
    def __init__(self, ):
        pass


class BinanceTest:
    def __init__(self):
        self.client = BinanceSpot('HjlK2qHg1DZ6ya9kjHkBrPq18j43Zcl5si7QoMvzvRXMwO28gBK8VIWAJfN2UOiI',
                                  'VledCYW4007QZis6x0vF4ejxtn6U8nHldzhP3ZBF6xFETTtYr8aF8LvTz4D9vIxP')

    def test_binance_order(self):
        orderId = '72532682'
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

    # def test_binance_order(self):
    #     price = '10000.010'
    #     buy_order = self.binanceClient.order_limit_buy(symbol='BTCUSDT', quantity=0.001011,
    #                                                    price=price, newOrderRespType='FULL')
    #     print buy_order

    def test_binance_symbols(self):
        print self.client.get_symbol_info(symbol='BTCUSDT')

    def test_cancel_order(self):
        orderId = '72532649'
        print self.client.cancel_order(symbol='BTCUSDT', orderId=orderId)


class OkexTest:
    def __init__(self):
        pass


if __name__ == '__main__':
    binance_client = BinanceSpot('HjlK2qHg1DZ6ya9kjHkBrPq18j43Zcl5si7QoMvzvRXMwO28gBK8VIWAJfN2UOiI',
                                 'VledCYW4007QZis6x0vF4ejxtn6U8nHldzhP3ZBF6xFETTtYr8aF8LvTz4D9vIxP')
    binance_test = BinanceTest()
    binance_test.test_binance_depth()
    #binance_test.test_binance_order()
