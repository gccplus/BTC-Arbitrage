# -*- coding: utf-8 -*-
from OKEXService import *
import time

key_dict = {}
with open('config', 'r') as f:
    for line in f.readlines():
        splited = line.split('=')
        if len(splited) == 2:
            key_dict[splited[0].strip()] = splited[1].strip()
okex_future_client = OkexFutureClient(key_dict['OKEX_ACCESS_KEY'], key_dict['OKEX_SECRET_KEY'])
okex_spot_client = OkexSpotClient(key_dict['OKEX_ACCESS_KEY'], key_dict['OKEX_SECRET_KEY'])

contract_type = 'quarter'
symbol = 'btc_usdt'
# print okexclient.ticker(symbol, contract_type)
# print okexclient.hold_amount(symbol, contract_type)
#print okex_future_client.index(symbol)
#print okex_future_client.userinfo_4fix()
#print okex_spot_client.place_order('0.025', '20000', 'sell',symbol)
# print okex_future_client.place_order(symbol, contract_type, '1', '1', CLOSE_LONG, MATCH_PRICE_TRUE)
print okex_future_client.order_info(symbol, contract_type, 2, '-1', 1, 10)
#print okex_future_client.userinfo_4fix()

# try:
#     print okex_future_client.cancel(symbol, contract_type, 4679421795502208)
# except Exception as e:
#     print e
#print okex_future_client.depth(symbol, contract_type, 10)
# print okex_future_client.devolve(symbol, 2, 0.001)
# print okex_future_client.position_4fix(symbol, contract_type)
# print okex_spot_client.place_order(symbol, contract_type, '100', '1', '1', '1')
# print okexclient.place_order(symbol, contract_type, '100', '1', '2', '1')
# print okexclient.place_order(symbol, contract_type, '100', '1', '3', '1')


# print okex_spot_client.depth(symbol)
# print okex_spot_client.ticker(symbol)

# print okex_spot_client.balances()
# 市 价买入固定金额usdt
# print okex_spot_client.place_order('', 200, 'buy_market', symbol)
#限价卖出
#print okex_spot_client.place_order('0.0356', '8427', 'sell', symbol)
# orderid = '412068556'
# print okex_spot_client.history(symbol, 1)
#
#
#
# print okex_spot_client.status_order(symbol, 412068556)


# out1 = open('this_week', 'a+')
# out2 = open('next_week', 'a+')
# out3 = open('quarter', 'a+')
# i = 0
# while True:
#     for contract_type in ['this_week', 'next_week', 'quarter']:
#         try:
#             future_depth = okex_future_client.depth(symbol, contract_type, 5)
#         except Exception as e:
#             print 'get future depth error: %s' % e
#             time.sleep(3)
#             continue
#
#         try:
#             spot_depth = okex_spot_client.depth(symbol, 5)
#         except Exception as e:
#             print 'get spot depth error %s' % e
#             time.sleep(3)
#             continue
#
#         best_future_bid = int(float(future_depth['bids'][0][0]))
#         best_future_ask = int(float(future_depth['asks'][-1][0]))
#         best_spot_bid = int(float(spot_depth['bids'][0][0]))
#         best_spot_ask = int(float(spot_depth['asks'][-1][0]))
#
#         a1 = best_future_bid - best_spot_ask
#         a2 = best_future_ask - best_spot_bid
#         nowtime = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
#         if contract_type == 'this_week':
#             out1.write('{}: {}{}\n'.format(nowtime, a1, a2))
#         elif contract_type == 'next_week':
#             out2.write('{}: {}{}\n'.format(nowtime, a1, a2))
#         else:
#             out3.write('{}: {{}\n'.format(nowtime, a1, a2))
#     i += 1
#     if i % 3 == 0:
#         out1.flush()
#         out2.flush()
#         out3.flush()
#     time.sleep(3)
