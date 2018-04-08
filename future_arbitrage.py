# -*- coding: utf-8 -*-
from OKEXService import OkexFutureClient, OkexSpotClient
import logging
import sys
import time
import numpy as np


class TermArbitrage:
    OKEX_ACCESS_KEY = 'ee02b2e1-b183-410d-883f-c22afc81b12e'
    OKEX_SECRET_KEY = '983676E3B392EF4AA67F1F7CF06E2ABB'

    # 正表示期货价格高于现货
    # TODO
    # 增加动态调整
    open_positive_thd = 35
    close_positive_thd = -5

    open_negative_thd = -35
    close_negetive_thd = 5

    # 风险控制
    margin_thd = 150
    risk_thd = 0.5

    margin_coefficient = 0.5
    min_contract_amount = 1

    symbol = 'btc_usdt'
    contract_type = 'this_week'

    # 每次最大合约交易量
    max_contract_exchange_amount = 5

    # 1 开多 2 开空 3 平多 4 平空
    debug_type = 3

    def __init__(self):
        self.future_client = OkexFutureClient(
            self.OKEX_ACCESS_KEY, self.OKEX_SECRET_KEY)
        self.spot_client = OkexSpotClient(
            self.OKEX_ACCESS_KEY, self.OKEX_SECRET_KEY)

        # 空头合约数量
        self.bear_amount = 0

        # 多头合约数量
        self.bull_amount = 0

        #
        self.buy_available = 0
        self.sell_available = 0
        self.buy_profit_lossratio = 0
        self.sell_profit_lossratio = 0
        self.sell_price_avg = 0
        self.buy_price_avg = 0

        # 保证金
        self.keep_deposit = 0
        self.risk_rate = 0
        self.future_rights = 0
        self.profit_real = 0
        self.profit_unreal = 0
        # 指数
        self.future_index = 0

        self.spot_free_btc = 0
        self.spot_free_usdt = 0
        self.spot_freezed_btc = 0
        self.spot_freezed_usdt = 0

        # 统计收益
        

        # config logging
        self.logger = logging.getLogger("Future")

        # 指定logger输出格式
        formatter = logging.Formatter(
            '%(asctime)s %(levelname)-8s: %(message)s')

        # 文件日志
        file_handler = logging.FileHandler("term.log")
        file_handler.setFormatter(formatter)

        # 控制台日志
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.formatter = formatter

        # 为logger添加的日志处理器
        self.logger.addHandler(file_handler)
        #self.logger.addHandler(console_handler)

        # 指定日志的最低输出级别，默认为WARN级别
        self.logger.setLevel(logging.INFO)

    def asset_balance(self):
        self.logger.info('平衡资产配比')
        total_btc = self.future_rights + self.spot_free_btc
        future_prop = 1 / (1 + 10 * self.margin_coefficient)
        trans_amount = abs(self.future_rights - total_btc * future_prop)
        if trans_amount < 0.0001:
            self.logger.info('平衡状态,无需转移')
            return
        if self.future_rights > total_btc * future_prop:
            self.logger.info('合约转币币: %s' % trans_amount)
            if trans_amount > self.future_rights - self.profit_real:
                trans_amount = self.future_rights - self.profit_real
            try:
                self.future_client.devolve(self.symbol, '2', trans_amount)
            except Exception as e:
                self.logger.error('合约转币币错误: %s' % e)
        else:
            self.logger.info('币币转合约: %s' % trans_amount)
            try:
                self.future_client.devolve(self.symbol, '1', trans_amount)
            except Exception as e:
                self.logger.error('币币转合约错误: %s' % e)
        self.update_account_info()

    def update_future_position(self):
        self.logger.info('全仓用户持仓查询')
        try:
            info = self.future_client.position(self.symbol, self.contract_type)
        except Exception as e:
            self.logger.error('全仓用户持仓查询异常: %s' % e)
        else:
            if info['result']:
                holding = info['holding']
                if len(holding) > 0:
                    # hold是数组
                    self.bull_amount = holding[0]['buy_amount']
                    self.bear_amount = holding[0]['sell_amount']
                    self.buy_available = holding[0]['buy_available']
                    self.sell_available = holding[0]['sell_available']
                    self.buy_price_avg = holding[0]['buy_price_avg']
                    self.sell_price_avg = holding[0]['sell_price_avg']
                    if self.bull_amount == 0 and self.bear_amount == 0:
                        self.logger.info('用户未持仓')
                        self.asset_balance()
                    else:
                        self.logger.info('多仓: %s\t空仓: %s' %
                                         (self.bull_amount, self.bear_amount))
                # 暂定认为len(holding)一定大于0
                # else:
                #     self.logger.info('用户未持仓')
                #     self.asset_balance()
            else:
                self.logger.info('postion_4fix result error')

    def update_account_info(self):
        self.logger.info('获取Future全仓账户信息')
        try:
            future_info = self.future_client.userinfo()
        except Exception as e:
            self.logger.error('获取Future全仓账户信息异常: %s' % e)
        else:
            if future_info['result']:
                btc_info = future_info['info']['btc']
                self.keep_deposit = btc_info['keep_deposit']
                self.risk_rate = btc_info['risk_rate']
                self.future_rights = btc_info['account_rights']
                self.profit_real = btc_info['profit_real']
                self.profit_unreal = btc_info['profit_unreal']
                self.logger.info('bond:%s\trights:%s' %
                                 (self.keep_deposit, self.future_rights))
        self.logger.info('获取spot账户信息')
        try:
            spot_info = self.spot_client.balances()
        except Exception as e:
            self.logger.error('获取spot账户信息异常: %s' % e)
        else:
            if spot_info['result']:
                self.spot_free_btc = float(
                    spot_info['info']['funds']['free']['btc'])
                self.spot_free_usdt = float(
                    spot_info['info']['funds']['free']['usdt'])
                self.spot_freezed_btc = float(
                    spot_info['info']['funds']['freezed']['btc'])
                self.spot_freezed_usdt = float(
                    spot_info['info']['funds']['freezed']['usdt'])
            self.logger.info('free_btc: %s\tfree_usdt: %s' %
                             (self.spot_free_btc, self.spot_free_usdt))

    def calc_available_contract(self, price):
        available_btc = self.future_rights * self.margin_coefficient - self.keep_deposit
        return int(available_btc * price / 10) if available_btc > 0 else 0

    def go(self):
        while True:
            self.logger.info('获取深度')
            try:
                future_depth = self.future_client.depth(
                    self.symbol, self.contract_type, 10)
            except Exception as e:
                self.logger.error('获取期货市场深度错误: %s' % e)
                time.sleep(3)
                continue
            future_bids = future_depth['bids']
            future_asks = future_depth['asks'][::-1]

            try:
                spot_depth = self.spot_client.depth(self.symbol, 10)
            except Exception as e:
                self.logger.error('获取现货市场深度错误: %s' % e)
                time.sleep(3)
                continue
            spot_bids = spot_depth['bids']
            spot_asks = spot_depth['asks'][::-1]

            self.logger.info(future_bids[0][0] - float(spot_asks[0][0]))
            self.logger.info(future_asks[0][0] - float(spot_bids[0][0]))

            # 开空
            if self.debug_type == 2 or future_bids[0][0] - float(spot_asks[0][0]) > self.open_positive_thd:
                self.logger.info('期货开空，现货买入')
                self.logger.info('期货价格: %s,现货价格: %s' %
                                 (future_bids[0][0], spot_asks[0][0]))

                avialable_bear_amount = self.calc_available_contract(
                    future_bids[0][0])

                if avialable_bear_amount == 0:
                    self.logger.info('可开合约数量不足')
                    continue
                self.logger.info('可开合约数量为: %s' % avialable_bear_amount)

                a = [(float(i[0]), float(i[1])) for i in spot_asks]
                spot_sum = np.sum(a[:3], axis=0)
                spot_std = np.std(a[:3], axis=0)
                if spot_sum[1] < 0.5 or spot_std[0] > 5:
                    self.logger.info('现货btc不足或标准差过大')
                    time.sleep(1)
                    continue
                future_contract_amount = min(future_bids[0][1], self.max_contract_exchange_amount,
                                             int(spot_sum[1] * float(spot_asks[0][0]) / 200))
                spot_usdt_amount = 100 * future_contract_amount

                if future_contract_amount > avialable_bear_amount:
                    future_contract_amount = avialable_bear_amount

                if spot_usdt_amount > self.spot_free_usdt:
                    self.logger.info('USDT数量:%s, 不足:%s,此单取消' % (
                        self.spot_free_usdt, spot_usdt_amount))
                    continue
                # 限价购买期货合约
                self.logger.info('期货开空%s' % future_contract_amount)
                price = future_bids[0][0]
                try:
                    future_order = self.future_client.place_order(self.symbol, self.contract_type, price,
                                                                  future_contract_amount, '2', '0')
                except Exception as e:
                    self.logger.error('Future订单异常: %s' % e)
                    continue
                print future_order

                orderid = future_order['order_id']
                try:
                    order_info = self.future_client.order_info(self.symbol, self.contract_type, 0,
                                                               orderid, 1, 5)
                except Exception as e:
                    self.logger.error('获取订单信息异常: %s,程序终止' % e)
                    break
                print order_info

                order_info = order_info['orders'][0]
                self.logger.info('order status: %s' % order_info['status'])
                # 等待成交或未成交
                if order_info['status'] == 0 or order_info['status'] == 1:
                    self.logger.info('撤销未完成委托')
                    try:
                        self.future_client.cancel(
                            self.symbol, self.contract_type, orderid)
                    except Exception as e:
                        self.logger.error('撤销异常: %s' % e)
                        self.logger.info('更新订单状态')
                        times = 0
                        while times < 10:
                            try:
                                order_info = self.future_client.order_info(self.symbol, self.contract_type,
                                                                           0, orderid, 1, 5)
                                order_info = order_info['orders'][0]
                                self.logger.info(
                                    '订单状态: %s' % order_info['status'])
                                # 若撤销失败，状态必定为完全成交
                                if order_info['status'] == 2:
                                    break
                            except Exception as e:
                                self.logger.error('查询订单信息异常: %s' % e)
                                times += 1
                                continue
                            times += 1
                        if times == 10:
                            self.logger.error('未知错误，程序终止')
                            break
                    else:
                        self.logger.info('撤销成功')
                        self.logger.info('更新订单状态')
                        try:
                            order_info = self.future_client.order_info(self.symbol, self.contract_type,
                                                                       0, orderid, 1, 5)
                            order_info = order_info['orders'][0]
                            self.logger.info('订单状态: %s' %
                                             order_info['status'])
                        except Exception as e:
                            self.logger.error(
                                '查询订单状态异常: %s,程序终止' % e)
                            break

                print order_info

                future_deal_contract_amount = order_info['deal_amount']
                # 未完成任何委托
                if future_deal_contract_amount == 0:
                    continue

                future_deal_price = order_info['price_avg']
                # fee精度是8位,fee默认是负数，改成习惯的正数
                future_deal_fee = -1 * float('%.8f' % order_info['fee'])
                # btc精度是8
                future_deal_btc_amount = float(
                    '%.8f' % (future_deal_contract_amount * 10 / future_deal_price))

                self.bear_amount += future_deal_contract_amount
                self.logger.info('future_deal_contract:%d\tfuture_deal_price:%s' % (
                    future_deal_contract_amount, future_deal_price
                ))

                # 市价买入现货
                spot_usdt_amount = 100 * future_deal_contract_amount
                self.logger.info('现货买入%s USDT' % spot_usdt_amount)
                try:
                    spot_order = self.spot_client.place_order(
                        '', spot_usdt_amount, 'buy_market', self.symbol)
                except Exception as e:
                    self.logger.error('spot订单异常: %s' % e)
                    self.logger.info('开始回滚')
                    # TODO
                    break

                print spot_order
                orderid = spot_order['order_id']
                times = 0
                while times < 20:
                    self.logger.info('第%s次确认订单信息' % (times + 1))
                    try:
                        order_info = self.spot_client.status_order(
                            self.symbol, orderid)
                    except Exception as e:
                        self.logger.error('查询订单信息异常: %s' % e)
                        times += 1
                        continue
                    print order_info

                    order_info = order_info['orders'][0]
                    # status:-1:已撤销  0:未成交  1:部分成交  2:完全成交 4:撤单处理中
                    if order_info['status'] == 2:
                        self.logger.info('spot buy filled')
                        spot_avg_price = order_info['avg_price']
                        spot_deal_amount = order_info['deal_amount']

                        self.logger.info(
                            'spot_deal_amount: %s\tspot_avg_price: %s' % (spot_deal_amount, spot_avg_price))
                        break

                    times += 1

                    if times == 19:
                        time.sleep(10)
                if times == 20:
                    self.logger.info('现货买入错误, 终止程序')
                    break
                self.update_account_info()
            # 开多
            if self.debug_type == 1 or future_asks[0][0] - float(spot_bids[0][0]) < self.open_negative_thd:
                self.logger.info('期货开多，现货卖出')
                self.logger.info('期货价格: %s,现货价格 %s' %
                                 (future_asks[0][0], spot_bids[0][0]))
                avialable_bull_amount = self.calc_available_contract(
                    future_asks[0][0])
                if avialable_bull_amount == 0:
                    self.logger.info('可开合约数量不足')
                    continue
                self.logger.info('可开合约数量: %s' % avialable_bull_amount)
                #print spot_bids
                a = [(float(i[0]), float(i[1])) for i in spot_bids]
                #print a
                spot_sum = np.sum(a[:3], axis=0)
                spot_std = np.std(a[:3], axis=0)
                self.logger.info('BIDS:\tsum:%10.4f\tstd:%10.4f' %
                                 (spot_sum[1], spot_std[0]))
                if spot_sum[1] < 0.5 or spot_std[0] > 5:
                    self.logger.info('标准差过大')
                    time.sleep(1)
                    continue
                future_contract_amount = min(self.max_contract_exchange_amount,
                                             avialable_bull_amount,
                                             future_bids[0][1],
                                             int(spot_sum[1] * float(spot_asks[0][0]) / 200))

                spot_limited_price = float(spot_bids[0][0]) - 5
                spot_btc_amount = float(
                    '%.4f' % (100 * future_contract_amount / spot_limited_price))

                if spot_btc_amount > self.spot_free_btc:
                    self.logger.info('现货BTC数量%s, 不足%s,交易取消' % (
                        self.spot_free_btc, spot_btc_amount))
                    continue
                self.logger.info('期货开多:%s' % future_contract_amount)
                # 限价购买期货合约
                price = future_asks[0][0] + 1
                try:
                    future_order = self.future_client.place_order(self.symbol, self.contract_type, price,
                                                                  future_contract_amount, '1', '0')
                except Exception as e:
                    self.logger.error('Future订单异常: %s' % e)
                    continue
                print future_order

                orderid = future_order['order_id']
                try:
                    order_info = self.future_client.order_info(self.symbol, self.contract_type, 0,
                                                               orderid, 1, 5)
                except Exception as e:
                    self.logger.error('获取订单信息异常: %s, 程序终止' % e)
                    break
                print order_info

                order_info = order_info['orders'][0]
                self.logger.info('order status: %s' % order_info['status'])
                # 等待成交或未成交
                if order_info['status'] == 0 or order_info['status'] == 1:
                    self.logger.info('撤销未完成委托')
                    try:
                        self.future_client.cancel(
                            self.symbol, self.contract_type, orderid)
                    except Exception as e:
                        self.logger.error('撤销异常: %s' % e)
                        self.logger.info('更新订单状态')
                        times = 0
                        while times < 10:
                            try:
                                order_info = self.future_client.order_info(self.symbol, self.contract_type,
                                                                           0, orderid, 1, 5)
                                order_info = order_info['orders'][0]
                                self.logger.info(
                                    '订单状态: %s' % order_info['status'])
                                # 若撤销失败，状态必定为完全成交
                                if order_info['status'] == 2:
                                    break
                            except Exception as e:
                                self.logger.error('查询订单信息异常: %s' % e)
                                times += 1
                                continue
                            times += 1
                        if times == 10:
                            self.logger.error('未知错误，程序终止')
                            break
                    else:
                        self.logger.info('撤销成功')
                        self.logger.info('更新订单状态')
                        try:
                            order_info = self.future_client.order_info(self.symbol, self.contract_type,
                                                                       0, orderid, 1, 5)
                            order_info = order_info['orders'][0]
                            self.logger.info('订单状态: %s' %
                                             order_info['status'])
                        except Exception as e:
                            self.logger.error(
                                '查询订单状态异常: %s,程序终止' % e)
                            break

                print order_info

                future_deal_contract_amount = order_info['deal_amount']
                # 未完成任何委托
                if future_deal_contract_amount == 0:
                    continue

                future_deal_price = order_info['price_avg']
                # fee精度是8位,fee默认是负数，改成习惯的正数
                future_deal_fee = -1 * float('%.8f' % order_info['fee'])
                # btc精度是8
                future_deal_btc_amount = float(
                    '%.8f' % (future_deal_contract_amount * 10 / future_deal_price))

                self.bull_amount += future_deal_contract_amount

                self.logger.info(
                    'future_deal:contract:%d\tbtc:%s\tfee:%s\tprice:%s' % (
                        future_deal_contract_amount, future_deal_btc_amount, future_deal_fee, future_deal_price
                    ))
                # 市价卖出现货
                self.logger.info('现货卖出')
                spot_limited_price = float(spot_bids[0][0]) - 5
                spot_btc_amount = float(
                    '%.4f' % (100 * future_deal_contract_amount / spot_limited_price))

                try:
                    spot_order = self.spot_client.place_order(spot_btc_amount, spot_limited_price, 'sell',
                                                              self.symbol)
                except Exception as e:
                    self.logger.error('spot订单异常: %s' % e)
                    self.logger.info('开始回滚')
                    # TODO
                    break

                print spot_order
                orderid = spot_order['order_id']
                times = 0
                while times < 20:
                    self.logger.info('第%s次确认订单信息' % (times + 1))
                    try:
                        order_info = self.spot_client.status_order(
                            self.symbol, orderid)
                    except Exception as e:
                        self.logger.error('查询订单信息异常: %s' % e)
                        times += 1
                        continue
                    print order_info

                    order_info = order_info['orders'][0]
                    # status:-1:已撤销  0:未成交  1:部分成交  2:完全成交 4:撤单处理中
                    if order_info['status'] == 2:
                        self.logger.info('spot sell filled')
                        spot_avg_price = order_info['avg_price']
                        spot_deal_amount = order_info['deal_amount']

                        self.logger.info(
                            'spot_deal_amount: %sBTC\tspot_deal_price: %s' % (spot_deal_amount, spot_avg_price))
                        break

                    times += 1

                    if times == 19:
                        time.sleep(10)
                if times == 20:
                    self.logger.info('现货卖出错误, 终止程序')
                    break
                self.update_account_info()
            # 平空
            if self.debug_type == 4 or future_asks[0][0] - float(spot_bids[0][0]) < self.close_positive_thd:
                if self.bear_amount == 0:
                    continue
                self.logger.info('期货平空，现货卖出')
                self.logger.info('期货价格: %s,现货价格 %s' %
                                 (future_asks[0][0], spot_bids[0][0]))
                self.logger.info('当前持空仓: %s' % self.bear_amount)

                a = [(float(i[0]), float(i[1])) for i in spot_bids]
                spot_sum = np.sum(a[:3], axis=0)
                spot_std = np.std(a[:3], axis=0)
                if spot_sum[1] < 0.5 or spot_std[0] > 5:
                    self.logger.info('现货btc不足或标准差过大')
                    time.sleep(1)
                    continue
                future_contract_amount = min(self.bear_amount, future_asks[0][1],
                                             spot_sum[1] * float(spot_bids[0][0]) / 200)

                spot_limited_price = float(spot_asks[0][0]) - 10
                spot_btc_amount = float(
                    '%.4f' % (100 * future_contract_amount / spot_limited_price))
                if spot_btc_amount > self.spot_free_btc:
                    self.logger.info('现货BTC数量：%s, 不足：%s,本单取消' % (
                        self.spot_free_btc, spot_btc_amount))
                    continue
                self.logger.info('期货平空%s' % future_contract_amount)

                price = future_asks[0][0]
                try:
                    future_order = self.future_client.place_order(self.symbol, self.contract_type, price,
                                                                  future_contract_amount, '4', '0')
                except Exception as e:
                    self.logger.error('Future订单异常: %s' % e)
                    continue
                print future_order

                orderid = future_order['order_id']
                try:
                    order_info = self.future_client.order_info(self.symbol, self.contract_type, 0,
                                                               orderid, 1, 5)
                except Exception as e:
                    self.logger.error('查询订单信息异常:%s, 程序终止' % e)
                    break

                order_info = order_info['orders'][0]
                self.logger.info('order status: %s' % order_info['status'])
                # 等待成交或未成交
                if order_info['status'] == 0 or order_info['status'] == 1:
                    self.logger.info('撤销未完成委托')
                    try:
                        self.future_client.cancel(
                            self.symbol, self.contract_type, orderid)
                    except Exception as e:
                        self.logger.error('撤销异常: %s' % e)
                        self.logger.info('更新订单状态')
                        times = 0
                        while times < 10:
                            try:
                                order_info = self.future_client.order_info(self.symbol, self.contract_type,
                                                                           0, orderid, 1, 5)
                                order_info = order_info['orders'][0]
                                self.logger.info(
                                    '订单状态: %s' % order_info['status'])
                                # 若撤销失败，状态必定为完全成交
                                if order_info['status'] == 2:
                                    break
                            except Exception as e:
                                self.logger.error('查询订单信息异常: %s' % e)
                                times += 1
                                continue
                            times += 1
                        if times == 10:
                            self.logger.error('未知错误，程序终止')
                            break
                    else:
                        self.logger.info('撤销成功')
                        self.logger.info('更新订单状态')
                        try:
                            order_info = self.future_client.order_info(self.symbol, self.contract_type,
                                                                       0, orderid, 1, 5)
                            order_info = order_info['orders'][0]
                            self.logger.info('订单状态: %s' %
                                             order_info['status'])
                        except Exception as e:
                            self.logger.error('查询订单信息异常: %s' % e)
                            break

                print order_info

                future_deal_contract_amount = order_info['deal_amount']
                # 未完成任何委托
                if future_deal_contract_amount == 0:
                    continue
                future_deal_price = order_info['price_avg']
                # fee精度是8位,fee默认是负数，改成习惯的正数
                future_deal_fee = -1 * float('%.8f' % order_info['fee'])
                # btc精度是8
                future_deal_btc_amount = float(
                    '%.8f' % (future_deal_contract_amount * 10 / future_deal_price))

                self.bear_amount -= future_deal_contract_amount

                self.logger.info('future_deal_contract:%d\tfuture_deal_price:%s' % (
                    future_deal_contract_amount, future_deal_price
                ))

                spot_limited_price = float(spot_asks[0][0]) - 10
                spot_btc_amount = float(
                    '%.4f' % (100 * future_deal_contract_amount / spot_limited_price))
                self.logger.info('现货卖出: %s' % spot_btc_amount)
                try:
                    spot_order = self.spot_client.place_order(
                        spot_btc_amount, spot_limited_price, 'sell', self.symbol)
                except Exception as e:
                    self.logger.error('spot订单异常: %s' % e)
                    self.logger.info('开始回滚')
                    # TODO
                    break

                print spot_order
                orderid = spot_order['order_id']
                times = 0
                while times < 20:
                    self.logger.info('第%s次确认订单信息' % (times + 1))
                    try:
                        order_info = self.spot_client.status_order(
                            self.symbol, orderid)
                    except Exception as e:
                        self.logger.error('查询订单信息异常: %s' % e)
                        times += 1
                        continue
                    print order_info

                    order_info = order_info['orders'][0]
                    # status:-1:已撤销  0:未成交  1:部分成交  2:完全成交 4:撤单处理中
                    if order_info['status'] == 2:
                        self.logger.info('spot sell filled')
                        spot_avg_price = order_info['avg_price']
                        spot_deal_amount = order_info['deal_amount']

                        self.logger.info(
                            'spot_deal_amount: %s\tspot_avg_price: %s' % (spot_deal_amount, spot_avg_price))
                        break

                    times += 1

                    if times == 19:
                        time.sleep(10)
                if times == 20:
                    self.logger.info('现货卖出错误, 终止程序')
                    break
                self.update_account_info()
                # 计算当前收益，并短信通知
                if self.bear_amount == 0:
                    self.asset_balance()

            # 平多
            if self.debug_type == 3 or future_bids[0][0] - float(spot_asks[0][0]) > self.close_negetive_thd:
                if self.bull_amount == 0:
                    continue
                self.logger.info('期货平多，现货买入')
                self.logger.info('期货价格: %s,现货价格 %s' %
                                 (future_bids[0][0], spot_asks[0][0]))
                self.logger.info('当前持多仓: %s' % self.bull_amount)

                a = [(float(i[0]), float(i[1])) for i in spot_asks]
                spot_sum = np.sum(a[:3], axis=0)
                spot_std = np.std(a[:3], axis=0)
                if spot_sum[1] < 0.10 or spot_std[0] > 5:
                    self.logger.info('现货btc不足或标准差过大')
                    time.sleep(1)
                    continue
                future_contract_amount = min(self.bull_amount, future_asks[0][1],
                                             spot_sum[1] * float(spot_asks[0][0]) / 200)

                spot_usdt_amount = 100 * future_contract_amount
                if spot_usdt_amount > self.spot_free_usdt:
                    self.logger.info('现货USDT数量:%s, 不足:%s,本单取消' % (
                        self.spot_free_usdt, spot_usdt_amount))
                    continue
                self.logger.info('期货平多%s' % future_contract_amount)
                # 限价购买期货合约
                price = future_bids[0][0]
                try:
                    future_order = self.future_client.place_order(self.symbol, self.contract_type, price,
                                                                  future_contract_amount, '3', 0)
                except Exception as e:
                    self.logger.error('Future订单异常: %s' % e)
                    continue
                print future_order

                orderid = future_order['order_id']
                try:
                    order_info = self.future_client.order_info(self.symbol, self.contract_type, 0,
                                                               orderid, 1, 5)
                except Exception as e:
                    self.logger.error('查询订单信息异常:%s,程序终止' % e)
                    break

                order_info = order_info['orders'][0]
                self.logger.info('order status: %s' % order_info['status'])
                # 等待成交或未成交
                if order_info['status'] == 0 or order_info['status'] == 1:
                    self.logger.info('撤销未完成委托')
                    try:
                        self.future_client.cancel(
                            self.symbol, self.contract_type, orderid)
                    except Exception as e:
                        self.logger.error('撤销异常: %s' % e)
                        self.logger.info('更新订单状态')
                        times = 0
                        while times < 10:
                            try:
                                order_info = self.future_client.order_info(self.symbol, self.contract_type,
                                                                           0, orderid, 1, 5)
                                order_info = order_info['orders'][0]
                                self.logger.info(
                                    '订单状态: %s' % order_info['status'])
                                # 若撤销失败，状态必定为完全成交
                                if order_info['status'] == 2:
                                    break
                            except Exception as e:
                                self.logger.error('查询订单信息异常: %s' % e)
                                times += 1
                                continue
                            times += 1
                        if times == 10:
                            self.logger.error('未知错误，程序终止')
                            break
                    else:
                        self.logger.info('撤销成功')
                        self.logger.info('更新订单状态')
                        try:
                            order_info = self.future_client.order_info(self.symbol, self.contract_type,
                                                                       0, orderid, 1, 5)
                            order_info = order_info['orders'][0]
                            self.logger.info('订单状态: %s' %
                                             order_info['status'])
                        except Exception as e:
                            self.logger.error('查询订单信息异常: %s' % e)
                            break

                print order_info

                future_deal_contract_amount = order_info['deal_amount']
                # 未完成任何委托
                if future_deal_contract_amount == 0:
                    continue
                future_deal_price = order_info['price_avg']
                # fee精度是8位,fee默认是负数，改成习惯的正数
                future_deal_fee = -1 * float('%.8f' % order_info['fee'])
                # btc精度是8
                future_deal_btc_amount = float(
                    '%.8f' % (future_deal_contract_amount * 10 / future_deal_price))

                self.bull_amount -= future_deal_contract_amount

                self.logger.info(
                    'future_deal:contract:%d\tbtc:%s\tfee:%s\tprice:%s' % (
                        future_deal_contract_amount, future_deal_btc_amount, future_deal_fee, future_deal_price
                    ))

                try:
                    spot_order = self.spot_client.place_order('', 100 * future_deal_contract_amount, 'buy_market',
                                                              self.symbol)
                except Exception as e:
                    self.logger.error('spot订单异常: %s' % e)
                    self.logger.info('开始回滚')
                    # TODO
                    break

                print spot_order
                orderid = spot_order['order_id']
                times = 0
                while times < 20:
                    self.logger.info('第%s次确认订单信息' % (times + 1))
                    try:
                        order_info = self.spot_client.status_order(
                            self.symbol, orderid)
                    except Exception as e:
                        self.logger.error('查询订单信息异常: %s' % e)
                    print order_info

                    order_info = order_info['orders'][0]
                    # status:-1:已撤销  0:未成交  1:部分成交  2:完全成交 4:撤单处理中
                    if order_info['status'] == 2:
                        self.logger.info('spot sell filled')
                        spot_avg_price = order_info['avg_price']
                        spot_deal_amount = order_info['deal_amount']
                        #spot_amount = order_info['amount']

                        self.logger.info(
                            'spot_deal_amount: %s\tspot_avg_price: %s' % (spot_deal_amount, spot_avg_price))
                        break

                    times += 1

                    if times == 19:
                        time.sleep(10)
                if times == 20:
                    self.logger.info('购买现货错误, 终止程序')
                    break
                self.update_account_info()
                # 计算当前收益，并短信通知
                if self.bull_amount == 0:
                    self.asset_balance()

            time.sleep(1)


if __name__ == '__main__':
    term = TermArbitrage()
    term.update_account_info()
    term.update_future_position()
    term.go()
