#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2017-12-15 15:40:03
# @Author  : KlausQiu
# @QQ      : 375235513
# @github  : https://github.com/KlausQIU
import base64
import hmac
import hashlib
import json

import urllib
import datetime
import requests
import urlparse


class HuobiSpot:
    def __init__(self, api_key, api_secret):
        # timeout in 5 seconds:
        self.TIMEOUT = 5

        self.API_HOST = "api.huobi.pro"

        self.SCHEME = 'https'

        # language setting: 'zh-CN', 'en':
        self.LANG = 'zh-CN'

        self.DEFAULT_GET_HEADERS = {
            'Accept': 'application/json',
            'Accept-Language': self.LANG,
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:53.0) Gecko/20100101 Firefox/53.0'
        }

        self.EFAULT_POST_HEADERS = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Accept-Language': self.LANG,
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:53.0) Gecko/20100101 Firefox/53.0'
        }
        # 此处填写APIKEY

        self.ACCESS_KEY = api_key
        self.SECRET_KEY = api_secret

        # 首次运行可通过get_accounts()获取acct_id,然后直接赋值,减少重复获取。
        self.ACCOUNT_ID = None

        # API 请求地址
        self.MARKET_URL = self.TRADE_URL = "https://api.huobi.pro"

        # 各种请求,获取数据方式

    def http_get_request(self, url, params, add_to_headers=None):
        headers = {
            "Content-type": "application/x-www-form-urlencoded",
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:53.0) Gecko/20100101 Firefox/53.0'
        }
        if add_to_headers:
            headers.update(add_to_headers)
        postdata = urllib.urlencode(params)
        try:
            response = requests.get(url, postdata, headers=headers, timeout=self.TIMEOUT)
            if response.status_code == 200:
                return response.json()
            else:
                return {"status": "fail"}
        except Exception as e:
            # print("httpGet failed, detail is:%s" % e)
            return {"status": "fail", "msg": e}

    def http_post_request(self, url, params, add_to_headers=None):
        headers = {
            "Accept": "application/json",
            'Content-Type': 'application/json',
            "User-Agent": "Chrome/39.0.2171.71",
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:53.0) Gecko/20100101 Firefox/53.0'
        }
        if add_to_headers:
            headers.update(add_to_headers)
        postdata = json.dumps(params)
        try:
            response = requests.post(url, postdata, headers=headers, timeout=self.TIMEOUT)
            if response.status_code == 200:
                return response.json()
            else:
                return response.json()
        except Exception as e:
            # print("httpPost failed, detail is:%s" % e)
            return {"status": "fail", "msg": e}

    def api_key_get(self, params, request_path):
        method = 'GET'
        timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        params.update({'AccessKeyId': self.ACCESS_KEY,
                       'SignatureMethod': 'HmacSHA256',
                       'SignatureVersion': '2',
                       'Timestamp': timestamp})

        host_name = host_url = self.TRADE_URL
        host_name = urlparse.urlparse(host_url).hostname
        host_name = host_name.lower()

        params['Signature'] = self.createSign(params, method, host_name, request_path, self.SECRET_KEY)
        url = host_url + request_path
        return self.http_get_request(url, params)

    def api_key_post(self, params, request_path):
        method = 'POST'
        timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        params_to_sign = {'AccessKeyId': self.ACCESS_KEY,
                          'SignatureMethod': 'HmacSHA256',
                          'SignatureVersion': '2',
                          'Timestamp': timestamp}

        host_url = self.TRADE_URL
        host_name = urlparse.urlparse(host_url).hostname
        host_name = host_name.lower()
        params_to_sign['Signature'] = self.createSign(params_to_sign, method, host_name, request_path, self.SECRET_KEY)
        url = host_url + request_path + '?' + urllib.urlencode(params_to_sign)
        return self.http_post_request(url, params)

    def createSign(self, pParams, method, host_url, request_path, secret_key):
        sorted_params = sorted(pParams.items(), key=lambda d: d[0], reverse=False)
        encode_params = urllib.urlencode(sorted_params)
        payload = [method, host_url, request_path, encode_params]
        payload = '\n'.join(payload)
        payload = payload.encode(encoding='UTF8')
        secret_key = secret_key.encode(encoding='UTF8')
        digest = hmac.new(secret_key, payload, digestmod=hashlib.sha256).digest()
        signature = base64.b64encode(digest)
        signature = signature.decode()
        return signature

    '''
    Market data API
    '''

    # 获取KLine
    def get_kline(self, symbol, period, size):
        """
        :param symbol
        :param period: 可选值：{1min, 5min, 15min, 30min, 60min, 1day, 1mon, 1week, 1year }
        :param size: [1,2000]
        :return:
        """
        params = {'symbol': symbol,
                  'period': period,
                  'size': size}

        url = self.MARKET_URL + '/market/history/kline'
        return self.http_get_request(url, params)

    # 获取marketdepth
    def get_depth(self, symbol, type):
        """
        :param symbol:
        :param type: 可选值：{ percent10, step0, step1, step2, step3, step4, step5 }
        :return:
        """
        params = {'symbol': symbol,
                  'type': type}

        url = self.MARKET_URL + '/market/depth'
        return self.http_get_request(url, params)

    # 获取tradedetail
    def get_trade(self, symbol):
        """
        :param symbol: 可选值：{ ethcny }
        :return:
        """
        params = {'symbol': symbol}

        url = self.MARKET_URL + '/market/trade'
        return self.http_get_request(url, params)

    # 获取 Market Detail 24小时成交量数据
    def get_detail(self, symbol):
        """
        :param symbol: 可选值：{ ethcny }
        :return:
        """
        params = {'symbol': symbol}

        url = self.MARKET_URL + '/market/detail'
        return self.http_get_request(url, params)

    # 查询系统支持的所有交易对

    def get_symbols(self):
        """
        :return:
        """
        url = self.MARKET_URL + '/v1/common/symbols'
        params = {}
        return self.http_get_request(url, params)

    '''
    Trade/Account API
    '''

    def get_accounts(self):
        """
        :return:
        """
        path = "/v1/account/accounts"
        params = {}
        return self.api_key_get(params, path)

    # ACCOUNT_ID = 0
    def get_hbpoint(self):
        try:
            accounts = self.get_accounts()
            acct_id = accounts['data'][3]['id']
        except BaseException as e:
            print 'get user_id error.%s' % e
        else:
            url = "/v1/account/accounts/{0}/balance".format(acct_id)
            params = {"account-id": acct_id}
            return self.api_key_get(params, url)

    # 获取当前账户资产
    def get_balance(self, acct_id=None):
        """
        :param acct_id
        :return:
        """
        # global ACCOUNT_ID

        if not acct_id:
            try:
                accounts = self.get_accounts()
                acct_id = self.ACCOUNT_ID = accounts['data'][0]['id']
            except BaseException as e:
                print 'get acct_id error.%s' % e
                acct_id = self.ACCOUNT_ID

        url = "/v1/account/accounts/{0}/balance".format(acct_id)
        params = {"account-id": acct_id}
        return self.api_key_get(params, url)

    # 创建并执行订单

    def send_order(self, amount, source, symbol, _type, price=0):
        """
        :param amount:
        :param source: 如果使用借贷资产交易，请在下单接口,请求参数source中填写'margin-api'
        :param symbol:
        :param _type: 可选值 {buy-market：市价买, sell-market：市价卖, buy-limit：限价买, sell-limit：限价卖}
        :param price:
        :return:
        """
        # try:
        #     accounts = self.get_accounts()
        #     acct_id = accounts['data'][0]['id']
        # except BaseException as e:
        #     print 'get acct_id error.%s' % e
        #     acct_id = ACCOUNT_ID

        params = {"account-id": self.ACCOUNT_ID,
                  "amount": amount,
                  "symbol": symbol,
                  "type": _type,
                  "source": source}
        if price:
            params["price"] = price

        url = '/v1/order/orders/place'
        return self.api_key_post(params, url)

    # 撤销订单
    def cancel_order(self, order_id):
        """

        :param order_id:
        :return:
        """
        params = {}
        url = "/v1/order/orders/{0}/submitcancel".format(order_id)
        return self.api_key_post(params, url)

    # 查询某个订单
    def order_info(self, order_id):
        """

        :param order_id:
        :return:
        """
        params = {}
        url = "/v1/order/orders/{0}".format(order_id)
        return self.api_key_get(params, url)

    # 查询某个订单的成交明细
    def order_matchresults(self, order_id):
        """

        :param order_id:
        :return:
        """
        params = {}
        url = "/v1/order/orders/{0}/matchresults".format(order_id)
        return self.api_key_get(params, url)

    # 查询当前委托、历史委托
    def orders_list(self, symbol, states, types=None, start_date=None, end_date=None, _from=None, direct=None,
                    size=None):
        """
        :param symbol:
        :param states: 可选值 {pre-submitted 准备提交, submitted 已提交, partial-filled 部分成交, partial-canceled 部分成交撤销, filled 完全成交, canceled 已撤销}
        :param types: 可选值 {buy-market：市价买, sell-market：市价卖, buy-limit：限价买, sell-limit：限价卖}
        :param start_date:
        :param end_date:
        :param _from:
        :param direct: 可选值{prev 向前，next 向后}
        :param size:
        :return:
        """
        params = {'symbol': symbol,
                  'states': states}

        if types:
            params[types] = types
        if start_date:
            params['start-date'] = start_date
        if end_date:
            params['end-date'] = end_date
        if _from:
            params['from'] = _from
        if direct:
            params['direct'] = direct
        if size:
            params['size'] = size
        url = '/v1/order/orders'
        return self.api_key_get(params, url)

    # 查询当前成交、历史成交
    def orders_matchresults(self, symbol, types=None, start_date=None, end_date=None, _from=None, direct=None,
                            size=None):
        """
        :param symbol:
        :param types: 可选值 {buy-market：市价买, sell-market：市价卖, buy-limit：限价买, sell-limit：限价卖}
        :param start_date:
        :param end_date:
        :param _from:
        :param direct: 可选值{prev 向前，next 向后}
        :param size:
        :return:
        """
        params = {'symbol': symbol}

        if types:
            params[types] = types
        if start_date:
            params['start-date'] = start_date
        if end_date:
            params['end-date'] = end_date
        if _from:
            params['from'] = _from
        if direct:
            params['direct'] = direct
        if size:
            params['size'] = size
        url = '/v1/order/matchresults'
        return self.api_key_get(params, url)

    # 申请提现虚拟币
    def withdraw(self, address, amount, currency, fee=0, addr_tag=""):
        """

        :param address_id:
        :param amount:
        :param currency:btc, ltc, bcc, eth, etc ...(火币Pro支持的币种)
        :param fee:
        :param addr_tag:
        :return: {
                  "status": "ok",
                  "data": 700
                }
        """
        params = {'address': address,
                  'amount': amount,
                  "currency": currency,
                  "fee": fee,
                  "addr-tag": addr_tag}
        url = '/v1/dw/withdraw/api/create'

        return self.api_key_post(params, url)

    # 申请取消提现虚拟币

    def cancel_withdraw(self, address_id):
        """

        :param address_id:
        :return: {
                  "status": "ok",
                  "data": 700
                }
        """
        params = {}
        url = '/v1/dw/withdraw-virtual/{0}/cancel'.format(address_id)

        return self.api_key_post(params, url)

    '''
    借贷API
    '''

    # 创建并执行借贷订单
    def send_margin_order(self, amount, source, symbol, _type, price=0):
        """
        :param amount:
        :param source: 'margin-api'
        :param symbol:
        :param _type: 可选值 {buy-market：市价买, sell-market：市价卖, buy-limit：限价买, sell-limit：限价卖}
        :param price:
        :return:
        """
        # try:
        #     accounts = self.get_accounts()
        #     acct_id = accounts['data'][0]['id']
        # except BaseException as e:
        #     print 'get acct_id error.%s' % e
        #     acct_id = ACCOUNT_ID

        params = {"account-id": self.ACCOUNT_ID,
                  "amount": amount,
                  "symbol": symbol,
                  "type": _type,
                  "source": 'margin-api'}
        if price:
            params["price"] = price

        url = '/v1/order/orders/place'
        return self.api_key_post(params, url)

    # 现货账户划入至借贷账户
    def exchange_to_margin(self, symbol, currency, amount):
        """
        :param amount:
        :param currency:
        :param symbol:
        :return:
        """
        params = {"symbol": symbol,
                  "currency": currency,
                  "amount": amount}

        url = "/v1/dw/transfer-in/margin"
        return self.api_key_post(params, url)

    # 借贷账户划出至现货账户
    def margin_to_exchange(self, symbol, currency, amount):
        """
        :param amount:
        :param currency:
        :param symbol:
        :return:
        """
        params = {"symbol": symbol,
                  "currency": currency,
                  "amount": amount}

        url = "/v1/dw/transfer-out/margin"
        return self.api_key_post(params, url)

    # 申请借贷
    def get_margin(self, symbol, currency, amount):
        """
        :param amount:
        :param currency:
        :param symbol:
        :return:
        """
        params = {"symbol": symbol,
                  "currency": currency,
                  "amount": amount}
        url = "/v1/margin/orders"
        return self.api_key_post(params, url)

    # 归还借贷
    def repay_margin(self, order_id, amount):
        """
        :param order_id:
        :param amount:
        :return:
        """
        params = {"order-id": order_id,
                  "amount": amount}
        url = "/v1/margin/orders/{0}/repay".format(order_id)
        return self.api_key_post(params, url)

    # 借贷订单
    def loan_orders(self, symbol, currency, start_date="", end_date="", start="", direct="", size=""):
        """
        :param symbol:
        :param currency:
        :param direct: prev 向前，next 向后
        :return:
        """
        params = {"symbol": symbol,
                  "currency": currency}
        if start_date:
            params["start-date"] = start_date
        if end_date:
            params["end-date"] = end_date
        if start:
            params["from"] = start
        if direct and direct in ["prev", "next"]:
            params["direct"] = direct
        if size:
            params["size"] = size
        url = "/v1/margin/loan-orders"
        return self.api_key_get(params, url)

    # 借贷账户详情,支持查询单个币种
    def margin_balance(self, symbol):
        """
        :param symbol:
        :return:
        """
        params = {}
        url = "/v1/margin/accounts/balance"
        if symbol:
            params['symbol'] = symbol

        return self.api_key_get(params, url)


if __name__ == '__main__':
    key_dict = {}
    with open('config', 'r') as f:
        for line in f.readlines():
            splited = line.split('=')
            if len(splited) == 2:
                key_dict[splited[0].strip()] = splited[1].strip()
    huobispot = HuobiSpot(key_dict['HUOBI_ACCESS_KEY'], key_dict['HUOBI_SECRET_KEY'])
    print huobispot.get_balance()
    # h_depth = huobispot.get_depth('btcusdt', 'step5')
    # h_bids = h_depth['tick']['bids']
    # # h_asks = h_depth['tick']['asks']
    # print h_bids
    # # print h_asks
    # print huobispot.get_hbpoint()
    # print huobispot.get_symbols()
    # # #
    # # import numpy as np
    # # print np.sum(h_bids[:5], axis=0)
    # # print np.std(h_bids[:10], axis=0)
    # import json
    #
    jjj = huobispot.send_order(0.0011, 'api', 'btcusdt', 'buy-limit', 5000)
    print jjj
    # print jjj
    # # 请教各位大神一个api问题
    # kkk = huobispot.send_order('1', 'api', 'btcusdt', 'buy-market')
    # print kkk
    # if jjj['status'] == 'ok':
    #     order_id = jjj['data']
    #     order_result = huobispot.order_info(order_id)
    #     print order_result
    #     order_result2 = huobispot.order_matchresults(order_id)
    #     print order_result2

    # 以上这个市价买的订单，第一个参数amount只能以usdt为单位，不能以btc为单位么？
    # kkk = huobispot.send_order('1.0001', 'api', 'btcusdt', 'buy-limit', 3000)
    # print kkk
    # print jjj
    # order_id = jjj['data']
    # print order_id
    # kkk = huobispot.order_matchresults(order_id)
    # mmm = huobispot.order_info('2301939102')
    # print mmm
    # kkk = huobispot.cancel_order('2301939102')
    # print kkk
    # = huobispot.order_info('2495142037')
    # print mmm
    # if mmm['data']['finished-at'] != 0:
    #     print mmm
    # print mmm
    # # print kkk
    # # print mmm
    # #     kkk = huobispot.cancel_order('1909633099')
    # #     print kkk
    # #     kkk = huobispot.order_info('1909633099')
    # #     print kkk
    # # print huobispot.order_matchresults('1850512770')
    # # print huobispot.orders_list('btcusdt', 'filled')
    # # order_id = '1944634118'
    # # print huobispot.cancel_order(order_id)
    # r = huobispot.get_symbols()
    # for item in r['data']:
    #     if item['base-currency'] == 'btc' and item['quote-currency'] == 'usdt':
    #         print item
    #         break
