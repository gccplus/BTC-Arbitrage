# coding: utf-8

from __future__ import absolute_import
import requests
import hashlib
import time
import logging
import urllib
from okex_errors import OKEX_ERROR

PROTOCOL = "https"
HOST = "www.okex.com/api"
VERSION = "v1"

PATH_SYMBOLS = "symbols"  # 获取支持的币币交易类型
PATH_TICKER = "ticker.do"  # 获取币币交易行情
PATH_TRADES = "trades.do"  # 获取币币交易信息
PATH_DEPTH = "depth.do"  # 获取币币市场深度
PATH_ORDER_INFO = "order_info.do"  # 获取订单信息
PATH_ORDERS_INFO = "orders_info.do"  # 批量获取订单信息
PATH_ORDER_HISTORY = "order_history.do"  # 获取历史订单信息，只返回最近两天的信息
PATH_CANCEL_ORDER = "cancel_order.do"  # 撤销订单
PATH_BALANCES_USERINFO = "userinfo.do"  # 个人资产情况
PATH_TRADE = "trade.do"  # 获取币币交易信息

OPEN_LONG = '1'  # 开多
OPEN_SHORT = '2'  # 开空
CLOSE_LONG = '3'  # 平多
CLOSE_SHORT = '4'  # 平空

MATCH_PRICE_TRUE = '1'
MATCH_PRICE_FALSE = '0'

# HTTP request timeout in seconds
TIMEOUT = 10.0


class OkexAPIException(Exception):
    def __init__(self, json_res):
        self.code = json_res['error_code']
        try:
            self.message = OKEX_ERROR[str(self.code)]
        except KeyError:
            self.code = 3001
            self.message = 'Key Error, error_code: %s' % json_res['error_code']

    def __str__(self):  # pragma: no cover
        return 'APIError(code=%s): %s' % (self.code, self.message)


class OkexBaseClient(object):
    def __init__(self, key, secret, proxies=None):
        self.URL = "{0:s}://{1:s}/{2:s}".format(PROTOCOL, HOST, VERSION)
        self.KEY = key
        self.SECRET = secret
        self.PROXIES = proxies

    @property
    def _nonce(self):
        """
        Returns a nonce
        Used in authentication
        """
        return str(int(time.time() * 1000))

    def _build_parameters(self, parameters):
        # sort the keys so we can test easily in Python 3.3 (dicts are not
        # ordered)
        keys = list(parameters.keys())
        keys.sort()
        return '&'.join(["%s=%s" % (k, parameters[k]) for k in keys])

    def url_for(self, path, path_arg=None, parameters=None):
        url = "%s/%s" % (self.URL, path)
        # If there is a path_arh, interpolate it into the URL.
        # In this case the path that was provided will need to have string
        # interpolation characters in it
        if path_arg:
            url = url % (path_arg)
        # Append any parameters to the URL.
        if parameters:
            url = "%s?%s" % (url, self._build_parameters(parameters))
        return url

    def _sign_payload(self, payload):
        sign = ''
        for key in sorted(payload.keys()):
            sign += key + '=' + str(payload[key]) + '&'
        data = sign + 'secret_key=' + self.SECRET
        return hashlib.md5(data.encode("utf8")).hexdigest().upper()

    def _convert_to_floats(self, data):
        """
        Convert all values in a dict to floats at first level
        """
        for key, value in data.items():
            data[key] = float(value)
        return data

    def _get(self, url, timeout=TIMEOUT):
        req = requests.get(url, timeout=timeout, proxies=self.PROXIES)
        if req.status_code / 100 != 2:
            logging.error(u"Failed to request:%s %d headers:%s",
                          url, req.status_code, req.headers)
        return req.json()

    def _post(self, url, params=None, needsign=True, headers=None, timeout=TIMEOUT):
        req_params = {'api_key': self.KEY}
        if params and needsign:
            req_params.update(params)
        req_params['sign'] = self._sign_payload(req_params)

        req_headers = {
            "Content-type": "application/x-www-form-urlencoded",
        }
        if headers:
            req_headers.update(headers)

        req = requests.post(url, headers=req_headers, data=urllib.urlencode(req_params), timeout=TIMEOUT,
                            proxies=self.PROXIES)
        if req.status_code / 100 != 2:
            logging.error(u"Failed to request:%s %d headers:%s",
                          url, req.status_code, req.headers)
        jsonr = req.json()
        if 'error_code' in jsonr:
            raise OkexAPIException(jsonr)
        return jsonr


class OkexFutureClient(OkexBaseClient):
    def kline(self, symbol, type, contract_type):
        """
        # Request
        GET https://www.okex.com/api/v1/future_depth.do
        # Response
        [
            [
                1440308700000,
                233.37,
                233.48,
                233.37,
                233.48,
                52,
                22.2810015
            ],
            [
                1440308760000,
                233.38,
                233.38,
                233.27,
                233.37,
                186,
                79.70234956
            ]
        ]
        :return:

        返回值说明
        [
            1440308760000,	时间戳
            233.38,		开
            233.38,		高
            233.27,		低
            233.37,		收
            186,		交易量
            79.70234956		交易量转化BTC或LTC数量
        ]
        """
        params = {
            'symbol': symbol,
            'type': type,
            'contract_type': contract_type
        }
        return self._get(self.url_for('future_kline.do', parameters=params))

    def ticker(self, symbol, contract_type):
        """
        # Request
        GET https://www.okex.com/api/v1/future_ticker.do?symbol=btc_usd&contract_type=this_week
        # Response
        {
            "date":"1411627632",
            "ticker":{
                "last":409.2,
                "buy" :408.23,
                "sell":409.18,
                "high":432.0,
                "low":405.71,
                "vol":55168.0,
                "contract_id":20140926012,
                "unit_amount":100.0
            }
        }
        :return:
        """
        params = {
            'symbol': symbol,
            'contract_type': contract_type
        }
        return self._get(self.url_for('future_ticker.do', parameters=params))

    def depth(self, symbol, contract_type, size, merge=None):
        """
        # Request
        GET https://www.okex.com/api/v1/future_depth.do?symbol=btc_usd&contract_type=this_week
        # Response
        {
            "asks":[
                [411.8,6],
                [411.75,11],
                [411.6,22],
                [411.5,9],
                [411.3,16]
            ],
            "bids":[
                [410.65,12],
                [410.64,3],
                [410.19,15],
                [410.18,40],
                [410.09,10]
            ]
        }
        :return:
        """
        params = {
            'symbol': symbol,
            'contract_type': contract_type,
            'size': size,
        }
        if merge:
            params['merge'] = merge
        return self._get(self.url_for('future_depth.do', parameters=params))

    def index(self, symbol):
        """
        # Request
        GET https://www.okex.com/api/v1/future_index.do?symbol=btc_usd
        # Response
        {"future_index":471.0817}
        :param symbol:
        :return:
        """
        return self._get(self.url_for('future_index.do', parameters={'symbol': symbol}))

    def hold_amount(self, symbol, contract_type):
        """
        GET https://www.okex.com/api/v1/future_hold_amount.do?symbol=btc_usd&contract_type=this_week
        # Response
        [
            {
                "amount": 106856,
                "contract_name": "BTC0213"
            }
        ]
        :return:
        """
        params = {
            'symbol': symbol,
            'contract_type': contract_type
        }
        return self._get(self.url_for('future_hold_amount.do', parameters=params))

    def price_limit(self, symbol, contract_type):
        """
        # Request
        GET https://www.okex.com/api/v1/future_price_limit.do?symbol=btc_usd&contract_type=this_week
        # Response
        {"high":443.07,"low":417.09}
        :return:
        """
        params = {
            'symbol': symbol,
            'contract_type': contract_type
        }
        return self._get(self.url_for('future_price_limit.do', parameters=params))

    def position(self, symbol, contract_type):
        """
        获取用户持仓获取OKEX合约账户信息 （全仓）
        # Request
        POST https://www.okex.com/api/v1/future_position.do
        # Response
        {
            "force_liqu_price": "0.07",
            "holding": [
                {
                    "buy_amount": 1,
                    "buy_available": 0,
                    "buy_price_avg": 422.78,
                    "buy_price_cost": 422.78,
                    "buy_profit_real": -0.00007096,
                    "contract_id": 20141219012,
                    "contract_type": "this_week",
                    "create_date": 1418113356000,
                    "lever_rate": 10,
                    "sell_amount": 0,
                    "sell_available": 0,
                    "sell_price_avg": 0,
                    "sell_price_cost": 0,
                    "sell_profit_real": 0,
                    "symbol": "btc_usd"
                }
            ],
            "result": true
        }
        返回值说明
        buy_amount(double):多仓数量
        buy_available:多仓可平仓数量
        buy_price_avg(double):开仓平均价
        buy_price_cost(double):结算基准价
        buy_profit_real(double):多仓已实现盈余
        contract_id(long):合约id
        create_date(long):创建日期
        lever_rate:杠杆倍数
        sell_amount(double):空仓数量
        sell_available:空仓可平仓数量
        sell_price_avg(double):开仓平均价
        sell_price_cost(double):结算基准价
        sell_profit_real(double):空仓已实现盈余
        symbol:btc_usd   ltc_usd    eth_usd    etc_usd    bch_usd
        contract_type:合约类型
        force_liqu_price:预估爆仓价
        """
        payload = {
            'symbol': symbol,
            'contract_type': contract_type,
        }
        return self._post(self.url_for('future_position.do'), params=payload)

    def userinfo(self):
        """
        获取OKEx合约账户信息(全仓)
        # Request
        POST https://www.okex.com/api/v1/future_userinfo.do
        # Response
        {
            "info": {
                "btc": {
                    "account_rights": 1,
                    "keep_deposit": 0,
                    "profit_real": 3.33,
                    "profit_unreal": 0,
                    "risk_rate": 10000
                },
                "ltc": {
                    "account_rights": 2,
                    "keep_deposit": 2.22,
                    "profit_real": 3.33,
                    "profit_unreal": 2,
                    "risk_rate": 10000
                }
            },
            "result": true
        }
        返回值说明
        account_rights:账户权益
        keep_deposit：保证金
        profit_real：已实现盈亏
        profit_unreal：未实现盈亏
        risk_rate：保证金率
        """
        return self._post(self.url_for('future_userinfo.do'))

    def userinfo_4fix(self):
        """
        # Request
        POST https://www.okex.com/api/v1/future_userinfo.do
        # Response
        {
            "info": {
                "btc": {
                    "account_rights": 1,
                    "keep_deposit": 0,
                    "profit_real": 3.33,
                    "profit_unreal": 0,
                    "risk_rate": 10000
                },
                "ltc": {
                    "account_rights": 2,
                    "keep_deposit": 2.22,
                    "profit_real": 3.33,
                    "profit_unreal": 2,
                    "risk_rate": 10000
                }
            },
            "result": true
        }
        :return:
        """
        return self._post(self.url_for('future_userinfo_4fix.do'))

    def order_info(self, symbol, contract_type, status, order_id, current_page, page_length):
        """
        # Request
        POST https://www.okex.com/api/v1/future_order_info.do
        status 查询状态 1:未完成的订单 2:已经完成的订单
        order_id 订单ID -1:查询指定状态的订单，否则查询相应订单号的订单
        # Response
        {
          "orders":
             [
                {
                    "amount":111,
                    "contract_name":"LTC0815",
                    "create_date":1408076414000,
                    "deal_amount":1,
                    "fee":0,
                    "order_id":106837,
                    "price":1111,
                    "price_avg":0,
                    "status":"0",
                    "symbol":"ltc_usd",
                    "type":"1",
                    "unit_amount":100,
                    "lever_rate":10
                }
             ],
           "result":true
        }
        :return:
        """
        payload = {
            'symbol': symbol,
            'contract_type': contract_type,
            'status': status,
            'order_id': order_id,
            'current_page': current_page,
            'page_length': page_length
        }
        return self._post(self.url_for('future_order_info.do'), params=payload)

    def place_order(self, symbol, contract_type, price, amount, type, match_price, lever_rate=10):
        """
        # Request
        POST https://www.okex.com/api/v1/future_trade.do
        :type
        1:开多 2:开空 3:平多 4:平空
        :match_price
        是否为对手价 0:不是 1:是 ,当取值为1时,price无效
        # Response
        {
           "order_id":986,
           "result":true
        }
        :return:
        """
        payload = {
            'symbol': symbol,
            'contract_type': contract_type,
            'price': price,
            'amount': amount,
            'type': type,
            'match_price': match_price,
            'lever_rate': lever_rate
        }
        return self._post(self.url_for('future_trade.do'), params=payload)

    def cancel(self, symbol, contract_type, order_id):
        """
        # Request
        POST https://www.okex.com/api/v1/future_cancel.do
        # Response
        #多笔订单返回结果(成功订单ID,失败订单ID)
        {
            "error":"161251:20015", //161251订单id 20015错误id
            "success":"161256"
        }
        #单笔订单返回结果
        {
            "order_id":"161277",
            "result":true
        }
        result:订单交易成功或失败(用于单笔订单)
        order_id:订单ID(用于单笔订单)
        success:成功的订单ID(用于多笔订单)
        error:失败的订单ID后跟失败错误码(用户多笔订单)
        :return:
        """
        payload = {
            'symbol': symbol,
            'contract_type': contract_type,
            'order_id': order_id
        }
        return self._post(self.url_for('future_cancel.do'), params=payload)

    def position_4fix(self, symbol, contract_type, type=None):
        """
        # Request
        POST
        https: // www.okex.com / api / v1 / future_position_4fix.do
        # Response
        {
            "holding": [{
                "buy_amount": 10,
                "buy_available": 2,
                "buy_bond": 1.27832803,
                "buy_flatprice": "338.97",
                "buy_price_avg": 555.67966869,
                "buy_price_cost": 555.67966869,
                "buy_profit_lossratio": "13.52",
                "buy_profit_real": 0,
                "contract_id": 20140815012,
                "contract_type": "this_week",
                "create_date": 1408594176000,
                "sell_amount": 8,
                "sell_available": 2,
                "sell_bond": 0.24315591,
                "sell_flatprice": "671.15",
                "sell_price_avg": 567.04644056,
                "sell_price_cost": 567.04644056,
                "sell_profit_lossratio": "-45.04",
                "sell_profit_real": 0,
                "symbol": "btc_usd",
                "lever_rate": 10
            }],
            "result": true
        }
        """
        payload = {
            'symbol': symbol,
            'contract_type': contract_type,
        }
        if type:
            payload['type'] = type
        return self._post(self.url_for('future_position_4fix.do'), params=payload)

    def devolve(self, symbol, type, amount):
        """
        # Request
        POST https://www.okex.com/api/v1/future_devolve.do
        1：币币转合约 2：合约转币币
        # Response
        {
            "result":true
        }
        或
        {
            "error_code":20029,
            "result":false
        }
        :return:
        result:划转结果。若是划转失败，将给出错误码提示。
        """
        payload = {
            'symbol': symbol,
            'type': type,
            'amount': amount
        }
        return self._post(self.url_for('future_devolve.do'), params=payload)


class OkexSpotClient(OkexBaseClient):
    """
    Authenticated client for trading through Bitfinex API
    """

    def ticker(self, symbol):
        """
        GET /api/v1/ticker.do?symbol=ltc_btc

        GET https://www.okex.com/api/v1/ticker.do?symbol=ltc_btc
        {
            "date":"1410431279",
            "ticker":{
                "buy":"33.15",
                "high":"34.15",
                "last":"33.15",
                "low":"32.05",
                "sell":"33.16",
                "vol":"10532696.39199642"
            }
        }
        """

        return self._get(self.url_for(PATH_TICKER, parameters={'symbol': symbol}))

    def trades(self, symbol, since_tid=None):
        """
        GET /api/v1/trades.do
        GET https://www.okex.com/api/v1/trades.do?symbol=ltc_btc&since=7622718804
        [
            {
                "date": "1367130137",
                "date_ms": "1367130137000",
                "price": 787.71,
                "amount": 0.003,
                "tid": "230433",
                "type": "sell"
            }
        ]
        """
        params = {'symbol': symbol}
        if since_tid:
            params['since'] = since_tid
        return self._get(self.url_for(PATH_TRADES, parameters=params))

    def depth(self, symbol, size=200):
        """
        # Request
        GET https://www.okex.com/api/v1/depth.do?symbol=ltc_btc
        # Response
        {
            "asks": [
                [792, 5],
                [789.68, 0.018],
                [788.99, 0.042],
                [788.43, 0.036],
                [787.27, 0.02]
            ],
            "bids": [
                [787.1, 0.35],
                [787, 12.071],
                [786.5, 0.014],
                [786.2, 0.38],
                [786, 3.217],
                [785.3, 5.322],
                [785.04, 5.04]
            ]
        }
        """
        params = {'symbol': symbol}
        if size > 0:
            params['size'] = size
        return self._get(self.url_for(PATH_DEPTH, parameters=params))

    def place_order(self, amount, price, ord_type, symbol):
        """
        # Request
        POST https://www.okex.com/api/v1/trade.do
        # Response
        {"result":true,"order_id":123456}
        """
        # assert (isinstance(amount, str) and isinstance(price, str))

        # if ord_type not in ('buy', 'sell', 'buy_market', 'sell_market'):
        #     # 买卖类型： 限价单（buy/sell） 市价单（buy_market/sell_market）
        #     raise OkexClientError("Invaild order type")

        payload = {
            "symbol": symbol, "type": ord_type
        }
        if price:
            payload['price'] = price
        if amount:
            payload['amount'] = amount

        result = self._post(self.url_for(PATH_TRADE), params=payload)
        # if 'error_code' not in result and result['result'] and result['order_id']:
        return result
        # raise OkexClientError('Failed to place order:' + str(result))

    def status_order(self, symbol, order_id):
        """
        # Request
        order_id -1:未完成订单，否则查询相应订单号的订单
        POST https://www.okex.com/api/v1/order_info.do
        # Response
        {
            "result": true,
            "orders": [
                {
                    "amount": 0.1,
                    "avg_price": 0,
                    "create_date": 1418008467000,
                    "deal_amount": 0,
                    "order_id": 10000591,
                    "orders_id": 10000591,
                    "price": 500,
                    "status": 0,
                    "symbol": "btc_usd",
                    "type": "sell"
                }
            ]
        }
        amount:委托数量
        create_date: 委托时间
        avg_price:平均成交价
        deal_amount:成交数量
        order_id:订单ID
        orders_id:订单ID(不建议使用)
        price:委托价格
        status:-1:已撤销  0:未成交  1:部分成交  2:完全成交 4:撤单处理中
        type:buy_market:市价买入 / sell_market:市价卖出
        """
        payload = {
            "symbol": symbol, "order_id": order_id
        }
        result = self._post(self.url_for(PATH_ORDER_INFO), params=payload)
        # if result['result']:
        return result
        # raise OkexClientError('Failed to get order status:' + str(result))

    def cancel_order(self, symbol, order_id):
        '''
        # Request
        POST https://www.okex.com/api/v1/cancel_order.do
        order_id: 订单ID(多个订单ID中间以","分隔,一次最多允许撤消3个订单)
        # Response
        #多笔订单返回结果(成功订单ID,失败订单ID)
        {"success":"123456,123457","error":"123458,123459"}
        '''
        payload = {
            "symbol": symbol, "order_id": order_id
        }
        result = self._post(self.url_for(PATH_CANCEL_ORDER), params=payload)
        # if result['result']:
        return result
        # raise OkexClientError('Failed to cancal order:%s %s' % (symbol, order_id))

    def cancel_orders(self, symbol, order_ids):
        final_result = {'result': True, 'success': [], 'error': []}
        for i in range(0, len(order_ids), 3):
            three_order_ids = ",".join(order_ids[i:i + 3])
            tmp = self.cancel_order(symbol, three_order_ids)
            final_result['result'] &= tmp['result']
            final_result['success'].extend(tmp['success'].split(','))
            final_result['error'].extend(tmp['error'].split(','))
        return final_result

    def active_orders(self, symbol):
        """
        Fetch active orders
        """
        return self.status_order(symbol, -1)

    def history(self, symbol, status, limit=500):
        """
        # Request
        POST https://www.okex.com/api/v1/order_history.do
        status: 查询状态 0：未完成的订单 1：已经完成的订单 （最近两天的数据）
        # Response
        {
            "current_page": 1,
            "orders": [
                {
                    "amount": 0,
                    "avg_price": 0,
                    "create_date": 1405562100000,
                    "deal_amount": 0,
                    "order_id": 0,
                    "price": 0,
                    "status": 2,
                    "symbol": "btc_usd",
                    "type": "sell”
                }
            ],
            "page_length": 1,
            "result": true,
            "total": 3
        }
        status:-1:已撤销   0:未成交 1:部分成交 2:完全成交 4:撤单处理中
        type:buy_market:市价买入 / sell_market:市价卖出
        """
        PAGE_LENGTH = 200  # Okex限制 每页数据条数，最多不超过200
        final_result = []
        for page_index in range(int(limit / PAGE_LENGTH) + 1):
            payload = {
                "symbol": symbol,
                "status": status,
                "current_page": page_index,
                "page_length": PAGE_LENGTH,
            }
            result = self._post(self.url_for(
                PATH_ORDER_HISTORY), params=payload)
            if len(result['orders']) > 0:
                final_result.extend(result['orders'])
            else:
                break
        return final_result

    def balances(self):
        '''
        # Request
        POST https://www.okex.com/api/v1/userinfo.do
        # Response
        {
            "info": {
                "funds": {
                    "free": {
                        "btc": "0",
                        "usd": "0",
                        "ltc": "0",
                        "eth": "0"
                    },
                    "freezed": {
                        "btc": "0",
                        "usd": "0",
                        "ltc": "0",
                        "eth": "0"
                    }
                }
            },
            "result": true
        }
        '''
        payload = {
        }
        result = self._post(self.url_for(
            PATH_BALANCES_USERINFO), params=payload)
        return result


class OkexClient(OkexBaseClient):
    """
    Client for the Okex.com API.
    See https://www.okex.com/rest_api.html for API documentation.
    """

    def ticker(self, symbol):
        """
        GET /api/v1/ticker.do?symbol=ltc_btc

        GET https://www.okex.com/api/v1/ticker.do?symbol=ltc_btc
        {
            "date":"1410431279",
            "ticker":{
                "buy":"33.15",
                "high":"34.15",
                "last":"33.15",
                "low":"32.05",
                "sell":"33.16",
                "vol":"10532696.39199642"
            }
        }
        """
        return self._get(self.url_for(PATH_TICKER, parameters={'symbol': symbol}))

    def trades(self, symbol, since_tid=None):
        """
        GET /api/v1/trades.do
        GET https://www.okex.com/api/v1/trades.do?symbol=ltc_btc&since=7622718804
        [
            {
                "date": "1367130137",
                "date_ms": "1367130137000",
                "price": 787.71,
                "amount": 0.003,
                "tid": "230433",
                "type": "sell"
            }
        ]
        """
        params = {'symbol': symbol}
        if since_tid:
            params['since'] = since_tid
        return self._get(self.url_for(PATH_TRADES, parameters=params))

    def depth(self, symbol, size=200):
        '''
        # Request
        GET https://www.okex.com/api/v1/depth.do?symbol=ltc_btc
        # Response
        {
            "asks": [
                [792, 5],
                [789.68, 0.018],
                [788.99, 0.042],
                [788.43, 0.036],
                [787.27, 0.02]
            ],
            "bids": [
                [787.1, 0.35],
                [787, 12.071],
                [786.5, 0.014],
                [786.2, 0.38],
                [786, 3.217],
                [785.3, 5.322],
                [785.04, 5.04]
            ]
        }
        '''
        params = {'symbol': symbol}
        if size > 0:
            params['size'] = size
        return self._get(self.url_for(PATH_DEPTH, parameters=params))
