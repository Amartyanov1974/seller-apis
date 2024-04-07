import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получение списка товаров

    Используя методы API Яндекс Маркета получаем список товаров.

    Args:
        page (str): идентификатор страницы c результатами.
                    Если параметр не указан, возвращается
                    самая старая страница.
        campaign_id (int): идентификатор компании в API Яндекс Маркет.
        access_token (str): токен для авторизации для запросов к Маркету.

    Return:
        dict: Список товаров.

    Пример:
        >>> get_product_list(page, campaign_id, access_token)
        >>> {
        >>>     "paging": {
        >>>         "nextPageToken": "string",
        >>>         "prevPageToken": "string"
        >>>     },
        >>>     "offerMappingEntries": [
        >>>         {
        >>>             "offer": {
        >>>                 "name": "Ударная дрель Makita HP1630, 710 Вт",
        >>>                 "shopSku": "string",
        >>>                 "category": "string",
        >>>                 "vendor": "LEVENHUK",
        >>>                 "vendorCode": "VNDR-0005A",
        >>>                 "description": "string",
        >>>                 "id": "string",
        >>>                 "feedId": 0,
        >>>                 "barcodes": [
        >>>                     46012300000000
        >>>                 ],
        >>>                 "urls": [
        >>>                     "string"
        >>>                 ],
        >>>                 "pictures": [
        >>>                     "string"
        >>>                 ],
        >>>                 "manufacturer": "string",
        >>>                 "manufacturerCountries": [
        >>>                     "string"
        >>>                 ],
        >>>                 "minShipment": 0,
        >>>                 "transportUnitSize": 0,
        >>>                 "quantumOfSupply": 0,
        >>>                 "deliveryDurationDays": 0,
        >>>                 "boxCount": 0,
        >>>                 "customsCommodityCodes": [
        >>>                     "string"
        >>>                 ],
        >>>                 "weightDimensions": {
        >>>                     "length": 65.55,
        >>>                     "width": 50.7,
        >>>                     "height": 20,
        >>>                     "weight": 1.001
        >>>                 },
        >>>                 "supplyScheduleDays": [
        >>>                     "MONDAY"
        >>>                 ],
        >>>                 "shelfLifeDays": 0,
        >>>                 "lifeTimeDays": 0,
        >>>                 "guaranteePeriodDays": 0,
        >>>                 "processingState": {
        >>>                     "status": "UNKNOWN",
        >>>                     "notes": [
        >>>                         {
        >>>                             "type": "ASSORTMENT",
        >>>                             "payload": "string"
        >>>                         }
        >>>                     ]
        >>>                 },
        >>>                 "availability": "ACTIVE",
        >>>                 "shelfLife": {
        >>>                     "timePeriod": 0,
        >>>                     "timeUnit": "HOUR",
        >>>                     "comment": "string"
        >>>                 },
        >>>                 "lifeTime": {
        >>>                     "timePeriod": 0,
        >>>                     "timeUnit": "HOUR",
        >>>                     "comment": "string"
        >>>                 },
        >>>                 "guaranteePeriod": {
        >>>                     "timePeriod": 0,
        >>>                     "timeUnit": "HOUR",
        >>>                     "comment": "string"
        >>>                 },
        >>>                 "certificate": "string",
        >>>                 "price": 0
        >>>             },
        >>>             "mapping": {
        >>>                 "marketSku": 0,
        >>>                 "modelId": 0,
        >>>                 "categoryId": 0
        >>>             },
        >>>             "awaitingModerationMapping": {
        >>>                 "marketSku": 0,
        >>>                 "modelId": 0,
        >>>                 "categoryId": 0
        >>>             },
        >>>             "rejectedMapping": {
        >>>                 "marketSku": 0,
        >>>                 "modelId": 0,
        >>>                 "categoryId": 0
        >>>             }
        >>>         }
        >>>     ]
        >>> }

    Raises:
        HTTPError: 404 (если сервер будет недоступен).
    Пример:
        >>> get_product_list(page, campaign_id, access_token)
        >>> requests.exceptions.HTTPError: 404 Client Error: Not Found for url
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Обновляем остатки в магазине Яндекс Маркет

    Используя методы API Яндекс Маркет обновляем информацию
    о количестве товара в наличии. За один запрос можно изменить
    наличие для 100 товаров.

    Args:
        stocks (list): список количества остатков.
        campaign_id (int): идентификатор компании в API Яндекс Маркет.
        access_token (str): токен для авторизации для запросов к Маркету.

    Return:
        dict: Словарь с результатами обновления.

    Пример:
        >>> stocks = [{"sku": "73309", "warehouseId": "string", "items": [{"count": 2, "type": "FIT", "updatedAt": "2022-12-29T18:02:01Z",}],}]
        >>> update_stocks(stocks, campaign_id, access_token)
        >>> {
        >>>     "skus": [
        >>>         {
        >>>             "sku": "73309",
        >>>             "items": [
        >>>                 {
        >>>                     "count": 2,
        >>>                     "updatedAt": "2022-12-29T18:02:01Z"
        >>>                 }
        >>>             ]
        >>>         }
        >>>     ]
        >>> }

    Raises:
        HTTPError: 404 (если сервер будет недоступен).
    Пример:
        >>> update_stocks(stocks, campaign_id, access_token)
        >>> requests.exceptions.HTTPError: 404 Client Error: Not Found for url

    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Обновляем стоимость товаров

    Используя методы API Яндекс Маркета обновляем информацию
    о стоимости товара.

    Args:
        prices (list): список количества остатков.
        campaign_id (int): идентификатор компании в API Яндекс Маркет.
        access_token (str): токен для авторизации для запросов к Маркету.

    Return:
        dict: Словарь с результатами обновления.

    Пример:
        >>> stocks = [{"id": "73309", "price": {"value": 38440, "currencyId": "RUR",},}]
        >>> update_price(prices, campaign_id, access_token)
        >>> {
        >>>     "offers": [
        >>>         {
        >>>             "offerId": "73309",
        >>>             "price": {
        >>>                 "value": 38440,
        >>>                 "currencyId": "RUR",
        >>>                 "discountBase": 0
        >>>             }
        >>>         }
        >>>     ]
        >>> }

    Raises:
        HTTPError: 404 (если сервер будет недоступен).
    Пример:
        >>> update_price(prices, campaign_id, access_token)
        >>> requests.exceptions.HTTPError: 404 Client Error: Not Found for url
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получаем артикулы товаров Яндекс маркета

    Из списка товаров извлекаем артикулы товара.

    Args:
        campaign_id (int): идентификатор компании в API Яндекс Маркет.
        market_token (str): токен для авторизации для запросов к Маркету.

    Return:
        list: Список товаров.

    Пример:
        >>> get_offer_ids(campaign_id, market_token)
        >>> ["136748"]

    Raises:
        AttributeError: 'NoneType' object has no attribute 'get'
        если функция get_product_list вернет None

    Пример:
        >>> get_offer_ids(campaign_id, market_token)
        >>> AttributeError: 'NoneType' object has no attribute 'get'
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Актуализируем остатки товара

    Оставляем в списке товара только те позиции, которые присутствуют
    в Яндекс Маркет и добавляем отсутствующие позиции в остатки из
    имеющихся в Маркете.

    Args:
        watch_remnants (list): остатки товара в магазине часов
        offer_ids (list): список товара в магазине Озон

    Return:
        list: Актуализированные остатки товара.

    Пример:
        >>> watch_remnants = [{'Код': 73309, 'Наименование товара': 'BM7334-66L', 'Изображение': 'Показать', 'Цена': "38'440.00 руб.", 'Количество': 2, 'Заказ': ''}]
        >>> offer_ids = ["73309"]
        >>> create_stocks(watch_remnants, offer_ids, warehouse_id)
        >>> [{"sku": "73309", "warehouseId": "string", "items": [{"count": 2, "type": "FIT", "updatedAt": "string",}],}]

    Raises:
        ValueError: Если в "Количестве" будут не только цифры

    Пример:
        >>> watch_remnants = [{'Код': 73309, 'Наименование товара': 'BM7334-66L', 'Изображение': 'Показать', 'Цена': "38'440.00 руб.", 'Количество': '2O', 'Заказ': ''}]
        >>> offer_ids = ["136748", "73309"]
        >>> create_stocks(watch_remnants, offer_ids, warehouse_id)
        >>> ValueError: invalid literal for int() with base 10: '2O'

    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Готовим список для обновления прайса

    Готовим список словарей для обновления прайса в Яндекс Маркете

    Args:
        watch_remnants (list): остатки товара в магазине часов
        offer_ids (list): список товара в Яндекс Маркете

    Return:
        list: Актуализированная стоимость товара.

    Пример:
        >>> watch_remnants = [{'Код': 73309, 'Наименование товара': 'BM7334-66L', 'Изображение': 'Показать', 'Цена': "38'440.00 руб.", 'Количество': 2, 'Заказ': ''}]
        >>> offer_ids = ["136748",  "73309"]
        >>> create_prices(watch_remnants, offer_ids)
        >>> [{"auto_action_enabled": "UNKNOWN", "currency_code": "RUB", "offer_id": "73309", "old_price": "0", "price": "38440"}]
        >>> [{"id": "73309", "price": {"value": 38440, "currencyId": "RUR",},}]
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Функция в работе программы участия не принимает"""

    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Функция в работе программы участия не принимает"""

    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
