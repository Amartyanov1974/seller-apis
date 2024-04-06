import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получение списка товаров

    Используя методы Seller API магазина Озон получаем список товаров
    в формате словаря по 1000 позиций максимум.

    Args:
        last_id (str): идентификатор последнего значения на странице,
                       будет пустым, если запрос был первым.
        client_id (str): идентификатор клиента.
        seller_token (str): API-ключ.

    Return:
        dict: Список товаров.

    Пример:
        >>> get_product_list(last_id, client_id, seller_token)
        >>> {
        >>>     "items":
        >>>         [
        >>>             {
        >>>                 "product_id": 223681945,
        >>>                 "offer_id": "136748"
        >>>             }
        >>>         ],
        >>>     "total": 1,
        >>>     "last_id": "bnVсbA=="
        >>> }

    Raises:
        HTTPError: 404 (если сервер будет недоступен).
    Пример:
        >>> get_product_list(last_id, client_id, seller_token)
        >>> None
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получить артикулы товаров магазина озон

    Из списка товаров извлечь артикулы товара - идентификаторы товара
    в системе продавца.

    Args:
        client_id (str): идентификатор клиента.
        seller_token (str): API-ключ.

    Return:
        list: Список товаров.

    Пример:
        >>> get_offer_ids(client_id, seller_token)
        >>> [{"product_id": "223681945", "offer_id": "136748"}]

    Raises:
        AttributeError: 'NoneType' object has no attribute 'get'
        если функция get_product_list вернет None, из-за отсутствия
        ответа сервера по какой либо причине

    Пример:
        >>> get_offer_ids(client_id, seller_token)
        >>> AttributeError: 'NoneType' object has no attribute 'get'
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновить цены товаров"""
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновить остатки"""
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Получаем остатки товара

    Скачиваем архив с файлом excel из которого извлекаем с помощью
    библиотеки pandas остатки товара.

    Args:
        Нет

    Return:
        list: Остатки товара.

    Пример:
        >>> download_stock()
        >>> [{'Код': 73309, 'Наименование товара': 'BM7334-66L', 'Изображение': 'Показать', 'Цена': "38'440.00 руб.", 'Количество': 1, 'Заказ': ''}]

    Raises:
        XLRDError('Unsupported format, or corrupt file')
        если целостность файла excel в архиве будет повреждена

    Пример:
        >>> download_stock()
        >>> xlrd.biffh.XLRDError: Unsupported format, or corrupt file: Expected BOF record; found b'deb http'
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(io=excel_file,na_values=None,keep_default_na=False,header=17,).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Актуализируем остатки товара

    Оставляем в списке товара только те позиции, которые присутствуют
    в магазине Озон и добавляем отсутствующие позиции в остатки из
    имеющихся в магазине Озон.

    Args:
        watch_remnants (list): остатки товара в магазине часов
        offer_ids (list): список товара в магазине Озон

    Return:
        list: Актуализированные остатки товара.

    Пример:
        >>> watch_remnants = [{'Код': 73309, 'Наименование товара': 'BM7334-66L', 'Изображение': 'Показать', 'Цена': "38'440.00 руб.", 'Количество': 2, 'Заказ': ''}]
        >>> offer_ids = [{"product_id": "223681945", "offer_id": "136748"}, {"product_id": "223681946", "offer_id": "73309"}]
        >>> create_stocks(watch_remnants, offer_ids)
        >>> [{"offer_id": "136748", "stock": 0}, {"offer_id": "73309", "stock": 2}]

    Raises:
        ValueError: Если в "Количестве" будут не только цифры

    Пример:
        >>> watch_remnants = [{'Код': 73309, 'Наименование товара': 'BM7334-66L', 'Изображение': 'Показать', 'Цена': "38'440.00 руб.", 'Количество': '2O', 'Заказ': ''}]
        >>> offer_ids = [{"product_id": "223681945", "offer_id": "136748"}, {"product_id": "223681946", "offer_id": "73309"}]
        >>> create_stocks(watch_remnants, offer_ids)
        >>> ValueError: invalid literal for int() with base 10: '2O'

    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразовываем строку с ценой, убирая лишние символы.

    С помощью регулярных выражений выбраем из строки цифры,
    для получения цены в виде комбинации цифр, без прочих символов.

    Args:
        price (str): цена товара.

    Return:
        str: Преобразованная цена.

    Пример:
        >>> price = "5'990.00 руб."
        >>> price_conversion(price)
        >>> '5990'

    Raises:
        AttributeError: object has no attribute 'split' (если на входе
            будут объекты, не являющиеся строкой).

    Пример:
        >>> price = 5990
        >>> price_conversion(price)
        >>> AttributeError: 'int' object has no attribute 'split'

    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Создаем генератор.

    Выдаем по n элементов из списока lst.

    Args:
        lst (list): список товаров.
        n (int):    количество элементов в группе

    Return:
        list: Группа товаров.

    Пример:
        >>> lst = [1, 2, 3, 4, 5]
        >>> n = 2
        >>> list(divide(lst, n))
        >>> [[1, 2], [3, 4], [5]]
    """

    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):

    """ Эта функция в работе этой программы не используется"""

    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):

    """ Эта функция в работе этой программы не используется"""

    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
