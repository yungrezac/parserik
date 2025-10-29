import re
from urllib.parse import urlparse
import requests
import time
import random
import datetime
import math
import os
from openpyxl import Workbook
import json

# Заголовки, маскирующиеся под реальный браузер
headers = {
    'Accept': '*/*',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'Connection': 'keep-alive',
    'Origin': 'https://www.wildberries.ru',
    'Referer': 'https://www.wildberries.ru/',
    'Sec-Ch-Ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"Windows"',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'cross-site',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
}

# --- НОВАЯ ФУНКЦИЯ-ГЕНЕРАТОР ДЛЯ СТРИМИНГА ПРОГРЕССА ---
def stream_parser(seller_id, brand_id):
    """
    Основная логика парсинга, перестроенная в генератор, который yield'ит обновления прогресса.
    """
    all_products = []
    # 1. Получение карты маршрутов для корзин
    yield json.dumps({'type': 'log', 'message': 'Получение карты маршрутов WB...'})
    baskets = get_mediabasket_route_map()
    if not baskets:
        yield json.dumps({'type': 'log', 'message': 'Не удалось получить карту маршрутов. Парсинг может быть неполным.'})

    # 2. Определение общего количества товаров
    url_total_list = f"https://catalog.wb.ru/sellers/v8/filters?ab_testing=false&appType=1&curr=rub&dest=12358357&fbrand={brand_id}&lang=ru&spp=30&supplier={seller_id}&uclusters=0"
    try:
        response_total = requests.get(url_total_list, headers=headers, timeout=10)
        response_total.raise_for_status()
        res_total = response_total.json()
        products_total = res_total.get('data', {}).get('total', 0)
    except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
        raise Exception(f"Критическая ошибка при получении общего числа товаров: {e}")

    if not products_total:
        raise Exception("Товары не найдены. Проверьте правильность ID продавца и бренда.")

    pages_count = math.ceil(products_total / 100)
    yield json.dumps({'type': 'start', 'total': products_total, 'message': f'Найдено товаров: {products_total}. Начинаем обработку...'})

    # 3. Постраничный обход и сбор данных
    current_page, count = 1, 0
    while current_page <= pages_count:
        url_list = f"https://catalog.wb.ru/sellers/v4/catalog?ab_testing=false&appType=1&curr=rub&dest=12358357&fbrand={brand_id}&hide_dtype=13&lang=ru&page={current_page}&sort=popular&spp=30&supplier={seller_id}"
        try:
            response = requests.get(url_list, headers=headers, timeout=10)
            response.raise_for_status()
            products_on_page = response.json().get('products', [])
        except (requests.exceptions.RequestException, json.JSONDecodeError):
            current_page += 1
            continue # Пропускаем страницу в случае ошибки

        for item in products_on_page:
            count += 1
            yield json.dumps({'type': 'progress', 'current': count, 'total': products_total, 'message': item.get('name', '')})
            
            # ... (логика получения доп. информации по каждому товару) ...
            # Эта часть остается такой же, как в fetch_data
            productId = str(item['id'])
            backetName = get_host_by_range(int(productId[:-5]), baskets)
            backetNumber, isAutoServer = 1, bool(backetName)
            while True:
                if not isAutoServer and backetNumber > 12: # Уменьшаем количество попыток для ускорения
                    item['advanced'] = {}
                    break
                
                backetFormattedNumber = f"0{backetNumber}" if backetNumber < 10 else str(backetNumber)
                urlItem = f"https://{backetName if isAutoServer else f'basket-{backetFormattedNumber}.wbbasket.ru'}/vol{productId[:-5]}/part{productId[:-3]}/{productId}/info/ru/card.json"
                
                try:
                    productResponse = requests.get(urlItem, headers=headers, timeout=3)
                    if productResponse.status_code == 200:
                        item['advanced'] = productResponse.json()
                        break
                    if not isAutoServer and productResponse.status_code == 404:
                        backetNumber += 1
                        continue
                    item['advanced'] = {}
                    break
                except requests.exceptions.RequestException:
                    item['advanced'] = {}
                    break
            all_products.append(item)
        current_page += 1

    # 4. Маппинг данных и создание файла
    yield json.dumps({'type': 'log', 'message': 'Формирование итоговой таблицы...'})
    mapped_data = map_data(all_products)

    yield json.dumps({'type': 'log', 'message': 'Создание Excel-файла...'})
    output_path = create_excel_file(mapped_data)
    if not output_path:
        raise Exception("Не удалось создать Excel-файл.")

    download_filename = os.path.basename(output_path)

    # 5. Отправка финального результата
    yield json.dumps({
        'type': 'result',
        'data': {
            'table_data': mapped_data,
            'download_filename': download_filename
        }
    })


# --- Вспомогательные функции (без критических изменений) ---

def check_string(s): return bool(re.fullmatch(r'(\d+%3B)*\d+', s))
def parse_input(input_str):
    parts = input_str.split()
    if len(parts) > 2: raise ValueError("Необходимо указать два параметра через пробел")
    sellerId, brandId = ('', '')
    if len(parts) == 2:
        sellerId, brandId = parts[0], parts[1]
        if not sellerId.isdigit() or not check_string(brandId): raise ValueError("Необходимо указать число и ID бренда(ов)")
    else:
        parseResult = urlparse(input_str)
        sellerId = str(parseResult.path).split('/')[2]
        query = str(parseResult.query)
        brandStartIndex = query.find('fbrand')
        if brandStartIndex == -1: raise ValueError("Параметр fbrand не найден в ссылке.")
        brandEndIndex = query.find('&', brandStartIndex)
        brandItems = query[brandStartIndex:] if brandEndIndex == -1 else query[brandStartIndex:brandEndIndex]
        brandId = brandItems.split('=')[1]
    return (sellerId, brandId)

def get_mediabasket_route_map():
    try:
        response = requests.get('https://cdn.wbbasket.ru/api/v3/upstreams', headers=headers, timeout=5)
        response.raise_for_status()
        data = response.json()
        if 'recommend' in data and 'mediabasket_route_map' in data['recommend']:
            return data['recommend']['mediabasket_route_map'][0]['hosts']
    except requests.exceptions.RequestException: return []
    return []

def get_host_by_range(range_value, route_map):
    if not isinstance(route_map, list): return ''
    for host_info in route_map: 
        if host_info['vol_range_from'] <= range_value <= host_info['vol_range_to']: return host_info['host']
    return ''

def map_data(data):
    new_data = []
    for item in data:
        advanced = item.get('advanced')
        if not advanced: continue
        
        # Получаем опции и группы опций
        options = advanced.get('options', [])
        grouped_options = advanced.get('grouped_options', [])
        
        # Ищем нужные группы
        dimensions_group = find_options_by_group_name(grouped_options, 'Габариты')
        advanced_info_group = find_options_by_group_name(grouped_options, 'Дополнительная информация')
        cosmetics_group = find_options_by_group_name(grouped_options, 'Косметическое средство')

        # Формируем фото и видео
        photo_urls = []
        video_url = ''
        if item.get('id'):
            product_id_str = str(item['id'])
            host = get_host_by_range(int(product_id_str[:-5]), get_mediabasket_route_map())
            if host:
                # Фото
                for i in range(1, 11): # Предположим, что у товара до 10 фото
                    photo_urls.append(f"https://{host}/vol{product_id_str[:-5]}/part{product_id_str[:-3]}/{product_id_str}/images/c516x688/{i}.jpg")
                
                # Видео (если есть)
                # Логика для видео может быть сложнее, здесь упрощенный вариант
                # video_url = f"https://{host}/vol{product_id_str[:-5]}/part{product_id_str[:-3]}/{product_id_str}/video/1.mp4"
        
        # Сертификаты
        certificates = advanced.get('certificates', [])
        cert_end_date = ''
        cert_reg_date = ''
        declaration_num = ''
        certificate_num = ''
        sgr_num = ''
        if certificates:
            cert = certificates[0]
            cert_end_date = cert.get('end_date','')
            cert_reg_date = cert.get('start_date','')
            # Логика для определения типа сертификата и номера
            # Это упрощение, т.к. в API может быть несколько сертификатов разных типов
            if 'ЕАЭС' in cert.get('__name', ''):
                declaration_num = cert.get('number', '')
            else:
                certificate_num = cert.get('number', '')

        new_item = {
            'Группа': '',  # Нет данных
            'Артикул продавца': item.get('vendorCode', ''),
            'Артикул WB': item.get('id', ''),
            'Наименование': item.get('name', ''),
            'Категория продавца': '', # Нет прямого аналога
            'Бренд': item.get('brand', ''),
            'Описание': advanced.get('description', ''),
            'Фото': ', '.join(photo_urls),
            'Видео': video_url,
            'Полное наименование товара': advanced.get('name', ''),
            'Состав': find_value_in_arrays(options, advanced_info_group, search_name='Состав'),
            'Баркод': '', # Нет данных
            'Вес с упаковкой (кг)': extract_number(find_value_in_arrays(options, dimensions_group, search_name='Вес с упаковкой (кг)')),
            'Вес товара без упаковки (г)': extract_number(find_value_in_arrays(options, dimensions_group, search_name='Вес товара без упаковки (г)')),
            'Высота упаковки': extract_number(find_value_in_arrays(options, dimensions_group, search_name='Высота упаковки')),
            'Длина упаковки': extract_number(find_value_in_arrays(options, dimensions_group, search_name='Длина упаковки')),
            'Ширина упаковки': extract_number(find_value_in_arrays(options, dimensions_group, search_name='Ширина упаковки')),
            'Дата окончания действия сертификата/декларации': cert_end_date,
            'Дата регистрации сертификата/декларации': cert_reg_date,
            'Номер декларации соответствия': declaration_num,
            'Номер сертификата соответствия': certificate_num,
            'Свидетельство о регистрации СГР': sgr_num,
            'SPF': find_value_in_arrays(options, cosmetics_group, search_name='SPF'),
            'Артикул OZON': '', # Нет данных
            'Возрастные ограничения': find_value_in_arrays(options, advanced_info_group, search_name='Возрастные ограничения'),
            'Время нанесения': find_value_in_arrays(options, cosmetics_group, search_name='Время нанесения'),
            'Действие': find_value_in_arrays(options, cosmetics_group, search_name='Действие'),
            'ИКПУ': '', # Нет данных
            'Код упаковки': '', # Нет данных
            'Комплектация': find_value_in_arrays(options, advanced_info_group, search_name='Комплектация'),
            'Назначение косметического средства': find_value_in_arrays(options, advanced_info_group, search_name='Назначение косметического средства'),
            'Назначение подарка': '', # Нет данных
            'Объем товара': find_value_in_arrays(options, cosmetics_group, search_name='Объем товара'),
            'Повод': '', # Нет данных
            'Раздел меню': '', # Нет данных
            'Срок годности': find_value_in_arrays(options, advanced_info_group, search_name='Срок годности'),
            'Страна производства': find_value_in_arrays(options, advanced_info_group, search_name='Страна производства'),
            'ТНВЭД': find_value_in_arrays(options, advanced_info_group, search_name='ТН ВЭД'),
            'Тип доставки': '', # Нет данных
            'Тип кожи': find_value_in_arrays(options, cosmetics_group, search_name='Тип кожи'),
            'Упаковка': find_value_in_arrays(options, advanced_info_group, search_name='Упаковка'),
            'Форма упаковки': '', # Нет данных
            'Ставка НДС': '20' # или другое значение, если оно есть
        }
        new_data.append(new_item)
    return new_data

def create_excel_file(data):
    if not data: return None
    if not os.path.exists("downloads"): os.makedirs("downloads")
    
    filename = f"result_{datetime.datetime.now():%Y-%m-%d_%H-%M-%S}.xlsx"
    output_path = os.path.join("downloads", filename)
    
    wb = Workbook()
    ws = wb.active
    
    # Заголовки из первого элемента данных, если они есть
    if data:
        headers = list(data[0].keys())
        ws.append(headers)
        for row_data in data:
            row = [row_data.get(h, '') for h in headers]
            ws.append(row)
            
    wb.save(output_path)
    return output_path

# --- Прочие вспомогательные функции (без изменений) ---
def find_options_by_group_name(grouped_options, group_name): 
    try: return next((g['options'] for g in grouped_options if g['group_name'] == group_name), [])
    except (TypeError, KeyError): return []

def find_value_in_arrays(*arrays, search_name):
    for arr in arrays:
        if not isinstance(arr, list): continue
        for item in arr: 
            if isinstance(item, dict) and item.get('name') == search_name: return item.get('value')
    return ''

def extract_number(value):
    if not isinstance(value, str): return ''
    match = re.search(r'\d+(?:[.,]\d+)?', value)
    if match:
        try: return float(match.group().replace(',', '.'))
        except ValueError: return ''
    return ''
