import re
from urllib.parse import urlparse
import requests
import time
import random
import datetime
import math
import os
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side, NamedStyle
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

def make_request(url, headers, timeout=10, retries=5, backoff_factor=0.5):
    """Надежная функция для выполнения HTTP-запросов с повторными попытками."""
    for i in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status() # Вызовет исключение для кодов 4xx/5xx
            return response
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                sleep_time = backoff_factor * (2 ** i) + random.uniform(0, 1)
                time.sleep(sleep_time)
                continue
            else:
                # Для других ошибок HTTP, можно просто прекратить попытки
                raise e
        except requests.exceptions.RequestException as e:
            # Для сетевых ошибок (timeout, connection error) также попробуем еще раз
            sleep_time = backoff_factor * (2 ** i) + random.uniform(0, 1)
            time.sleep(sleep_time)
            continue
    # Если все попытки не увенчались успехом
    raise Exception(f"Не удалось получить данные после {retries} попыток. URL: {url}")


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
        response_total = make_request(url_total_list, headers=headers)
        res_total = response_total.json()
        products_total = res_total.get('data', {}).get('total', 0)
    except (requests.exceptions.RequestException, json.JSONDecodeError, Exception) as e:
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
            response = make_request(url_list, headers=headers)
            products_on_page = response.json().get('products', [])
        except (requests.exceptions.RequestException, json.JSONDecodeError):
            current_page += 1
            time.sleep(random.uniform(1, 3)) # Добавим задержку при ошибке на странице
            continue

        for item in products_on_page:
            count += 1
            yield json.dumps({'type': 'progress', 'current': count, 'total': products_total, 'message': item.get('name', '')})
            
            productId = str(item['id'])
            backetName = get_host_by_range(int(productId[:-5]), baskets)
            backetNumber, isAutoServer = 1, bool(backetName)
            while True:
                if not isAutoServer and backetNumber > 12:
                    item['advanced'] = {}
                    break
                
                backetFormattedNumber = f"0{backetNumber}" if backetNumber < 10 else str(backetNumber)
                urlItem = f"https://{backetName if isAutoServer else f'basket-{backetFormattedNumber}.wbbasket.ru'}/vol{productId[:-5]}/part{productId[:-3]}/{productId}/info/ru/card.json"
                
                try:
                    # Используем более короткий таймаут для карточек, но без повторных попыток, чтобы не замедлять
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
            time.sleep(random.uniform(0.1, 0.4)) # Небольшая задержка между товарами

        current_page += 1
        time.sleep(random.uniform(1, 2)) # Задержка между страницами

    # 4. Маппинг данных и создание файла
    yield json.dumps({'type': 'log', 'message': 'Формирование итоговой таблицы...'})
    mapped_data = map_data(all_products, baskets) # Передаем baskets в map_data

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
        response = make_request('https://cdn.wbbasket.ru/api/v3/upstreams', headers=headers, timeout=5)
        data = response.json()
        if 'recommend' in data and 'mediabasket_route_map' in data['recommend']:
            return data['recommend']['mediabasket_route_map'][0]['hosts']
    except (requests.exceptions.RequestException, json.JSONDecodeError, KeyError):
        return []
    return []

def get_host_by_range(range_value, route_map):
    if not isinstance(route_map, list): return ''
    for host_info in route_map: 
        if 'vol_range_from' in host_info and 'vol_range_to' in host_info and host_info['vol_range_from'] <= range_value <= host_info['vol_range_to']: 
            return host_info['host']
    return ''

def map_data(data, baskets):
    new_data = []
    for item in data:
        advanced = item.get('advanced')
        if not advanced: continue
        
        options = advanced.get('options', [])
        grouped_options = advanced.get('grouped_options', [])
        
        dimensions_group = find_options_by_group_name(grouped_options, 'Габариты')
        advanced_info_group = find_options_by_group_name(grouped_options, 'Дополнительная информация')
        cosmetics_group = find_options_by_group_name(grouped_options, 'Косметическое средство')

        photo_urls = []
        video_url = ''
        if item.get('id'):
            product_id_str = str(item['id'])
            host = get_host_by_range(int(product_id_str[:-5]), baskets)
            if host:
                for i in range(1, 11):
                    photo_urls.append(f"https://{host}/vol{product_id_str[:-5]}/part{product_id_str[:-3]}/{product_id_str}/images/c516x688/{i}.jpg")

        certificates = advanced.get('certificates', [])
        cert_end_date, cert_reg_date, declaration_num, certificate_num, sgr_num = '', '', '', '', ''
        if certificates:
            cert = certificates[0]
            cert_end_date = cert.get('end_date','')
            cert_reg_date = cert.get('start_date','')
            if 'ЕАЭС' in cert.get('__name', ''):
                declaration_num = cert.get('number', '')
            else:
                certificate_num = cert.get('number', '')

        new_item = {
            'Группа': '', 
            'Артикул продавца': item.get('vendorCode', ''),
            'Артикул WB': item.get('id', ''),
            'Наименование': item.get('name', ''),
            'Категория продавца': '', 
            'Бренд': item.get('brand', ''),
            'Описание': advanced.get('description', ''),
            'Фото': ', '.join(photo_urls),
            'Видео': video_url,
            'Полное наименование товара': advanced.get('name', ''),
            'Состав': find_value_in_arrays(options, advanced_info_group, search_name='Состав'),
            'Баркод': '', 
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
            'Артикул OZON': '', 
            'Возрастные ограничения': find_value_in_arrays(options, advanced_info_group, search_name='Возрастные ограничения'),
            'Время нанесения': find_value_in_arrays(options, cosmetics_group, search_name='Время нанесения'),
            'Действие': find_value_in_arrays(options, cosmetics_group, search_name='Действие'),
            'ИКПУ': '', 
            'Код упаковки': '', 
            'Комплектация': find_value_in_arrays(options, advanced_info_group, search_name='Комплектация'),
            'Назначение косметического средства': find_value_in_arrays(options, advanced_info_group, search_name='Назначение косметического средства'),
            'Назначение подарка': '', 
            'Объем товара': extract_number(find_value_in_arrays(options, cosmetics_group, search_name='Объем товара')),
            'Повод': '', 
            'Раздел меню': '', 
            'Срок годности': find_value_in_arrays(options, advanced_info_group, search_name='Срок годности'),
            'Страна производства': find_value_in_arrays(options, advanced_info_group, search_name='Страна производства'),
            'ТНВЭД': find_value_in_arrays(options, advanced_info_group, search_name='ТН ВЭД'),
            'Тип доставки': '', 
            'Тип кожи': find_value_in_arrays(options, cosmetics_group, search_name='Тип кожи'),
            'Упаковка': find_value_in_arrays(options, advanced_info_group, search_name='Упаковка'),
            'Форма упаковки': '', 
            'Ставка НДС': '20'
        }
        new_data.append(new_item)
    return new_data

def create_excel_file(data):
    if not data:
        return None

    if not os.path.exists("downloads"):
        os.makedirs("downloads")
    
    filename = f"result_{datetime.datetime.now():%Y-%m-%d_%H-%M-%S}.xlsx"
    output_path = os.path.join("downloads", filename)
    
    wb = Workbook()
    ws = wb.active

    # Стили
    header_style_s0 = NamedStyle(name="header_style_s0")
    header_style_s0.fill = PatternFill(start_color="ECDAFF", end_color="ECDAFF", fill_type="solid")
    header_style_s0.font = Font(name='Calibri', size=16)
    header_style_s0.alignment = Alignment(vertical='bottom')

    header_style_s1 = NamedStyle(name="header_style_s1")
    header_style_s1.fill = PatternFill(start_color="ECDAFF", end_color="ECDAFF", fill_type="solid")
    header_style_s1.font = Font(name='Calibri', size=12)
    header_style_s1.alignment = Alignment(vertical='bottom')

    header_style_s2 = NamedStyle(name="header_style_s2")
    header_style_s2.fill = PatternFill(start_color="9A41FE", end_color="9A41FE", fill_type="solid")
    header_style_s2.font = Font(name='Calibri', size=12, bold=True, color="FFFFFF")
    header_style_s2.alignment = Alignment(vertical='center')

    description_style_s3 = NamedStyle(name="description_style_s3")
    description_style_s3.fill = PatternFill(start_color="F0F0F3", end_color="F0F0F3", fill_type="solid")
    description_style_s3.font = Font(name='Calibri', size=10)
    description_style_s3.alignment = Alignment(vertical='top', wrap_text=True)

    wb.add_named_style(header_style_s0)
    wb.add_named_style(header_style_s1)
    wb.add_named_style(header_style_s2)
    wb.add_named_style(description_style_s3)

    # --- Заголовки ---
    # Строка 1
    ws.append(['Основная информация','','','','','','','','','Размеры и Баркоды','Габариты','','','','','Документы','','','','','Дополнительная информация','','','','','','','','','','','','','','','','','','','','','Цены',''])
    ws.merge_cells('A1:I1')
    ws.merge_cells('J1:N1')
    ws.merge_cells('O1:S1')
    ws.merge_cells('T1:AO1')
    for cell in ws[1]:
        cell.style = header_style_s0
    ws.row_dimensions[1].height = 41

    # Строка 2
    ws.append([''] * 42)
    for cell in ws[2]:
        cell.style = header_style_s1
    ws.row_dimensions[2].height = 63

    # Строка 3
    headers_row3 = ['Группа', 'Артикул продавца', 'Артикул WB', 'Наименование', 'Категория продавца', 'Бренд', 'Описание', 'Фото', 'Видео', 'Полное наименование товара', 'Состав', 'Баркод', 'Вес с упаковкой (кг)', 'Вес товара без упаковки (г)', 'Высота упаковки', 'Длина упаковки', 'Ширина упаковки', 'Дата окончания действия сертификата/декларации', 'Дата регистрации сертификата/декларации', 'Номер декларации соответствия', 'Номер сертификата соответствия', 'Свидетельство о регистрации СГР', 'SPF', 'Артикул OZON', 'Возрастные ограничения', 'Время нанесения', 'Действие', 'ИКПУ', 'Код упаковки', 'Комплектация', 'Назначение косметического средства', 'Назначение подарка', 'Объем товара', 'Повод', 'Раздел меню', 'Срок годности', 'Страна производства', 'ТНВЭД', 'Тип доставки', 'Тип кожи', 'Упаковка', 'Форма упаковки', 'Ставка НДС', '']
    ws.append(headers_row3)
    for cell in ws[3]:
        cell.style = header_style_s2

    ws.row_dimensions[3].height = 41
    
    # Строка 4
    descriptions_row4 = [
        '',
        'Это номер или название, по которому вы сможете идентифицировать свой товар.',
        'Уникальный идентификатор карточки, который присваивается после успешного создания товара.',
        '',
        'Категория выбирается строго из справочника, справочник можно посмотреть через единичное создание карточек.',
        '',
        'Если вы не заполните характеристики, то мы постараемся заполнить их сами из вашего описания или по фото товара.',
        "Список ссылок на фотографии разделённый ';' (Количество - до 30 шт.)",
        'Ссылка на видео (Количество - 1 шт.)',
        'Максимальное количество значений: 1',
        'Максимальное количество значений: 20',
        '',
        'Единица измерения: кг',
        'Единица измерения: г',
        'Единица измерения: см',
        'Единица измерения: см',
        'Единица измерения: см',
        'Максимальное количество значений: 1',
        'Максимальное количество значений: 1',
        'Максимальное количество значений: 1',
        'Максимальное количество значений: 1',
        'Максимальное количество значений: 1',
        'Максимальное количество значений: 3',
        'Максимальное количество значений: 1',
        'Максимальное количество значений: 3',
        'Максимальное количество значений: 3',
        'Максимальное количество значений: 3',
        'Максимальное количество значений: 1',
        'Максимальное количество значений: 1',
        'Максимальное количество значений: 12',
        'Максимальное количество значений: 3',
        'Максимальное количество значений: 3',
        'Единица измерения: мл',
        'Максимальное количество значений: 3',
        'Максимальное количество значений: 1',
        'Максимальное количество значений: 3',
        'Максимальное количество значений: 1',
        'Максимальное количество значений: 1',
        'Максимальное количество значений: 1',
        'Максимальное количество значений: 1',
        'Максимальное количество значений: 3',
        'Максимальное количество значений: 1',
        'Максимальное количество значений: 1',
        ''
    ]
    ws.append(descriptions_row4)
    for cell in ws[4]:
        cell.style = description_style_s3
    ws.row_dimensions[4].height = 56


    # --- Данные ---
    if data:
        # Используем headers_row3 для обеспечения правильного порядка
        for row_data in data:
            row_to_append = []
            for header in headers_row3:
                 row_to_append.append(row_data.get(header, ''))
            ws.append(row_to_append)
            
    # Устанавливаем ширину столбцов
    for col in range(ord('A'), ord('Q') + 1):
        ws.column_dimensions[chr(col)].width = 30

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
