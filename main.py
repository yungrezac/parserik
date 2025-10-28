import re
from urllib.parse import urlparse
import requests
import time
import random
import datetime
import math
import os
from openpyxl import Workbook

# Полный набор заголовков для маскировки запросов
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

# Ввод и парсинг
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
        brandEndIndex = query.find('&', brandStartIndex)
        brandItems = query[brandStartIndex:] if brandEndIndex == -1 else query[brandStartIndex:brandEndIndex]
        brandId = brandItems.split('=')[1]
    return (sellerId, brandId)

# Задержки
def get_delay(): return random.uniform(2, 5)
def get_delay_safe(): return random.uniform(5, 10)
def get_delay_aggressive(): return random.uniform(1, 2)

# Вспомогательные
def safe_get_field(obj, field_name): return obj.get(field_name, '') if isinstance(obj, dict) else getattr(obj, field_name, '')
def find_options_by_group_name(grouped_options, group_name): 
    try: return next((g['options'] for g in grouped_options if g['group_name'] == group_name), [])
    except (TypeError, KeyError): return []
def get_host_by_range(range_value, route_map):
    if not isinstance(route_map, list): return ''
    for host_info in route_map: 
        if host_info['vol_range_from'] <= range_value <= host_info['vol_range_to']: return host_info['host']
    return ''

# Получение данных с WB
def get_mediabasket_route_map():
    try:
        response = requests.get('https://cdn.wbbasket.ru/api/v3/upstreams', headers=headers)
        response.raise_for_status()
        data = response.json()
        if 'recommend' in data and 'mediabasket_route_map' in data['recommend']:
            return data['recommend']['mediabasket_route_map'][0]['hosts']
    except requests.exceptions.RequestException as e: print(f"Ошибка при запросе карты маршрутов: {e}")
    return []

def fetch_data(sellerId, brandId, backets):
    allProducts = []
    productsPerPage = 100
    urlTotalList = f"https://catalog.wb.ru/sellers/v8/filters?ab_testing=false&appType=1&curr=rub&dest=12358357&fbrand={brandId}&lang=ru&spp=30&supplier={sellerId}&uclusters=0"
    
    responseTotal = None
    try:
        responseTotal = requests.get(urlTotalList, headers=headers, timeout=10)
        responseTotal.raise_for_status()
        resTotal = responseTotal.json()
    except requests.exceptions.JSONDecodeError:
        print("Критическая ошибка: WB не вернул JSON для общего числа товаров.")
        if responseTotal: print(f"Ответ сервера: {responseTotal.text[:500]}...")
        return []
    except requests.exceptions.RequestException as e:
        print(f"Критическая ошибка при запросе к WB (общее число товаров): {e}")
        return []

    productsTotal = resTotal.get('data', {}).get('total', 0)
    if not productsTotal: 
        print("Товары не найдены. Проверьте ID продавца и бренда.")
        return []
        
    pagesCount = math.ceil(productsTotal / productsPerPage)
    print(f"Всего найдено товаров: {productsTotal}. Страниц: {pagesCount}")
    
    currentPage, count = 1, 1
    while currentPage <= pagesCount:
        urlList = f"https://catalog.wb.ru/sellers/v4/catalog?ab_testing=false&appType=1&curr=rub&dest=12358357&fbrand={brandId}&hide_dtype=13&lang=ru&page={currentPage}&sort=popular&spp=30&supplier={sellerId}"
        try:
            response = requests.get(urlList, headers=headers, timeout=10)
            response.raise_for_status()
            products = response.json().get('products', [])
        except (requests.exceptions.JSONDecodeError, requests.exceptions.RequestException) as e:
            print(f"Ошибка получения списка товаров на странице {currentPage}. Пропускаем. {e}")
            currentPage += 1
            continue

        for item in products:
            print(f'Обработка товара {count}/{productsTotal}')
            productId = str(item['id'])
            backetName = get_host_by_range(int(productId[:-5]), backets)

            backetNumber, isAutoServer = 1, bool(backetName)
            while True:
                if not isAutoServer and backetNumber > 24: 
                    item['advanced'] = {}
                    break
                
                backetFormattedNumber = f"0{backetNumber}" if backetNumber < 10 else str(backetNumber)
                urlItem = f"https://{backetName if isAutoServer else f'basket-{backetFormattedNumber}.wbbasket.ru'}/vol{productId[:-5]}/part{productId[:-3]}/{productId}/info/ru/card.json"
                
                try:
                    productResponse = requests.get(urlItem, headers=headers, timeout=5)
                    if productResponse.status_code == 200:
                        item['advanced'] = productResponse.json()
                        time.sleep(get_delay_aggressive())
                        break
                    
                    if not isAutoServer and productResponse.status_code == 404:
                        backetNumber += 1
                        continue
                    
                    # Для всех других ошибок (включая 404 на авто-сервере) - выходим
                    print(f"Неожиданный статус {productResponse.status_code} для {productId}. Товар пропущен.")
                    item['advanced'] = {}
                    break

                except requests.exceptions.RequestException as e:
                    print(f"Ошибка получения карточки товара {productId}. Пропускаем. {e}")
                    item['advanced'] = {}
                    break
            count += 1
        allProducts.extend(products)
        currentPage += 1
    return allProducts

# (Остальной код без изменений)

def find_first_of_set(string, char_set, start=0):
    for i, char in enumerate(string, start):
        if char in char_set: return i
    return -1

def find_by_name(data, search_name):
    for item in data: 
        if item['name'] == search_name: return item['value']
    return '-'

def find_value_in_arrays(array1, array2, search_name):
    for arr in [array1, array2]:
        for item in arr: 
            if item['name'] == search_name: return item['value']
    return ''

def extract_number(value: str):
    if not isinstance(value, str): return ''
    match = re.search(r'\d+(?:[.,]\d+)?', value)
    if match:
        try: return float(match.group().replace(',', '.'))
        except ValueError: return ''
    return ''

def map_data(data):
    new_data = []
    for item in data:
        advanced = item.get('advanced')
        if not advanced: continue
        
        new_item = {
            'id': safe_get_field(item, 'id'), 'name': safe_get_field(item, 'name'),
            'category': safe_get_field(item, 'entity'), 'brand': safe_get_field(item, 'brand'),
            'description': safe_get_field(advanced, 'description')
        }

        options = safe_get_field(advanced, 'options')
        compound = find_by_name(options, 'Состав')
        if compound == '-':
            desc = new_item['description']
            start_idx = desc.find("Состав:")
            if start_idx != -1:
                end_idx = find_first_of_set(desc, '.;', start_idx)
                compound = desc[start_idx + 7: end_idx if end_idx != -1 else None].strip()
        new_item['compound'] = compound if compound != '-' else ''

        groupedOptions = safe_get_field(advanced, 'grouped_options')
        dims = find_options_by_group_name(groupedOptions, 'Габариты')
        adv_info = find_options_by_group_name(groupedOptions, 'Дополнительная информация')

        new_item.update({
            'gross': extract_number(find_value_in_arrays(options, dims, 'Вес с упаковкой (кг)')),
            'net': extract_number(find_value_in_arrays(options, dims, 'Вес товара без упаковки (г)')),
            'height': extract_number(find_value_in_arrays(options, dims, 'Длина упаковки')),
            'length': extract_number(find_value_in_arrays(options, dims, 'Высота упаковки')),
            'width': extract_number(find_value_in_arrays(options, dims, 'Ширина упаковки')),
            'equipment': find_value_in_arrays(options, adv_info, 'Комплектация'),
            'expiration_date': find_value_in_arrays(options, adv_info, 'Срок годности'),
            'country': find_value_in_arrays(options, adv_info, 'Страна производства'),
            'package': find_value_in_arrays(options, adv_info, 'Упаковка'),
            'package_items_count': extract_number(find_value_in_arrays(options, adv_info, 'Количество предметов в упаковке')),
            'tax': 20
        })
        new_data.append(new_item)
    return new_data

# Excel
def generate_filename(base_name="result"): return f"{base_name}_{datetime.datetime.now():%Y-%m-%d_%H-%M-%S}.xlsx"
def create_excel_file(data):
    if not data: return None
    if not os.path.exists("downloads"): os.makedirs("downloads")
    output_path = os.path.join("downloads", generate_filename())
    wb, ws = Workbook(), Workbook().active
    headers = list(data[0].keys())
    ws.append(headers)
    for row_data in data: ws.append([row_data.get(h, '') for h in headers])
    wb.save(output_path)
    print(f"Данные успешно записаны в файл {output_path}")
    return output_path

def run_parser(seller_id, brand_id):
    baskets = get_mediabasket_route_map()
    if not baskets: print("Не удалось получить карту маршрутов. Парсинг может быть неполным.")
    data = fetch_data(seller_id, brand_id, baskets)
    return map_data(data)

if __name__ == "__main__":
    parsedInput = parse_input(input("Введите ссылку WB или ID магазина и ID бренда(ов) через пробел:"))
    mapped_data = run_parser(parsedInput[0], parsedInput[1])
    if mapped_data: create_excel_file(mapped_data)
