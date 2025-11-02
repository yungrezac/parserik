import re
import os
import json
import time
import datetime
import math
import random
from urllib.parse import unquote, urlparse
import requests
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side, NamedStyle
from flask import Flask, render_template, request, send_from_directory, Response, stream_with_context, jsonify

# --- Инициализация Flask ---
app = Flask(__name__, static_folder='public', template_folder='public')
port = int(os.environ.get('PORT', 5000))

# --- Управление пользователями и API (без изменений) ---
def get_user_profile(user_data):
    return {
        'id': user_data.get('id', 'anonymous'), 'first_name': user_data.get('first_name', 'Anonymous'),
        'last_name': user_data.get('last_name', ''), 'username': user_data.get('username', 'anonymous'),
        'tariff': 'free', 'created_at': datetime.datetime.utcnow().isoformat()
    }

@app.route('/api/me', methods=['POST'])
def get_me():
    try:
        params = dict(x.split('=', 1) for x in unquote(request.json.get('initData', '')).split('&'))
        user_data = json.loads(params.get('user', '{}'))
    except (AttributeError, ValueError, json.JSONDecodeError):
        user_data = {}
    return jsonify(get_user_profile(user_data))

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/categories')
def get_categories():
    try:
        with open('subcategories.json', 'r', encoding='utf-8') as f:
            return jsonify(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError):
        return jsonify({"error": "Файл subcategories.json не найден."}), 500

@app.route('/download/<path:filename>')
def download_file(filename):
    directory = os.path.join(os.getcwd(), 'downloads')
    return send_from_directory(directory, filename, as_attachment=True)

# --- Маршрут для стриминга парсинга (адаптированный) ---
@app.route('/stream')
def stream_run():
    seller_id = request.args.get('seller_id')
    brand_id = request.args.get('brand_id')
    if not seller_id:
        return Response("Ошибка: не указан ID продавца.", status=400)

    def generate():
        try:
            for update in stream_parser(seller_id, brand_id):
                yield f"data: {json.dumps(update, ensure_ascii=False)}\n\n"
        except Exception as e:
            import traceback
            traceback.print_exc()
            error_message = {'type': 'error', 'message': f'Критическая ошибка сервера: {e}'}
            yield f"data: {json.dumps(error_message, ensure_ascii=False)}\n\n"

    return Response(stream_with_context(generate()), mimetype='text/event-stream')


# --- ЯДРО ПАРСИНГА (из вашего main.py) ---

headers = {
    'Accept': '*/*', 'Accept-Language': 'ru-RU,ru;q=0.9', 'Connection': 'keep-alive',
    'Origin': 'https://www.wildberries.ru', 'Referer': 'https://www.wildberries.ru/',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
}

def make_request(url, headers, timeout=10, retries=5, backoff_factor=0.5):
    for i in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                time.sleep(backoff_factor * (2 ** i) + random.uniform(0, 1))
                continue
            raise e
        except requests.exceptions.RequestException:
            time.sleep(backoff_factor * (2 ** i) + random.uniform(0, 1))
            continue
    raise Exception(f"Не удалось получить данные после {retries} попыток. URL: {url}")

def get_mediabasket_route_map():
    try:
        data = make_request('https://cdn.wbbasket.ru/api/v3/upstreams', headers=headers, timeout=5).json()
        return data['recommend']['mediabasket_route_map'][0]['hosts']
    except (Exception):
        return []

def get_host_by_range(range_value, route_map):
    if not isinstance(route_map, list): return ''
    for host_info in route_map:
        if host_info.get('vol_range_from') <= range_value <= host_info.get('vol_range_to'):
            return host_info['host']
    return ''

def stream_parser(seller_id, brand_id):
    all_products = []
    yield {'type': 'log', 'message': 'Получение карты маршрутов WB...'}
    baskets = get_mediabasket_route_map()

    brand_query = f"&fbrand={brand_id}" if brand_id and brand_id.strip() else ""
    url_total = f"https://catalog.wb.ru/sellers/v8/filters?ab_testing=false&appType=1&curr=rub&dest=12358357{brand_query}&lang=ru&spp=30&supplier={seller_id}&uclusters=0"
    try:
        products_total = make_request(url_total, headers).json().get('data', {}).get('total', 0)
    except Exception as e:
        raise Exception(f"Ошибка при получении числа товаров: {e}")

    if not products_total:
        raise Exception("Товары не найдены. Проверьте ID.")

    pages_count = math.ceil(products_total / 100)
    yield {'type': 'start', 'total': products_total, 'message': f'Найдено товаров: {products_total}'}

    count = 0
    for page_num in range(1, pages_count + 1):
        url_list = f"https://catalog.wb.ru/sellers/v4/catalog?ab_testing=false&appType=1&curr=rub&dest=12358357&page={page_num}&sort=popular&spp=30&supplier={seller_id}{brand_query}"
        try:
            products_on_page = make_request(url_list, headers).json().get('data', {}).get('products', [])
            if not products_on_page: continue
        except Exception: continue

        for item in products_on_page:
            count += 1
            yield {'type': 'progress', 'current': count, 'total': products_total, 'message': item.get('name', '')}
            
            productId = str(item['id'])
            backetName = get_host_by_range(int(productId[:-5]), baskets)
            item['advanced'] = {}
            if backetName:
                urlItem = f"https://{backetName}/vol{productId[:-5]}/part{productId[:-3]}/{productId}/info/ru/card.json"
                try:
                    res = requests.get(urlItem, headers=headers, timeout=3)
                    if res.status_code == 200: item['advanced'] = res.json()
                except Exception: pass
            all_products.append(item)
            time.sleep(random.uniform(0.05, 0.15))
        time.sleep(random.uniform(0.5, 1.0))

    yield {'type': 'log', 'message': 'Формирование таблицы...'}
    mapped_data = map_data(all_products)
    yield {'type': 'log', 'message': 'Создание Excel-файла...'}
    output_path = create_excel_file(mapped_data)
    if not output_path: raise Exception("Не удалось создать Excel-файл.")
    
    yield {'type': 'result', 'download_filename': os.path.basename(output_path)}

# --- Функции маппинга и создания Excel (из вашего main.py) ---

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
    return float(match.group().replace(',', '.')) if match else ''

def map_data(data):
    new_data = []
    for item in data:
        advanced = item.get('advanced')
        if not advanced: continue

        options = advanced.get('options', [])
        grouped_options = advanced.get('grouped_options', [])
        dimensions_group = find_options_by_group_name(grouped_options, 'Габариты')
        info_group = find_options_by_group_name(grouped_options, 'Дополнительная информация')
        cosmetics_group = find_options_by_group_name(grouped_options, 'Косметическое средство')

        cert = advanced.get('certificates', [{}])[0]
        new_item = {
            'Артикул продавца': item.get('vendorCode', ''), 'Бренд': item.get('brand', ''),
            'Наименование': item.get('name', ''), 'Описание': advanced.get('description', ''),
            'Полное наименование товара': advanced.get('name', ''),
            'Состав': find_value_in_arrays(options, info_group, search_name='Состав'),
            'Вес с упаковкой (кг)': extract_number(find_value_in_arrays(options, dimensions_group, search_name='Вес с упаковкой (кг)')),
            'Вес товара без упаковки (г)': extract_number(find_value_in_arrays(options, dimensions_group, search_name='Вес товара без упаковки (г)')),
            'Высота упаковки': extract_number(find_value_in_arrays(options, dimensions_group, search_name='Высота упаковки')),
            'Длина упаковки': extract_number(find_value_in_arrays(options, dimensions_group, search_name='Длина упаковки')),
            'Ширина упаковки': extract_number(find_value_in_arrays(options, dimensions_group, search_name='Ширина упаковки')),
            'Дата окончания действия сертификата/декларации': cert.get('end_date', ''),
            'Дата регистрации сертификата/декларации': cert.get('start_date', ''),
            'Номер декларации соответствия': cert.get('number', '') if 'ЕАЭС' in cert.get('__name', '') else '',
            'Номер сертификата соответствия': cert.get('number', '') if 'ЕАЭС' not in cert.get('__name', '') else '',
            'Свидетельство о регистрации СГР': '', 'SPF': find_value_in_arrays(options, cosmetics_group, search_name='SPF'),
            'Возрастные ограничения': find_value_in_arrays(options, info_group, search_name='Возрастные ограничения'),
            'Время нанесения': find_value_in_arrays(options, cosmetics_group, search_name='Время нанесения'),
            'Действие': find_value_in_arrays(options, cosmetics_group, search_name='Действие'),
            'Комплектация': find_value_in_arrays(options, info_group, search_name='Комплектация'),
            'Назначение косметического средства': find_value_in_arrays(options, info_group, search_name='Назначение косметического средства'),
            'Объем товара': extract_number(find_value_in_arrays(options, cosmetics_group, search_name='Объем товара')),
            'Срок годности': find_value_in_arrays(options, info_group, search_name='Срок годности'),
            'Страна производства': find_value_in_arrays(options, info_group, search_name='Страна производства'),
            'ТНВЭД': find_value_in_arrays(options, info_group, search_name='ТН ВЭД'),
            'Тип кожи': find_value_in_arrays(options, cosmetics_group, search_name='Тип кожи'),
            'Упаковка': find_value_in_arrays(options, info_group, search_name='Упаковка'),
            'Ставка НДС': '20'
        }
        new_data.append(new_item)
    return new_data

def create_excel_file(data):
    if not data: return None
    os.makedirs("downloads", exist_ok=True)
    filename = f"result_{datetime.datetime.now():%Y-%m-%d_%H-%M-%S}.xlsx"
    output_path = os.path.join("downloads", filename)
    
    wb = Workbook()
    ws = wb.active

    s0=NamedStyle(name="s0",fill=PatternFill("solid",fgColor="ECDAFF"),font=Font(name='Calibri',size=16),alignment=Alignment(vertical='bottom'))
    s1=NamedStyle(name="s1",fill=PatternFill("solid",fgColor="ECDAFF"),font=Font(name='Calibri',size=12),alignment=Alignment(vertical='bottom'))
    s2=NamedStyle(name="s2",fill=PatternFill("solid",fgColor="9A41FE"),font=Font(name='Calibri',size=12,bold=True,color="FFFFFF")),alignment=Alignment(vertical='center'))
    s3=NamedStyle(name="s3",fill=PatternFill("solid",fgColor="F0F0F3"),font=Font(name='Calibri',size=10),alignment=Alignment(vertical='top',wrap_text=True))
    
    ws.merge_cells('C1:K1'); ws['C1']='Основная информация'; ws.merge_cells('M1:Q1'); ws['M1']='Габариты'
    ws.merge_cells('R1:V1'); ws['R1']='Документы'; ws.merge_cells('W1:AP1'); ws['W1']='Дополнительная информация'
    for r in ['C1','L1','M1','R1','W1','AQ1']: ws[r].style = s0
    ws.row_dimensions[1].height = 41
    
    headers3 = ['Группа', 'Артикул продавца', 'Артикул WB', 'Наименование', 'Категория продавца', 'Бренд', 'Описание', 'Фото', 'Видео', 'Полное наименование товара', 'Состав', 'Баркод', 'Вес с упаковкой (кг)', 'Вес товара без упаковки (г)', 'Высота упаковки', 'Длина упаковки', 'Ширина упаковки', 'Дата окончания действия сертификата/декларации', 'Дата регистрации сертификата/декларации', 'Номер декларации соответствия', 'Номер сертификата соответствия', 'Свидетельство о регистрации СГР', 'SPF', 'Артикул OZON', 'Возрастные ограничения', 'Время нанесения', 'Действие', 'ИКПУ', 'Код упаковки', 'Комплектация', 'Назначение косметического средства', 'Назначение подарка', 'Объем товара', 'Повод', 'Раздел меню', 'Срок годности', 'Страна производства', 'ТНВЭД', 'Тип доставки', 'Тип кожи', 'Упаковка', 'Форма упаковки', 'Ставка НДС', '']
    descriptions4 = ['','Это номер или название...','Уникальный идентификатор...','','Категория выбирается...','','Если вы не заполните...',"Список ссылок...","Ссылка на видео...",'Макс. значений: 1','Макс. значений: 20','','Единица: кг','Единица: г','Единица: см','Единица: см','Единица: см','Макс. значений: 1','Макс. значений: 1','Макс. значений: 1','Макс. значений: 1','Макс. значений: 1
