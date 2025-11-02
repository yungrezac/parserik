import re
import os
import json
import time
import datetime
import math
import random
from urllib.parse import unquote
import requests
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment, NamedStyle
from flask import Flask, render_template, request, send_from_directory, Response, stream_with_context, jsonify

# --- Инициализация Flask ---
app = Flask(__name__, static_folder='public', template_folder='public')
port = int(os.environ.get('PORT', 5000))

# --- Управление пользователями и API ---
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

# --- Основные маршруты ---
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

# --- Маршрут для стриминга парсинга ---
@app.route('/stream')
def stream_run():
    args = request.args
    seller_id = args.get('seller_id')
    brand_id = args.get('brand_id')
    category = args.get('category')
    subcategory = args.get('subcategory')

    if not all([seller_id, category, subcategory]):
        return Response("Ошибка: не указаны все параметры (seller_id, category, subcategory).", status=400)

    def generate():
        try:
            with open('subcategories.json', 'r', encoding='utf-8') as f:
                categories_data = json.load(f)
            columns = categories_data.get(category, {}).get(subcategory)

            if not columns:
                error_msg = {'type': 'error', 'message': f'Конфигурация столбцов для "{category} -> {subcategory}" не найдена.'}
                yield f"data: {json.dumps(error_msg, ensure_ascii=False)}\n\n"
                return

            for update in stream_parser(seller_id, brand_id, columns):
                yield f"data: {json.dumps(update, ensure_ascii=False)}\n\n"
        except Exception as e:
            error_message = {'type': 'error', 'message': f'Критическая ошибка: {e}'}
            yield f"data: {json.dumps(error_message, ensure_ascii=False)}\n\n"

    return Response(stream_with_context(generate()), mimetype='text/event-stream')


# --- ЯДРО ПАРСИНГА ---
headers = {
    'Accept': '*/*', 'Accept-Language': 'ru-RU,ru;q=0.9', 'Connection': 'keep-alive',
    'Origin': 'https://www.wildberries.ru', 'Referer': 'https://www.wildberries.ru/',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
}

def make_request(url, retries=5, backoff_factor=0.5):
    for i in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException:
            time.sleep(backoff_factor * (2 ** i) + random.uniform(0, 1))
    raise Exception(f"Не удалось получить данные после {retries} попыток.")

def get_mediabasket_route_map():
    try:
        data = make_request('https://cdn.wbbasket.ru/api/v3/upstreams').json()
        return data['recommend']['mediabasket_route_map'][0]['hosts']
    except Exception:
        return []

def get_host_by_range(range_value, route_map):
    if not isinstance(route_map, list): return ''
    for host_info in route_map:
        if host_info.get('vol_range_from') <= range_value <= host_info.get('vol_range_to'):
            return host_info['host']
    return ''

def stream_parser(seller_id, brand_id, columns):
    all_products = []
    yield {'type': 'log', 'message': 'Получение карты маршрутов WB...'}
    baskets = get_mediabasket_route_map()

    brand_query = f"&fbrand={brand_id}" if brand_id and brand_id.strip() else ""
    url_total = f"https://catalog.wb.ru/sellers/v8/filters?ab_testing=false&appType=1&curr=rub&dest=-1257786&supplier={seller_id}{brand_query}&lang=ru&spp=30"
    try:
        products_total = make_request(url_total).json().get('data', {}).get('total', 0)
    except Exception as e:
        raise Exception(f"Ошибка при получении числа товаров: {e}")

    if not products_total:
        raise Exception("Товары не найдены. Проверьте ID.")

    pages_count = math.ceil(products_total / 100)
    yield {'type': 'start', 'total': products_total, 'message': f'Найдено товаров: {products_total}'}

    count = 0
    for page_num in range(1, pages_count + 1):
        url_list = f"https://catalog.wb.ru/sellers/v4/catalog?ab_testing=false&appType=1&curr=rub&dest=-1257786&page={page_num}&sort=popular&spp=30&supplier={seller_id}{brand_query}"
        try:
            products_on_page = make_request(url_list).json().get('data', {}).get('products', [])
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
                    res = requests.get(urlItem, headers=headers, timeout=2)
                    if res.status_code == 200: item['advanced'] = res.json()
                except Exception: pass
            all_products.append(item)
            time.sleep(random.uniform(0.05, 0.1))
        time.sleep(random.uniform(0.5, 1.0))

    yield {'type': 'log', 'message': 'Сопоставление данных...'}
    mapped_data = map_data(all_products, columns)
    
    yield {'type': 'log', 'message': 'Создание Excel-файла...'}
    output_path = create_excel_file(mapped_data, columns)
    if not output_path: raise Exception("Не удалось создать Excel-файл.")
    
    yield {'type': 'result', 'download_filename': os.path.basename(output_path)}


def find_value_in_options(options, name):
    if not isinstance(options, list): return ''
    for opt in options:
        if isinstance(opt, dict) and opt.get('name') == name: return opt.get('value')
    return ''

def map_data(data, columns):
    master_mapping = {
        'Артикул продавца': lambda i, a: i.get('vendorCode', ''),
        'Бренд': lambda i, a: i.get('brand', ''),
        'Наименование': lambda i, a: i.get('name', ''),
        'Описание': lambda i, a: a.get('description', ''),
        'Состав': lambda i, a: find_value_in_options(a.get('options', []), 'Состав'),
        'Страна производства': lambda i, a: find_value_in_options(a.get('options', []), 'Страна производства'),
        'Комплектация': lambda i, a: find_value_in_options(a.get('options', []), 'Комплектация'),
        'ТНВЭД': lambda i, a: find_value_in_options(a.get('options', []), 'ТН ВЭД'),
    }
    
    new_data = []
    for item in data:
        adv = item.get('advanced', {})
        row_data = {col: master_mapping.get(col, lambda i, a, c=col: find_value_in_options(a.get('options', []), c))(item, adv) for col in columns}
        new_data.append(row_data)
    return new_data

def create_excel_file(data, columns):
    if not data: return None
    os.makedirs("downloads", exist_ok=True)
    filename = f"result_{datetime.datetime.now():%Y-%m-%d_%H-%M-%S}.xlsx"
    output_path = os.path.join("downloads", filename)
    
    wb = Workbook()
    ws = wb.active
    ws.title = "Результаты"
    
    # Стили
    header_style = NamedStyle(name="header_style", fill=PatternFill("solid", fgColor="6A5ACD"), font=Font(name='Arial', size=11, bold=True, color="FFFFFF"), alignment=Alignment(horizontal='center', vertical='center'))
    wb.add_named_style(header_style)
    
    # Заголовки
    ws.append(columns)
    for cell in ws[1]:
        cell.style = header_style
        
    # Данные
    for row_data in data:
        ws.append([row_data.get(header, '') for header in columns])
        
    # Ширина столбцов
    for col in ws.columns:
        max_length = max((len(str(cell.value)) for cell in col if cell.value), default=0)
        ws.column_dimensions[col[0].column_letter].width = min(max_length + 3, 50)
        
    wb.save(output_path)
    return output_path


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port, debug=True)
