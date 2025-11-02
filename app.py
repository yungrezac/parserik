
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

# --- Конфигурация для хранения данных ---
DATA_DIR = 'data'
USERS_FILE = os.path.join(DATA_DIR, 'users.json')
HISTORY_FILE = os.path.join(DATA_DIR, 'history.json')

# --- Функции для работы с файлами ---
def load_data(file_path):
    if not os.path.exists(file_path):
        return {}
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        return {}

def save_data(file_path, data):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

# --- Аутентификация и управление пользователями ---
def is_valid_telegram_data(init_data_str):
    if not init_data_str:
        return None, False
    try:
        params = dict(x.split('=', 1) for x in unquote(init_data_str).split('&'))
        user_data = json.loads(params['user'])
        return user_data, True
    except Exception as e:
        print(f"Could not parse initData: {e}")
        return None, False

def get_user_profile(user_data):
    users = load_data(USERS_FILE)
    user_id = str(user_data.get('id'))
    
    if user_id not in users:
        users[user_id] = {
            'id': user_id,
            'first_name': user_data.get('first_name', ''),
            'last_name': user_data.get('last_name', ''),
            'username': user_data.get('username', ''),
            'tariff': 'free',
            'created_at': datetime.datetime.utcnow().isoformat()
        }
        save_data(USERS_FILE, users)
        
    return users[user_id]

# --- API маршруты ---
@app.route('/api/me', methods=['POST'])
def get_me():
    init_data = request.json.get('initData')
    user_data, is_valid = is_valid_telegram_data(init_data)
    if not is_valid:
        return jsonify({"error": "Invalid initData"}), 403
    
    user_profile = get_user_profile(user_data)
    return jsonify(user_profile)

@app.route('/api/history', methods=['POST'])
def get_history():
    init_data = request.json.get('initData')
    user_data, is_valid = is_valid_telegram_data(init_data)
    if not is_valid:
        return jsonify({"error": "Invalid initData"}), 403
        
    user_id = str(user_data.get('id'))
    history = load_data(HISTORY_FILE)
    user_history = history.get(user_id, [])
    return jsonify(user_history)

@app.route('/api/history/add', methods=['POST'])
def add_to_history():
    init_data = request.json.get('initData')
    user_data, is_valid = is_valid_telegram_data(init_data)
    if not is_valid:
        return jsonify({"error": "Invalid initData"}), 403
        
    user_id = str(user_data.get('id'))
    history_item = request.json.get('historyItem')

    history = load_data(HISTORY_FILE)
    if user_id not in history:
        history[user_id] = []
    
    history[user_id].insert(0, history_item)
    # Ограничение на 50 записей
    history[user_id] = history[user_id][:50]
    
    save_data(HISTORY_FILE, history)
    return jsonify({"success": True})


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
        return jsonify({"error": "Файл subcategories.json не найден или поврежден."}), 500

@app.route('/stream')
def stream_run():
    args = request.args
    user_data, is_valid = is_valid_telegram_data(args.get('initData'))
    if not is_valid:
        return Response(json.dumps({'type': 'error', 'message': 'Ошибка аутентификации.'}), mimetype='text/event-stream')

    if not all(k in args for k in ['seller_id', 'category', 'subcategory']):
        return Response("Ошибка: не указаны параметры.", status=400)

    def generate():
        try:
            with open('subcategories.json', 'r', encoding='utf-8') as f:
                categories_data = json.load(f)
            columns = categories_data.get(args.get('category'), {}).get(args.get('subcategory'))
            if not columns:
                yield f"data: {json.dumps({'type': 'error', 'message': 'Нет столбцов для подкатегории.'})}\n\n"
                return
            
            user_id = str(user_data.get('id'))

            for update in stream_parser(args.get('seller_id'), args.get('brand_id'), columns, user_id):
                yield f"data: {json.dumps(update)}\n\n"

        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': f'Критическая ошибка: {e}'})}\n\n"

    return Response(stream_with_context(generate()), mimetype='text/event-stream')


@app.route('/download/<path:filename>')
def download_file(filename):
    directory = os.path.join(os.getcwd(), 'downloads')
    return send_from_directory(directory, filename, as_attachment=True)

# --- ЛОГИКА ПАРСИНГА ---
headers = {
    'Accept': '*/*', 'Accept-Language': 'ru-RU,ru;q=0.9', 'Connection': 'keep-alive', 'Origin': 'https://www.wildberries.ru',
    'Referer': 'https://www.wildberries.ru/', 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
}

def stream_parser(seller_id, brand_id, columns, user_id):
    all_products = []
    yield {'type': 'log', 'message': 'Получение карты маршрутов WB...'}
    baskets = get_mediabasket_route_map()
    brand_query = f"&fbrand={brand_id}" if brand_id else ""
    url_total_list = f"https://catalog.wb.ru/sellers/v8/filters?ab_testing=false&appType=1&curr=rub&dest=-1257786&supplier={seller_id}{brand_query}&lang=ru&spp=30"
    try:
        res_total = make_request(url_total_list, headers=headers).json()
        products_total = res_total.get('data', {}).get('total', 0)
    except Exception as e:
        raise Exception(f"Ошибка при получении числа товаров: {e}")
    if not products_total: raise Exception("Товары не найдены.")
    pages_count = math.ceil(products_total / 100)
    yield {'type': 'start', 'total': products_total, 'message': f'Найдено товаров: {products_total}.'}
    count = 0
    for page_num in range(1, pages_count + 1):
        url_list = f"https://catalog.wb.ru/sellers/v4/catalog?ab_testing=false&appType=1&curr=rub&dest=-1257786&page={page_num}&sort=popular&spp=30&supplier={seller_id}{brand_query}"
        try: products = make_request(url_list, headers=headers).json().get('products', [])
        except Exception: continue
        for item in products:
            count += 1
            yield {'type': 'progress', 'current': count, 'total': products_total, 'message': item.get('name', '')}
            productId = str(item['id'])
            backetName = get_host_by_range(int(productId[:-5]), baskets)
            item['advanced'] = {}
            if backetName:
                urlItem = f"https://{backetName}/vol{productId[:-5]}/part{productId[:-3]}/{productId}/info/ru/card.json"
                try:
                    adv_res = make_request(urlItem, headers, timeout=2)
                    item['advanced'] = adv_res.json()
                except Exception: pass
            all_products.append(item)
            time.sleep(random.uniform(0.05, 0.15))
        time.sleep(random.uniform(0.5, 1.0))
    yield {'type': 'log', 'message': 'Формирование таблицы...'}
    mapped_data = map_data(all_products, columns)
    yield {'type': 'log', 'message': 'Создание Excel-файла...'}
    output_path = create_excel_file(mapped_data, columns, user_id)
    if not output_path: raise Exception("Не удалось создать Excel-файл.")
    download_filename = os.path.basename(output_path)

    yield {'type': 'result', 'download_filename': download_filename}

def make_request(url, headers, timeout=10, retries=3, backoff_factor=0.3):
    for i in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            if i == retries - 1: raise e
            time.sleep(backoff_factor * (2 ** i))

def get_mediabasket_route_map():
    try:
        r = make_request('https://cdn.wbbasket.ru/api/v3/upstreams', headers=headers)
        return r.json().get('recommend', {}).get('mediabasket_route_map', [{}])[0].get('hosts', [])
    except: return []

def get_host_by_range(val, route_map):
    if not isinstance(route_map, list): return ''
    for host in route_map:
        if host.get('vol_range_from') <= val <= host.get('vol_range_to'): return host.get('host')
    return ''

def map_data(data, columns):
    master_mapping = {
        'Артикул продавца': lambda i, a: i.get('vendorCode', ''), 'Бренд': lambda i, a: i.get('brand', ''),
        'Наименование': lambda i, a: i.get('name', ''), 'Описание': lambda i, a: a.get('description', ''),
        'Состав': lambda i, a: find_value_in_options(a.get('options', []), 'Состав'),
        'Страна производства': lambda i, a: find_value_in_options(a.get('options', []), 'Страна производства'),
        'Комплектация': lambda i, a: find_value_in_options(a.get('options', []), 'Комплектация'),
        'ТНВЭД': lambda i, a: find_value_in_options(a.get('options', []), 'ТН ВЭД'),
    }
    new_data = []
    for item in data:
        adv = item.get('advanced', {})
        row_data = {col: master_mapping.get(col, lambda i, a: find_value_in_options(a.get('options', []), col))(item, adv) for col in columns}
        new_data.append(row_data)
    return new_data

def find_value_in_options(options, name):
    if not isinstance(options, list): return ''
    for opt in options:
        if isinstance(opt, dict) and opt.get('name') == name: return opt.get('value')
    return ''

def create_excel_file(data, columns, user_id):
    if not data: return None
    
    # Создаем директорию для пользователя
    user_downloads_dir = os.path.join('downloads', user_id)
    os.makedirs(user_downloads_dir, exist_ok=True)
    
    filename = f"wb_parse_{datetime.datetime.now():%Y-%m-%d_%H-%M-%S}.xlsx"
    output_path = os.path.join(user_downloads_dir, filename)
    
    wb = Workbook()
    ws = wb.active
    ws.title = "Результаты"
    header_style = NamedStyle(name="header_style", fill=PatternFill(start_color="6A5ACD", end_color="6A5ACD", fill_type="solid"), font=Font(name='Arial', size=11, bold=True, color="FFFFFF"), alignment=Alignment(horizontal='center', vertical='center'))
    wb.add_named_style(header_style)
    ws.append(columns)
    for cell in ws[1]: cell.style = header_style
    for row_data in data: ws.append([row_data.get(header, '') for header in columns])
    for col in ws.columns:
        max_length = max((len(str(cell.value)) for cell in col if cell.value), default=0)
        ws.column_dimensions[col[0].column_letter].width = (max_length + 3) if max_length < 50 else 50
    wb.save(output_path)
    return output_path

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port, debug=True)
