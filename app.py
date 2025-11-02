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
from flask_cors import CORS

# --- Инициализация Flask ---
app = Flask(__name__, static_folder='public', template_folder='public')
CORS(app)
port = int(os.environ.get('PORT', 5000))

DB_FILE = 'database.json'

# --- Работа с БД (JSON) ---
def load_db():
    if not os.path.exists(DB_FILE):
        return {'users': {}, 'parses': []}
    try:
        with open(DB_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        return {'users': {}, 'parses': []}

def save_db(data):
    with open(DB_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

# --- Аутентификация Telegram --- 
def is_valid_telegram_data(init_data_str):
    """
    Извлекает данные пользователя из initData без криптографической проверки.
    ВАЖНО: Это небезопасно и подходит только для внутреннего использования,
    так как не проверяет, что данные действительно пришли от Telegram.
    """
    if not init_data_str:
        return None, False
    try:
        params = dict(x.split('=', 1) for x in unquote(init_data_str).split('&'))
        user_data = json.loads(params['user'])
        return user_data, True
    except Exception as e:
        print(f"Could not parse initData: {e}")
        return None, False

def get_or_create_user(user_data):
    db = load_db()
    user_id = str(user_data['id'])
    if user_id not in db['users']:
        db['users'][user_id] = {
            'id': user_id,
            'first_name': user_data.get('first_name', ''),
            'last_name': user_data.get('last_name', ''),
            'username': user_data.get('username', ''),
            'tariff': 'free', # Тариф по умолчанию
            'created_at': datetime.datetime.utcnow().isoformat()
        }
        save_db(db)
    return db['users'][user_id]

# --- API маршруты ---
@app.route('/api/me', methods=['POST'])
def get_me():
    try:
        init_data = request.json.get('initData')
        user_data, is_valid = is_valid_telegram_data(init_data)
        if not is_valid:
            return jsonify({"error": "Invalid initData"}), 403
        user_profile = get_or_create_user(user_data)
        return jsonify(user_profile)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/history', methods=['POST'])
def get_history():
    try:
        init_data = request.json.get('initData')
        user_data, is_valid = is_valid_telegram_data(init_data)
        if not is_valid:
            return jsonify({"error": "Invalid initData"}), 403
        user_id = str(user_data['id'])
        db = load_db()
        user_history = [p for p in db['parses'] if p['user_id'] == user_id]
        user_history.sort(key=lambda x: x['timestamp'], reverse=True)
        return jsonify(user_history)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# --- Основные маршруты ---
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/categories')
def get_categories():
    try:
        with open('subcategories.json', 'r', encoding='utf-8') as f:
            return jsonify(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading categories: {e}")
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
            
            category = args.get('category')
            subcategory = args.get('subcategory')
            
            columns = categories_data.get(category, {}).get(subcategory)
            if not columns:
                yield f"data: {json.dumps({'type': 'error', 'message': 'Нет столбцов для подкатегории.'})}\n\n"
                return

            for update in stream_parser(args.get('seller_id'), args.get('brand_id'), columns):
                if update.get('type') == 'result':
                    db = load_db()
                    user_id = str(user_data['id'])
                    new_parse = {
                        'id': len(db['parses']) + 1, 
                        'user_id': user_id, 
                        'seller_id': args.get('seller_id'),
                        'brand_id': args.get('brand_id'), 
                        'category': category, 
                        'subcategory': subcategory,
                        'filename': update['download_filename'], 
                        'timestamp': datetime.datetime.utcnow().isoformat()
                    }
                    db['parses'].append(new_parse)
                    save_db(db)
                yield f"data: {json.dumps(update)}\n\n"
        except Exception as e:
            print(f"Stream error: {e}")
            yield f"data: {json.dumps({'type': 'error', 'message': f'Критическая ошибка: {str(e)}'})}\n\n"

    return Response(stream_with_context(generate()), mimetype='text/event-stream')

@app.route('/download/<path:filename>')
def download_file(filename):
    directory = os.path.join(os.getcwd(), 'downloads')
    try:
        return send_from_directory(directory, filename, as_attachment=True)
    except FileNotFoundError:
        return "File not found", 404

# --- ЛОГИКА ПАРСИНГА ---
headers = {
    'Accept': '*/*', 
    'Accept-Language': 'ru-RU,ru;q=0.9', 
    'Connection': 'keep-alive', 
    'Origin': 'https://www.wildberries.ru', 
    'Referer': 'https://www.wildberries.ru/', 
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
}

def stream_parser(seller_id, brand_id, columns):
    all_products = []
    yield {'type': 'log', 'message': 'Получение карты маршрутов WB...'}
    
    try:
        baskets = get_mediabasket_route_map()
        brand_query = f"&fbrand={brand_id}" if brand_id else ""
        url_total_list = f"https://catalog.wb.ru/sellers/v8/filters?ab_testing=false&appType=1&curr=rub&dest=-1257786&supplier={seller_id}{brand_query}&lang=ru&spp=30"
        
        res_total = make_request(url_total_list, headers=headers).json()
        products_total = res_total.get('data', {}).get('total', 0)
        
        if not products_total: 
            yield {'type': 'error', 'message': "Товары не найдены."}
            return
            
    except Exception as e:
        yield {'type': 'error', 'message': f"Ошибка при получении числа товаров: {e}"}
        return
        
    pages_count = math.ceil(products_total / 100)
    yield {'type': 'start', 'total': products_total, 'message': f'Найдено товаров: {products_total}.'}
    
    count = 0
    for page_num in range(1, pages_count + 1):
        url_list = f"https://catalog.wb.ru/sellers/v4/catalog?ab_testing=false&appType=1&curr=rub&dest=-1257786&page={page_num}&sort=popular&spp=30&supplier={seller_id}{brand_query}"
        try: 
            response = make_request(url_list, headers=headers)
            products = response.json().get('products', [])
        except Exception as e:
            yield {'type': 'log', 'message': f'Ошибка при загрузке страницы {page_num}: {e}'}
            continue
            
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
                except Exception as e:
                    pass
                    
            all_products.append(item)
            time.sleep(random.uniform(0.05, 0.15))
            
        time.sleep(random.uniform(0.5, 1.0))
        
    yield {'type': 'log', 'message': 'Формирование таблицы...'}
    mapped_data = map_data(all_products, columns)
    
    yield {'type': 'log', 'message': 'Создание Excel-файла...'}
    output_path = create_excel_file(mapped_data, columns)
    
    if not output_path: 
        yield {'type': 'error', 'message': "Не удалось создать Excel-файл."}
        return
        
    download_filename = os.path.basename(output_path)
    yield {'type': 'result', 'download_filename': download_filename}

def make_request(url, headers, timeout=10, retries=3, backoff_factor=0.3):
    for i in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            if i == retries - 1: 
                raise e
            time.sleep(backoff_factor * (2 ** i))

def get_mediabasket_route_map():
    try: 
        r = make_request('https://cdn.wbbasket.ru/api/v3/upstreams', headers=headers)
        return r.json().get('recommend', {}).get('mediabasket_route_map', [{}])[0].get('hosts', [])
    except Exception as e:
        print(f"Error getting route map: {e}")
        return []

def get_host_by_range(val, route_map):
    if not isinstance(route_map, list): 
        return ''
    for host in route_map: 
        if host.get('vol_range_from') <= val <= host.get('vol_range_to'): 
            return host.get('host')
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
        row_data = {}
        for col in columns:
            if col in master_mapping:
                row_data[col] = master_mapping[col](item, adv)
            else:
                row_data[col] = find_value_in_options(adv.get('options', []), col)
        new_data.append(row_data)
        
    return new_data

def find_value_in_options(options, name):
    if not isinstance(options, list): 
        return ''
    for opt in options: 
        if isinstance(opt, dict) and opt.get('name') == name: 
            return opt.get('value', '')
    return ''

def create_excel_file(data, columns):
    if not data: 
        return None
        
    os.makedirs("downloads", exist_ok=True)
    filename = f"wb_parse_{datetime.datetime.now():%Y-%m-%d_%H-%M-%S}.xlsx"
    output_path = os.path.join("downloads", filename)
    
    try:
        wb = Workbook()
        ws = wb.active
        ws.title = "Результаты"
        
        header_style = NamedStyle(
            name="header_style", 
            fill=PatternFill(start_color="6A5ACD", end_color="6A5ACD", fill_type="solid"), 
            font=Font(name='Arial', size=11, bold=True, color="FFFFFF"), 
            alignment=Alignment(horizontal='center', vertical='center')
        )
        wb.add_named_style(header_style)
        
        ws.append(columns)
        for cell in ws[1]: 
            cell.style = header_style
            
        for row_data in data: 
            ws.append([row_data.get(header, '') for header in columns])
            
        for col in ws.columns:
            max_length = max((len(str(cell.value)) for cell in col if cell.value), default=0)
            adjusted_width = (max_length + 3) if max_length < 50 else 50
            ws.column_dimensions[col[0].column_letter].width = adjusted_width
            
        wb.save(output_path)
        return output_path
        
    except Exception as e:
        print(f"Error creating Excel file: {e}")
        return None

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port, debug=True)
