
import re
import os
import json
import time
import datetime
import math
import random
from urllib.parse import urlparse, urlencode
import requests
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment, NamedStyle
from flask import Flask, render_template, request, send_from_directory, Response, stream_with_context, jsonify

# --- Инициализация Flask ---
app = Flask(__name__, static_folder='public', template_folder='public')
port = int(os.environ.get('PORT', 5000))

# --- Заголовки для запросов ---
headers = {
    'Accept': '*/*',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'Connection': 'keep-alive',
    'Origin': 'https://www.wildberries.ru',
    'Referer': 'https://www.wildberries.ru/',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'cross-site',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
}

# --- Функции для работы с API WB ---
def make_request(url, headers, timeout=10, retries=5, backoff_factor=0.5):
    for i in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            # Для сетевых ошибок или кодов состояния, не равных 2xx
            if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 429:
                 # Особая обработка для Too Many Requests
                sleep_time = backoff_factor * (2 ** i) + random.uniform(0, 1)
                time.sleep(sleep_time)
                continue
            # Для других ошибок просто попробуем еще раз
            time.sleep(backoff_factor * (2 ** i))
            continue
    raise Exception(f"Не удалось получить данные после {retries} попыток. URL: {url}")

def find_subject_id_from_seller_filters(seller_id, subcategory_name):
    """Находит ID категории (subject) через фильтры продавца."""
    url = f"https://catalog.wb.ru/sellers/v8/filters?ab_testing=false&appType=1&curr=rub&dest=-1257786&supplier={seller_id}&lang=ru"
    try:
        response = make_request(url, headers)
        filters = response.json().get('data', {}).get('filters', [])
        
        # Ищем блок фильтров по категориям
        for f in filters:
            if f.get('id') == 'subject':
                for category in f.get('items', []):
                    if category.get('name') == subcategory_name:
                        return category.get('id')
        return None
    except Exception:
        return None


# --- Маршруты API ---
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/categories')
def get_categories():
    try:
        with open('subcategories.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
        return jsonify(data)
    except (FileNotFoundError, json.JSONDecodeError):
        return jsonify({"error": "Файл subcategories.json не найден или поврежден."}), 500

@app.route('/stream')
def stream_run():
    seller_id = request.args.get('seller_id')
    category_name = request.args.get('category')
    subcategory_name = request.args.get('subcategory')

    if not all([seller_id, category_name, subcategory_name]):
        return Response("Ошибка: укажите ID продавца, категорию и подкатегорию.", status=400)

    def generate():
        try:
            # 1. Ищем ID подкатегории через фильтры продавца
            yield json.dumps({"type": "log", "message": f"Поиск ID для подкатегории '{subcategory_name}' у продавца {seller_id}..."})
            subject_id = find_subject_id_from_seller_filters(seller_id, subcategory_name)

            if not subject_id:
                error_msg = f"Не удалось найти ID для '{subcategory_name}' у данного продавца. Возможно, у него нет товаров в этой категории."
                yield json.dumps({"type": "error", "message": error_msg})
                return

            yield json.dumps({"type": "log", "message": f"Найден ID: {subject_id}. Начинаем парсинг..."})

            # 2. Получаем столбцы для Excel из subcategories.json
            with open('subcategories.json', 'r', encoding='utf-8') as f:
                categories_data = json.load(f)
            columns = categories_data.get(category_name, {}).get(subcategory_name)
            if not columns:
                error_msg = "Не найдены настройки столбцов для данной подкатегории."
                yield json.dumps({"type": "error", "message": error_msg})
                return

            # 3. Запускаем парсер с найденным subject_id
            parser_generator = stream_parser(seller_id, subject_id, columns)
            for progress_update in parser_generator:
                yield f"data: {progress_update}\n\n"
                time.sleep(0.05)

        except Exception as e:
            error_payload = json.dumps({"type": "error", "message": f"Критическая ошибка: {e}"})
            yield f"data: {error_payload}\n\n"

    return Response(stream_with_context(generate()), mimetype='text/event-stream')

@app.route('/download/<path:filename>')
def download_file(filename):
    directory = os.path.join(os.getcwd(), 'downloads')
    try:
        return send_from_directory(directory, filename, as_attachment=True)
    except FileNotFoundError:
        return "Файл не найден.", 404

# --- Основная логика парсинга ---
def stream_parser(seller_id, subject_id, columns):
    all_products = []
    yield json.dumps({'type': 'log', 'message': 'Получение карты маршрутов корзин...'})
    baskets = get_mediabasket_route_map()

    filter_query = f"subject={subject_id}"
    url_total_list = f"https://catalog.wb.ru/sellers/v8/filters?ab_testing=false&appType=1&curr=rub&dest=-1257786&{filter_query}&supplier={seller_id}&lang=ru&spp=30"
    
    try:
        res_total = make_request(url_total_list, headers).json()
        products_total = res_total.get('data', {}).get('total', 0)
    except Exception as e:
        raise Exception(f"Ошибка при получении общего числа товаров: {e}")

    if not products_total:
        raise Exception("Товары в данной категории у продавца не найдены.")

    pages_count = math.ceil(products_total / 100)
    yield json.dumps({'type': 'start', 'total': products_total, 'message': f'Найдено товаров: {products_total}. Обработка...'})

    count = 0
    for page_num in range(1, pages_count + 1):
        url_list = f"https://catalog.wb.ru/sellers/v4/catalog?ab_testing=false&appType=1&curr=rub&dest=-1257786&hide_dtype=13&page={page_num}&sort=popular&spp=30&supplier={seller_id}&{filter_query}"
        
        try:
            response = make_request(url_list, headers)
            products_on_page = response.json().get('products', [])
        except (requests.exceptions.RequestException, json.JSONDecodeError):
            continue

        for item in products_on_page:
            count += 1
            yield json.dumps({'type': 'progress', 'current': count, 'total': products_total, 'message': item.get('name', '')})
            
            productId = str(item['id'])
            backetName = get_host_by_range(int(productId[:-5]), baskets)
            backetNumber, isAutoServer = 1, bool(backetName)
            while True:
                if not isAutoServer and backetNumber > 12: item['advanced'] = {}; break
                backetFormattedNumber = f"0{backetNumber}" if backetNumber < 10 else str(backetNumber)
                urlItem = f"https://{backetName if isAutoServer else f'basket-{backetFormattedNumber}.wbbasket.ru'}/vol{productId[:-5]}/part{productId[:-3]}/{productId}/info/ru/card.json"
                try:
                    productResponse = requests.get(urlItem, headers=headers, timeout=3)
                    if productResponse.status_code == 200: item['advanced'] = productResponse.json(); break
                    if not isAutoServer and productResponse.status_code == 404: backetNumber += 1; continue
                    item['advanced'] = {}; break
                except requests.exceptions.RequestException: item['advanced'] = {}; break
            
            all_products.append(item)
            time.sleep(random.uniform(0.05, 0.15))

        time.sleep(random.uniform(0.5, 1.0))

    yield json.dumps({'type': 'log', 'message': 'Формирование итоговой таблицы...'})
    mapped_data = map_data(all_products, columns)

    yield json.dumps({'type': 'log', 'message': 'Создание Excel-файла...'})
    output_path = create_excel_file(mapped_data, columns, subcategory_name)
    if not output_path:
        raise Exception("Не удалось создать Excel-файл.")

    download_filename = os.path.basename(output_path)
    yield json.dumps({'type': 'result', 'download_filename': download_filename})

# --- Вспомогательные функции для обработки данных и Excel ---
def get_mediabasket_route_map():
    try:
        r = make_request('https://cdn.wbbasket.ru/api/v3/upstreams', headers, timeout=5)
        return r.json().get('recommend', {}).get('mediabasket_route_map', [{}])[0].get('hosts', [])
    except: return []

def get_host_by_range(val, route_map):
    if not isinstance(route_map, list): return ''
    for host in route_map: 
        if 'vol_range_from' in host and 'vol_range_to' in host and host['vol_range_from'] <= val <= host['vol_range_to']: 
            return host['host']
    return ''

def map_data(data, columns):
    new_data = []
    for item in data:
        advanced = item.get('advanced', {})
        row_data = {}
        # Динамическое сопоставление на основе списка столбцов
        for col_name in columns:
            # Сначала ищем в базовой информации о товаре
            if col_name == 'Артикул продавца':
                row_data[col_name] = item.get('vendorCode', '')
            elif col_name == 'Бренд':
                row_data[col_name] = item.get('brand', '')
            elif col_name == 'Наименование':
                row_data[col_name] = item.get('name', '')
            # Затем ищем в расширенной информации (включая 'options')
            elif col_name == 'Описание':
                row_data[col_name] = advanced.get('description', '')
            else:
                # Общий поиск в 'options' для всех остальных полей
                row_data[col_name] = find_value_in_options(advanced.get('options', []), col_name)
        new_data.append(row_data)
    return new_data

def find_value_in_options(options, name):
    if not isinstance(options, list): return ''
    for opt in options: 
        if isinstance(opt, dict) and opt.get('name') == name: return opt.get('value')
    return ''

def create_excel_file(data, columns, subcategory_name):
    if not data: return None
    if not os.path.exists("downloads"): os.makedirs("downloads")
    
    safe_subcategory_name = re.sub(r'[\/*?:\[\]]', '', subcategory_name)
    filename = f"{safe_subcategory_name}_{datetime.datetime.now():%Y-%m-%d}.xlsx"
    output_path = os.path.join("downloads", filename)
    
    wb = Workbook()
    ws = wb.active
    ws.title = safe_subcategory_name
    
    header_style = NamedStyle(name="header_style_v3")
    header_style.fill = PatternFill(start_color="9A41FE", end_color="9A41FE", fill_type="solid")
    header_style.font = Font(name='Calibri', size=12, bold=True, color="FFFFFF")
    header_style.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
    wb.add_named_style(header_style)
    
    ws.append(columns)
    for cell in ws[1]:
        cell.style = header_style
    ws.row_dimensions[1].height = 40

    for row_data in data:
        ws.append([row_data.get(header, '') for header in columns])
        
    for col in ws.columns:
        max_length = 0
        column_letter = col[0].column_letter
        for cell in col:
            try: # Улавливаем возможные ошибки, если значение не строка
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass
        adjusted_width = (max_length + 2) if max_length < 45 else 45
        ws.column_dimensions[column_letter].width = adjusted_width

    wb.save(output_path)
    return output_path

# --- Запуск Flask ---
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port, debug=True)
