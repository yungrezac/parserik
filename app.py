
import re
import os
import json
import time
import datetime
import math
import random
from urllib.parse import urlparse
import requests
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side, NamedStyle
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

# --- Маршруты API ---
@app.route('/')
def index():
    """Отдает главную страницу."""
    return render_template('index.html')

@app.route('/categories')
def get_categories():
    """Отдает структуру категорий и подкатегорий из JSON файла."""
    try:
        with open('subcategories.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
        return jsonify(data)
    except (FileNotFoundError, json.JSONDecodeError):
        return jsonify({"error": "Файл subcategories.json не найден или поврежден."}), 500

@app.route('/stream')
def stream_run():
    """
    Основной эндпоинт для запуска парсинга и трансляции прогресса через SSE.
    """
    seller_id = request.args.get('seller_id')
    brand_id = request.args.get('brand_id')
    category = request.args.get('category')
    subcategory = request.args.get('subcategory')

    if not all([seller_id, brand_id, category, subcategory]):
        return Response("Ошибка: не указаны все необходимые параметры (seller_id, brand_id, category, subcategory).", status=400)

    def generate():
        """Функция-генератор для стриминга событий."""
        try:
            with open('subcategories.json', 'r', encoding='utf-8') as f:
                categories_data = json.load(f)
            
            columns = categories_data.get(category, {}).get(subcategory)
            if not columns or not isinstance(columns, list):
                error_payload = json.dumps({"type": "error", "message": "Для данной подкатегории не заданы столбцы для парсинга."})
                yield f"data: {error_payload}\n\n"
                return

            for progress_update in stream_parser(seller_id, brand_id, columns):
                yield f"data: {progress_update}\n\n"
                time.sleep(0.05)

        except Exception as e:
            error_payload = json.dumps({"type": "error", "message": f"Критическая ошибка на сервере: {e}"})
            yield f"data: {error_payload}\n\n"

    return Response(stream_with_context(generate()), mimetype='text/event-stream')


@app.route('/download/<path:filename>')
def download_file(filename):
    """Эндпоинт для скачивания сгенерированного файла."""
    directory = os.path.join(os.getcwd(), 'downloads')
    try:
        return send_from_directory(directory, filename, as_attachment=True)
    except FileNotFoundError:
        return "Файл не найден.", 404

# --- Логика парсинга (адаптирована из main.py) ---
def stream_parser(seller_id, brand_id, columns):
    """
    Генератор, который парсит данные и yield'ит обновления прогресса.
    Принимает `columns` для динамического формирования Excel файла.
    """
    all_products = []
    yield json.dumps({'type': 'log', 'message': 'Получение карты маршрутов WB...'})
    baskets = get_mediabasket_route_map()
    if not baskets:
        yield json.dumps({'type': 'log', 'message': 'Не удалось получить карту маршрутов.'})

    url_total_list = f"https://catalog.wb.ru/sellers/v8/filters?ab_testing=false&appType=1&curr=rub&dest=12358357&fbrand={brand_id}&lang=ru&spp=30&supplier={seller_id}&uclusters=0"
    try:
        res_total = make_request(url_total_list, headers=headers).json()
        products_total = res_total.get('data', {}).get('total', 0)
    except Exception as e:
        raise Exception(f"Ошибка при получении общего числа товаров: {e}")

    if not products_total:
        raise Exception("Товары не найдены. Проверьте ID продавца и бренда.")

    pages_count = math.ceil(products_total / 100)
    yield json.dumps({'type': 'start', 'total': products_total, 'message': f'Найдено товаров: {products_total}. Начинаем обработку...'})

    current_page, count = 1, 0
    while current_page <= pages_count:
        url_list = f"https://catalog.wb.ru/sellers/v4/catalog?ab_testing=false&appType=1&curr=rub&dest=12358357&fbrand={brand_id}&hide_dtype=13&lang=ru&page={current_page}&sort=popular&spp=30&supplier={seller_id}"
        try:
            products_on_page = make_request(url_list, headers=headers).json().get('products', [])
        except (requests.exceptions.RequestException, json.JSONDecodeError):
            current_page += 1
            time.sleep(random.uniform(1, 3))
            continue

        for item in products_on_page:
            count += 1
            yield json.dumps({'type': 'progress', 'current': count, 'total': products_total, 'message': item.get('name', '')})
            
            # --- (Этот блок оставлен без изменений, т.к. он получает детальную инфо по товару) ---
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
            # --- (Конец блока) ---
            
            all_products.append(item)
            time.sleep(random.uniform(0.1, 0.3))

        current_page += 1
        time.sleep(random.uniform(1, 2))

    yield json.dumps({'type': 'log', 'message': 'Формирование итоговой таблицы...'})
    mapped_data = map_data(all_products, columns)

    yield json.dumps({'type': 'log', 'message': 'Создание Excel-файла...'})
    output_path = create_excel_file(mapped_data, columns)
    if not output_path:
        raise Exception("Не удалось создать Excel-файл.")

    download_filename = os.path.basename(output_path)
    yield json.dumps({'type': 'result', 'download_filename': download_filename})

# --- Вспомогательные функции ---
def make_request(url, headers, timeout=10, retries=5, backoff_factor=0.5):
    for i in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException:
            time.sleep(backoff_factor * (2 ** i) + random.uniform(0, 1))
            continue
    raise Exception(f"Не удалось получить данные после {retries} попыток. URL: {url}")

def get_mediabasket_route_map():
    try:
        response = make_request('https://cdn.wbbasket.ru/api/v3/upstreams', headers=headers, timeout=5)
        data = response.json()
        return data.get('recommend', {}).get('mediabasket_route_map', [{'hosts':[]}])[0].get('hosts', [])
    except Exception:
        return []

def get_host_by_range(range_value, route_map):
    if not isinstance(route_map, list): return ''
    for host_info in route_map: 
        if host_info.get('vol_range_from') <= range_value <= host_info.get('vol_range_to'): 
            return host_info.get('host')
    return ''

def map_data(data, columns):
    """
    Собирает данные по товарам в соответствии с указанными столбцами.
    """
    # Этот маппинг можно расширить, чтобы он понимал больше полей
    # Ключ - название столбца из subcategories.json
    # Значение - лямбда-функция для извлечения данных из сырого объекта товара
    master_mapping = {
        'Артикул продавца': lambda item, adv: item.get('vendorCode', ''),
        'Бренд': lambda item, adv: item.get('brand', ''),
        'Наименование': lambda item, adv: item.get('name', ''),
        'Описание': lambda item, adv: adv.get('description', ''),
        'Состав': lambda item, adv: find_value_in_arrays(adv.get('options', []), 'Состав'),
        'Страна производства': lambda item, adv: find_value_in_arrays(adv.get('options', []), 'Страна производства'),
        'Комплектация': lambda item, adv: find_value_in_arrays(adv.get('options', []), 'Комплектация'),
        'ТНВЭД': lambda item, adv: find_value_in_arrays(adv.get('options', []), 'ТН ВЭД'),
    }
    
    new_data = []
    for item in data:
        advanced = item.get('advanced')
        if not advanced: continue
        
        row_data = {}
        for col_name in columns:
            if col_name in master_mapping:
                row_data[col_name] = master_mapping[col_name](item, advanced)
            else:
                # Если для столбца нет правила, ищем его в характеристиках "как есть"
                row_data[col_name] = find_value_in_arrays(advanced.get('options', []), col_name)

        new_data.append(row_data)
    return new_data

def find_value_in_arrays(options_array, search_name):
    if not isinstance(options_array, list): return ''
    for item in options_array: 
        if isinstance(item, dict) and item.get('name') == search_name: return item.get('value')
    return ''

def create_excel_file(data, columns):
    """Создает Excel-файл на основе предоставленных данных и столбцов."""
    if not data: return None
    if not os.path.exists("downloads"): os.makedirs("downloads")
    
    filename = f"result_{datetime.datetime.now():%Y-%m-%d_%H-%M-%S}.xlsx"
    output_path = os.path.join("downloads", filename)
    
    wb = Workbook()
    ws = wb.active
    ws.title = "Результаты парсинга"
    
    # Стили
    header_style = NamedStyle(name="header_style")
    header_style.fill = PatternFill(start_color="9A41FE", end_color="9A41FE", fill_type="solid")
    header_style.font = Font(name='Calibri', size=12, bold=True, color="FFFFFF")
    header_style.alignment = Alignment(horizontal='center', vertical='center')
    wb.add_named_style(header_style)
    
    # Заголовки
    ws.append(columns)
    for cell in ws[1]:
        cell.style = header_style
    
    # Данные
    for row_data in data:
        row_to_append = [row_data.get(header, '') for header in columns]
        ws.append(row_to_append)
        
    # Автоподбор ширины столбцов
    for col in ws.columns:
        max_length = 0
        column = col[0].column_letter
        for cell in col:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass
        adjusted_width = (max_length + 2)
        ws.column_dimensions[column].width = adjusted_width

    wb.save(output_path)
    return output_path

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port, debug=True)
