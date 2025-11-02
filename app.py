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

# --- Аутентификация и управление пользователями (упрощенно) ---
def get_user_profile(user_data):
    return {
        'id': user_data.get('id', 'anonymous'),
        'first_name': user_data.get('first_name', 'Anonymous'),
        'last_name': user_data.get('last_name', ''),
        'username': user_data.get('username', 'anonymous'),
        'tariff': 'free',
        'created_at': datetime.datetime.utcnow().isoformat()
    }

# --- API маршруты ---
@app.route('/api/me', methods=['POST'])
def get_me():
    init_data = request.json.get('initData')
    try:
        params = dict(x.split('=', 1) for x in unquote(init_data).split('&'))
        user_data = json.loads(params.get('user', '{}'))
    except Exception:
        user_data = {}
    user_profile = get_user_profile(user_data)
    return jsonify(user_profile)

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
    seller_id = args.get('seller_id')
    brand_id = args.get('brand_id') # Может быть пустым

    if not seller_id:
        return Response("Ошибка: не указан ID продавца.", status=400)

    def generate():
        try:
            # Передаем управление генератору парсинга
            for update in stream_parser(seller_id, brand_id):
                # Отправляем каждое обновление на клиент в формате Server-Sent Events
                yield f"data: {json.dumps(update, ensure_ascii=False)}\n\n"
        except Exception as e:
            # В случае критической ошибки отправляем сообщение об ошибке
            error_message = {'type': 'error', 'message': str(e)}
            yield f"data: {json.dumps(error_message, ensure_ascii=False)}\n\n"

    return Response(stream_with_context(generate()), mimetype='text/event-stream')


@app.route('/download/<path:filename>')
def download_file(filename):
    directory = os.path.join(os.getcwd(), 'downloads')
    return send_from_directory(directory, filename, as_attachment=True)


# --- НОВАЯ ЛОГИКА ПАРСИНГА ---

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
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                time.sleep(backoff_factor * (2 ** i) + random.uniform(0, 1))
                continue
            else:
                raise e
        except requests.exceptions.RequestException:
            time.sleep(backoff_factor * (2 ** i) + random.uniform(0, 1))
            continue
    raise Exception(f"Не удалось получить данные после {retries} попыток. URL: {url}")


def stream_parser(seller_id, brand_id):
    """
    Основная логика парсинга, перестроенная в генератор, который yield'ит обновления прогресса.
    """
    all_products = []
    yield {'type': 'log', 'message': 'Получение карты маршрутов WB...'}
    baskets = get_mediabasket_route_map()
    if not baskets:
        yield {'type': 'log', 'message': 'Не удалось получить карту маршрутов. Парсинг может быть неполным.'}

    brand_query = f"&fbrand={brand_id}" if brand_id else ""
    url_total_list = f"https://catalog.wb.ru/sellers/v8/filters?ab_testing=false&appType=1&curr=rub&dest=12358357{brand_query}&lang=ru&spp=30&supplier={seller_id}&uclusters=0"
    try:
        res_total = make_request(url_total_list, headers=headers).json()
        products_total = res_total.get('data', {}).get('total', 0)
    except Exception as e:
        raise Exception(f"Критическая ошибка при получении общего числа товаров: {e}")

    if not products_total:
        raise Exception("Товары не найдены. Проверьте правильность ID продавца и бренда.")

    pages_count = math.ceil(products_total / 100)
    yield {'type': 'start', 'total': products_total, 'message': f'Найдено товаров: {products_total}.'}

    count = 0
    for page_num in range(1, pages_count + 1):
        url_list = f"https://catalog.wb.ru/sellers/v4/catalog?ab_testing=false&appType=1&curr=rub&dest=12358357&page={page_num}&sort=popular&spp=30&supplier={seller_id}{brand_query}"
        try:
            products_data = make_request(url_list, headers=headers).json()
            products_on_page = products_data.get('data', {}).get('products', [])
            if not products_on_page: continue
        except (json.JSONDecodeError, requests.exceptions.RequestException):
            continue

        for item in products_on_page:
            count += 1
            yield {'type': 'progress', 'current': count, 'total': products_total, 'message': item.get('name', '')}
            
            productId = str(item['id'])
            backetName = get_host_by_range(int(productId[:-5]), baskets)
            item['advanced'] = {}
            if backetName:
                urlItem = f"https://{backetName}/vol{productId[:-5]}/part{productId[:-3]}/{productId}/info/ru/card.json"
                try:
                    adv_res = requests.get(urlItem, headers=headers, timeout=3)
                    if adv_res.status_code == 200:
                        item['advanced'] = adv_res.json()
                except requests.exceptions.RequestException:
                    pass
            all_products.append(item)
            time.sleep(random.uniform(0.05, 0.1))
        time.sleep(random.uniform(0.5, 1.0))

    yield {'type': 'log', 'message': 'Формирование итоговой таблицы...'}
    mapped_data = map_data(all_products)

    yield {'type': 'log', 'message': 'Создание Excel-файла...'}
    output_path = create_excel_file(mapped_data)
    if not output_path:
        raise Exception("Не удалось создать Excel-файл.")

    download_filename = os.path.basename(output_path)
    yield {'type': 'result', 'download_filename': download_filename}


def get_mediabasket_route_map():
    try:
        response = make_request('https://cdn.wbbasket.ru/api/v3/upstreams', headers=headers, timeout=5)
        data = response.json()
        return data.get('recommend', {}).get('mediabasket_route_map', [{}])[0].get('hosts', [])
    except (requests.exceptions.RequestException, json.JSONDecodeError, KeyError, IndexError):
        return []

def get_host_by_range(range_value, route_map):
    if not isinstance(route_map, list): return ''
    for host_info in route_map:
        if 'vol_range_from' in host_info and 'vol_range_to' in host_info and host_info['vol_range_from'] <= range_value <= host_info['vol_range_to']:
            return host_info['host']
    return ''

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
        advanced_info_group = find_options_by_group_name(grouped_options, 'Дополнительная информация')
        cosmetics_group = find_options_by_group_name(grouped_options, 'Косметическое средство')

        cert = advanced.get('certificates', [{}])[0]
        cert_end_date = cert.get('end_date','')
        cert_reg_date = cert.get('start_date','')
        declaration_num = cert.get('number', '') if 'ЕАЭС' in cert.get('__name', '') else ''
        certificate_num = cert.get('number', '') if 'ЕАЭС' not in cert.get('__name', '') else ''

        new_item = {
            'Артикул продавца': item.get('vendorCode', ''), 'Наименование': item.get('name', ''),
            'Бренд': item.get('brand', ''), 'Описание': advanced.get('description', ''),
            'Полное наименование товара': advanced.get('name', ''),
            'Состав': find_value_in_arrays(options, advanced_info_group, search_name='Состав'),
            'Вес с упаковкой (кг)': extract_number(find_value_in_arrays(options, dimensions_group, search_name='Вес с упаковкой (кг)')),
            'Вес товара без упаковки (г)': extract_number(find_value_in_arrays(options, dimensions_group, search_name='Вес товара без упаковки (г)')),
            'Высота упаковки': extract_number(find_value_in_arrays(options, dimensions_group, search_name='Высота упаковки')),
            'Длина упаковки': extract_number(find_value_in_arrays(options, dimensions_group, search_name='Длина упаковки')),
            'Ширина упаковки': extract_number(find_value_in_arrays(options, dimensions_group, search_name='Ширина упаковки')),
            'Дата окончания действия сертификата/декларации': cert_end_date, 'Дата регистрации сертификата/декларации': cert_reg_date,
            'Номер декларации соответствия': declaration_num, 'Номер сертификата соответствия': certificate_num,
            'Свидетельство о регистрации СГР': cert.get('sgr_number', ''), 'SPF': find_value_in_arrays(options, cosmetics_group, search_name='SPF'),
            'Возрастные ограничения': find_value_in_arrays(options, advanced_info_group, search_name='Возрастные ограничения'),
            'Время нанесения': find_value_in_arrays(options, cosmetics_group, search_name='Время нанесения'),
            'Действие': find_value_in_arrays(options, cosmetics_group, search_name='Действие'),
            'Комплектация': find_value_in_arrays(options, advanced_info_group, search_name='Комплектация'),
            'Назначение косметического средства': find_value_in_arrays(options, advanced_info_group, search_name='Назначение косметического средства'),
            'Объем товара': extract_number(find_value_in_arrays(options, cosmetics_group, search_name='Объем товара')),
            'Срок годности': find_value_in_arrays(options, advanced_info_group, search_name='Срок годности'),
            'Страна производства': find_value_in_arrays(options, advanced_info_group, search_name='Страна производства'),
            'ТНВЭД': find_value_in_arrays(options, advanced_info_group, search_name='ТН ВЭД'),
            'Тип кожи': find_value_in_arrays(options, cosmetics_group, search_name='Тип кожи'),
            'Упаковка': find_value_in_arrays(options, advanced_info_group, search_name='Упаковка'),
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
    
    # Стили
    header_style_s0 = NamedStyle(name="header_style_s0", fill=PatternFill(start_color="ECDAFF", end_color="ECDAFF", fill_type="solid"), font=Font(name='Calibri', size=16), alignment=Alignment(vertical='bottom'))
    header_style_s1 = NamedStyle(name="header_style_s1", fill=PatternFill(start_color="ECDAFF", end_color="ECDAFF", fill_type="solid"), font=Font(name='Calibri', size=12), alignment=Alignment(vertical='bottom'))
    header_style_s2 = NamedStyle(name="header_style_s2", fill=PatternFill(start_color="9A41FE", end_color="9A41FE", fill_type="solid"), font=Font(name='Calibri', size=12, bold=True, color="FFFFFF"), alignment=Alignment(vertical='center'))
    description_style_s3 = NamedStyle(name="description_style_s3", fill=PatternFill(start_color="F0F0F3", end_color="F0F0F3", fill_type="solid"), font=Font(name='Calibri', size=10), alignment=Alignment(vertical='top', wrap_text=True))
    wb.add_named_style(header_style_s0); wb.add_named_style(header_style_s1); wb.add_named_style(header_style_s2); wb.add_named_style(description_style_s3)

    # Заголовки (строки 1-4)
    ws.merge_cells('C1:K1'); ws['C1'] = 'Основная информация'
    ws.merge_cells('L1:L1'); ws['L1'] = 'Размеры и Баркоды'
    ws.merge_cells('M1:Q1'); ws['M1'] = 'Габариты'
    ws.merge_cells('R1:V1'); ws['R1'] = 'Документы'
    ws.merge_cells('W1:AP1'); ws['W1'] = 'Дополнительная информация'
    ws.merge_cells('AQ1:AQ1'); ws['AQ1'] = 'Цены'
    for cell in ws[1]: cell.style = header_style_s0
    ws.row_dimensions[1].height = 41
    
    headers_row3 = ['Группа', 'Артикул продавца', 'Артикул WB', 'Наименование', 'Категория продавца', 'Бренд', 'Описание', 'Фото', 'Видео', 'Полное наименование товара', 'Состав', 'Баркод', 'Вес с упаковкой (кг)', 'Вес товара без упаковки (г)', 'Высота упаковки', 'Длина упаковки', 'Ширина упаковки', 'Дата окончания действия сертификата/декларации', 'Дата регистрации сертификата/декларации', 'Номер декларации соответствия', 'Номер сертификата соответствия', 'Свидетельство о регистрации СГР', 'SPF', 'Артикул OZON', 'Возрастные ограничения', 'Время нанесения', 'Действие', 'ИКПУ', 'Код упаковки', 'Комплектация', 'Назначение косметического средства', 'Назначение подарка', 'Объем товара', 'Повод', 'Раздел меню', 'Срок годности', 'Страна производства', 'ТНВЭД', 'Тип доставки', 'Тип кожи', 'Упаковка', 'Форма упаковки', 'Ставка НДС']
    ws.append(headers_row3)
    for cell in ws[2]: cell.style = header_style_s2
    ws.row_dimensions[2].height = 41
    
    # Данные
    if data:
        for row_data in data:
            ws.append([row_data.get(header, '') for header in headers_row3])
            
    for col in ws.columns:
        ws.column_dimensions[col[0].column_letter].width = 30

    wb.save(output_path)
    return output_path


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port, debug=True)
