import re
from urllib.parse import urlparse
import requests
import time
import random
import datetime
import math
import os
from openpyxl import Workbook


# Ввод и парсинг

def check_string(s):
    """
    Проверяет, соответствует ли строка формату списка ID брендов, разделённых %3B.

    Args:
        s (str): Входная строка.

    Returns:
        bool: True, если формат корректен, иначе False.
    """
    return bool(re.fullmatch(r'(\d+%3B)*\d+', s))


def parse_input(input_str):
    """
    Парсит строку ввода и извлекает sellerId и brandId.

    Поддерживает как прямой ввод, так и ссылку WB с параметром fbrand.

    Args:
        input_str (str): Строка, содержащая sellerId и brandId или URL.

    Returns:
        tuple: (sellerId, brandId)

    Raises:
        ValueError: Если формат некорректен.
    """
    parts = input_str.split()

    # Проверяем количество частей
    if len(parts) > 2:
        raise ValueError("Необходимо указать два параметра через пробел")

    sellerId = ''
    brandId = ''

    if len(parts) == 2:
        sellerId = parts[0]
        brandId = parts[1]

        if not sellerId.isdigit() or not check_string(brandId):
            raise ValueError("Необходимо указать число и ID бренда(ов)")
    else:
        parseResult = urlparse(input_str)
        path = str(parseResult.path)
        query = str(parseResult.query)

        sellerId = path.split('/')[2]

        brandStartIndex = query.find('fbrand')
        brandEndIndex = query.find('&', brandStartIndex)

        if brandEndIndex == -1:
            brandItems = query[brandStartIndex:]
        else:
            brandItems = query[brandStartIndex:brandEndIndex]

        brandId = brandItems.split('=')[1]

    return (sellerId, brandId)


# Задержки

def get_delay():
    """
    Возвращает случайную безопасную задержку между 2 и 5 секундами.

    Returns:
        float: Время задержки.
    """
    return random.uniform(2, 5)


def get_delay_safe():
    """
    Возвращает увеличенную безопасную задержку между 5 и 10 секундами.

    Returns:
        float: Время задержки.
    """
    return random.uniform(5, 10)


def get_delay_aggressive():
    """
    Возвращает агрессивную задержку между 1 и 2 секундами.

    Returns:
        float: Время задержки.
    """
    return random.uniform(1, 2)


# Вспомогательные

def safe_get_field(obj, field_name):
    """
    Безопасно получает поле из объекта dict или объекта с атрибутами.

    Args:
        obj (Any): Объект или словарь.
        field_name (str): Название поля.

    Returns:
        Any: Значение поля или пустая строка.
    """
    if obj is None:
        return ''
    if isinstance(obj, dict):
        return obj.get(field_name, '')
    return getattr(obj, field_name, '')


def find_options_by_group_name(grouped_options, group_name):
    """
    Ищет список опций в сгруппированных опциях по имени группы.

    Args:
        grouped_options (list): Список групп опций.
        group_name (str): Название группы.

    Returns:
        list: Найденные опции или пустой список.
    """
    try:
        return next((group['options'] for group in grouped_options if group['group_name'] == group_name), [])
    except (TypeError, KeyError):
        return []


def get_host_by_range(range_value, route_map):
    """
    Возвращает хост корзины, соответствующий переданному числу по vol_range.

    Args:
        range_value (int): Значение volume из ID товара.
        route_map (list): Список словарей с диапазонами и хостами.

    Returns:
        str: Хост корзины.

    Raises:
        ValueError: Если значение не попадает ни в один диапазон.
    """
    if not route_map:
        return ''

    # Проверяем, что route_map имеет корректную структуру
    if not isinstance(route_map, list):
        raise ValueError("Некорректная структура route_map")

    # Проходим по всем элементам в списке
    for host_info in route_map:
        # Проверяем, попадает ли значение в диапазон
        if host_info['vol_range_from'] <= range_value <= host_info['vol_range_to']:
            return host_info['host']

    # Если значение не попадает ни в один диапазон
    raise ValueError(
        f"Значение {range_value} не попадает ни в один из доступных диапазонов")


# Получение данных с WB

def get_mediabasket_route_map():
    """
    Получает карту маршрутов корзин с сервера Wildberries.

    Returns:
        list: Список словарей с диапазонами и хостами.
    """
    try:
        # Формируем URL эндпоинта
        url = 'https://cdn.wbbasket.ru/api/v3/upstreams'

        # Отправляем GET-запрос
        response = requests.get(url)

        # Проверяем статус ответа
        if response.status_code == 200:
            data = response.json()

            # Проверяем наличие вложенных полей
            if 'recommend' in data:
                recommend_data = data['recommend']
                if 'mediabasket_route_map' in recommend_data:
                    mediabasket_route_map = recommend_data['mediabasket_route_map'][0]
                    if 'hosts' in mediabasket_route_map:
                        return mediabasket_route_map['hosts']
        return []

    except requests.exceptions.RequestException as e:
        print(f"Ошибка при выполнении запроса: {e}")
        return []
    except Exception as e:
        print(f"Произошла ошибка: {e}")
        return []


def fetch_data(sellerId, brandId, backets):
    """
    Получает и обогащает карточки товаров по sellerId и brandId.

    Args:
        sellerId (str): ID продавца.
        brandId (str): ID брендов (может быть несколько через %3B).
        backets (list): Карта маршрутов корзин.

    Returns:
        list: Список товаров с расширенной информацией.
    """
    allProducts = []

    productsPerPage = 100
    urlTotalList = f"https://catalog.wb.ru/sellers/v8/filters?ab_testing=false&appType=1&curr=rub&dest=12358357&fbrand={brandId}&lang=ru&spp=30&supplier={sellerId}&uclusters=0"
    responseTotal = requests.get(urlTotalList)
    resTotal = responseTotal.json()
    productTotalData = resTotal['data']
    productsTotal = productTotalData['total']
    pagesCount = math.ceil(productsTotal / productsPerPage)
    currentPage = 1
    count = 1

    while (currentPage <= pagesCount):
        urlList = f"https://catalog.wb.ru/sellers/v4/catalog?ab_testing=false&appType=1&curr=rub&dest=12358357&fbrand={brandId}&hide_dtype=13&lang=ru&page={currentPage}&sort=popular&spp=30&supplier={sellerId}"
        response = requests.get(urlList)
        products = response.json()['products']

        for item in products:
            print(f'Получено {count}/{productsTotal}')
            productId = str(item['id'])
            backetName = get_host_by_range(
                int(productId[:len(productId) - 5]), backets)
            backetNumber = 1
            isAutoServer = True if len(backetName) > 0 else False
            isNewLap = False

            while True:
                backetFormattedNumber = f"0{backetNumber}" if backetNumber < 10 else str(
                    backetNumber)
                urlItem = f"https://{backetName if isAutoServer else f'basket-{backetFormattedNumber}.wbbasket.ru'}/vol{productId[:len(productId) - 5]}/part{productId[:len(productId) - 3]}/{productId}/info/ru/card.json"

                # Отправляем запрос
                productResponse = requests.get(urlItem)

                # Проверяем статус ответа
                if productResponse.status_code == 200:
                    # При успешном ответе используем оптимальную задержку
                    print('Использована оптимальная задержка')
                    time.sleep(get_delay_aggressive())

                    product = productResponse.json()
                    item['advanced'] = product
                    break
                elif productResponse.status_code == 429:  # Too Many Requests
                    # При получении ошибки "слишком много запросов"
                    # Увеличиваем задержку и делаем повтор
                    print('Использована безопасная задержка')
                    time.sleep(get_delay())
                    continue
                elif productResponse.status_code == 404:  # Not Found
                    backetNumber = backetNumber + 1 if backetNumber < 24 else 1

                    if isNewLap and backetName == 24:
                        item['advanced'] = {}
                        break

                    if backetName == 1:
                        isNewLap = True

                    continue
                else:
                    # При других ошибках используем агрессивную задержку
                    print('Использована агрессивная задержка')
                    time.sleep(get_delay_safe())
                    continue

            count += 1
        currentPage += 1
        allProducts.extend(products)
    return allProducts


# Парсинг и маппинг

def find_first_of_set(string, char_set, start=0):
    """
    Находит индекс первого символа из множества в строке, начиная с позиции.

    Args:
        string (str): Строка для поиска.
        char_set (set): Множество символов.
        start (int): Стартовая позиция.

    Returns:
        int: Индекс первого найденного символа, или -1 если не найден.
    """
    for i, char in enumerate(string, start):
        if char in char_set:
            return i
    return -1


def find_by_name(data, search_name):
    """
    Ищет значение по имени в списке словарей.

    Args:
        data (list): Список словарей с ключами 'name' и 'value'.
        search_name (str): Имя поля.

    Returns:
        str: Значение поля, если найдено, иначе '-'.
    """
    for item in data:
        if item['name'] == search_name:
            return item['value']
    return '-'


def find_value_in_arrays(array1, array2, search_name):
    """
    Ищет значение по имени в двух массивах словарей.

    Args:
        array1 (list): Первый список словарей.
        array2 (list): Второй список словарей.
        search_name (str): Название поля.

    Returns:
        str: Найденное значение, либо '' если не найдено.
    """
    # Проверяем первый массив
    for item in array1:
        if item['name'] == search_name:
            return item['value']

    # Если не нашли в первом, проверяем второй массив
    for item in array2:
        if item['name'] == search_name:
            return item['value']

    # Если не нашли ни в одном массиве
    return ''


def extract_number(value: str) -> float | str:
    """
    Извлекает первое числовое значение из строки.

    Args:
        value (str): Строка, из которой нужно извлечь число.

    Returns:
        float | str: Число в формате float, если найдено и успешно преобразовано,
                     иначе пустая строка ''.
    """
    if not isinstance(value, str):
        return ''

    match = re.search(r'\d+(?:[.,]\d+)?', value)
    if match:
        number_str = match.group().replace(',', '.')
        try:
            return float(number_str)
        except ValueError:
            return ''
    return ''


def map_data(data):
    """
    Преобразует список товаров в унифицированный формат для выгрузки.

    Args:
        data (list): Сырые данные товаров с расширенной информацией.

    Returns:
        list: Список словарей с отформатированными полями.
    """
    new_data = []
    for item in data:
        new_item = {
            'id': safe_get_field(item, 'id'),
            'name': safe_get_field(item, 'name'),
            'category': safe_get_field(item, 'entity'),
            'brand': safe_get_field(item, 'brand'),
        }

        advanced = item.get('advanced', {})
        description = safe_get_field(advanced, 'description')
        new_item.update({'description': description})

        options = safe_get_field(advanced, 'options')
        compound = find_by_name(options, 'Состав')

        if options and compound != '-':
            new_item.update({'compound': compound})
        else:
            compoundStartIndex = description.find("Состав:")
            if compoundStartIndex != -1:
                endIndex = find_first_of_set(
                    description, '.;', compoundStartIndex)
                compound = description[compoundStartIndex + 8: endIndex]
                new_item.update({'compound': compound})
            else:
                new_item.update({'compound': ''})

        groupedOptions = safe_get_field(advanced, 'grouped_options')

        dimensions = []
        advancedInfo = []

        if groupedOptions:
            dimensions = find_options_by_group_name(groupedOptions, 'Габариты')
            advancedInfo = find_options_by_group_name(
                groupedOptions, 'Дополнительная информация')

        gross = extract_number(find_value_in_arrays(
            options, dimensions, 'Вес с упаковкой (кг)'))
        net = extract_number(find_value_in_arrays(
            options, dimensions, 'Вес товара без упаковки (г)'))
        length = extract_number(find_value_in_arrays(
            options, dimensions, 'Высота упаковки'))
        height = extract_number(find_value_in_arrays(
            options, dimensions, 'Длина упаковки'))
        width = extract_number(find_value_in_arrays(
            options, dimensions, 'Ширина упаковки'))
        equipment = find_value_in_arrays(options, advancedInfo, 'Комплектация')
        expirationDate = find_value_in_arrays(
            options, advancedInfo, 'Срок годности')
        country = find_value_in_arrays(
            options, advancedInfo, 'Страна производства')
        package = find_value_in_arrays(options, advancedInfo, 'Упаковка')
        packageItemsCount = extract_number(find_value_in_arrays(
            options, advancedInfo, 'Количество предметов в упаковке'))

        new_item.update({'gross': gross})
        new_item.update({'net': net})
        new_item.update({'height': height})
        new_item.update({'length': length})
        new_item.update({'width': width})
        new_item.update({'equipment': equipment})
        new_item.update({'expiration_date': expirationDate})
        new_item.update({'country': country})
        new_item.update({'package': package})
        new_item.update({'package_items_count': packageItemsCount})

        new_item.update({'tax': 20})
        new_data.append(new_item)

    return new_data


# Excel

def generate_filename(base_name="result"):
    """
    Генерирует имя Excel-файла с текущими датой и временем.

    Args:
        base_name (str): Базовое имя файла.

    Returns:
        str: Сформированное имя файла.
    """
    current_time = datetime.datetime.now()
    filename = f"{base_name}_{current_time:%Y-%m-%d_%H-%M-%S}.xlsx"
    return filename


def create_excel_file(data):
    """
    Создаёт Excel-файл и записывает туда данные.

    Args:
        data (list): Список словарей с данными для записи.

    Returns:
        str: Путь к созданному файлу.
    """
    if not data:
        print("Нет данных для записи в файл.")
        return None

    if not os.path.exists("downloads"):
        os.makedirs("downloads")

    output_path = os.path.join("downloads", generate_filename())
    wb = Workbook()
    ws = wb.active
    
    headers = list(data[0].keys())
    ws.append(headers)

    for row_data in data:
        row = [row_data.get(header, '') for header in headers]
        ws.append(row)
    
    wb.save(output_path)
    print(f"Данные успешно записаны в файл {output_path}")
    return output_path


def run_parser(seller_id, brand_id):
    """
    Запускает процесс парсинга данных, но не сохраняет в файл.
    Returns:
        list: Список словарей с отпарсенными данными.
    """
    # получение распределения серверов
    baskets = get_mediabasket_route_map()
    # получение товаров
    data = fetch_data(seller_id, brand_id, baskets)
    # парсинг данных
    mapped_data = map_data(data)
    # возвращаем данные
    return mapped_data


if __name__ == "__main__":
    def input_data():
        """
        Запраширует у пользователя ввод: ссылку WB или ID продавца и бренда(ов), разделённые пробелом.

        Returns:
            str: Введённая пользователем строка.
        """
        s = input(
            "Введите ссылку WB или ID магазина и ID бренда(ов) через пробел:")
        return s

    # ввод данных
    url = input_data()
    # парсинг ввода
    parsedInput = parse_input(url)
    # получаем данные
    mapped_data = run_parser(parsedInput[0], parsedInput[1])
    # создаем файл
    if mapped_data:
        create_excel_file(mapped_data)
