from flask import Flask, render_template, request, jsonify, send_from_directory
import os
from main import run_parser, create_excel_file, parse_input

# Инициализация Flask приложения
app = Flask(__name__, static_folder='public', template_folder='public')
# Render предоставляет переменную окружения PORT для прослушивания
port = int(os.environ.get('PORT', 5000))

@app.route('/')
def index():
    """Отдает главную страницу."""
    return render_template('index.html')

@app.route('/run', methods=['POST'])
def run():
    """
    Основной эндпоинт для запуска парсинга.
    Получает данные, запускает парсер, создает Excel-файл и возвращает
    данные для таблицы и имя файла для скачивания.
    """
    try:
        input_str = request.form.get('input_str')
        if not input_str:
            return jsonify({'error': 'Необходимо ввести данные (ссылку или ID).'}), 400

        # Парсим ввод, чтобы получить ID продавца и бренда
        seller_id, brand_id = parse_input(input_str)

        # Запускаем основную логику парсинга
        mapped_data = run_parser(seller_id, brand_id)

        if not mapped_data:
            return jsonify({'error': 'Не удалось найти товары. Проверьте правильность ссылки или ID.'}), 404

        # Сразу после парсинга создаем Excel-файл
        output_path = create_excel_file(mapped_data)
        
        if not output_path:
             return jsonify({'error': 'Произошла ошибка при создании Excel-файла.'}), 500

        # Получаем только имя файла для формирования ссылки
        download_filename = os.path.basename(output_path)

        # Возвращаем два объекта: данные для таблицы и имя файла для кнопки "Скачать"
        return jsonify({
            'table_data': mapped_data,
            'download_filename': download_filename
        })

    except ValueError as e:
        # Обрабатываем ошибку некорректного ввода
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        # Обрабатываем все остальные ошибки
        print(f"Критическая ошибка в эндпоинте /run: {e}")
        return jsonify({'error': 'На сервере произошла внутренняя ошибка. Подробности в логах.'}), 500

@app.route('/download/<path:filename>')
def download_file(filename):
    """
    Эндпоинт для скачивания сгенерированного файла.
    """
    # Указываем путь к папке 'downloads'
    directory = os.path.join(os.getcwd(), 'downloads')
    try:
        # Отправляем файл пользователю
        return send_from_directory(directory, filename, as_attachment=True)
    except FileNotFoundError:
        return "Файл не найден.", 404

if __name__ == '__main__':
    # Запускаем приложение на порту, который указывает Render
    app.run(host='0.0.0.0', port=port)
