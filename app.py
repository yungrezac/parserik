from flask import Flask, render_template, request, send_from_directory, Response, stream_with_context
import os
import time
import json
from main import stream_parser, parse_input

# Инициализация Flask приложения
app = Flask(__name__, static_folder='public', template_folder='public')
port = int(os.environ.get('PORT', 5000))

@app.route('/')
def index():
    """Отдает главную страницу."""
    return render_template('index.html')

@app.route('/stream', methods=['GET'])
def stream_run():
    """
    Эндпоинт, который использует Server-Sent Events (SSE)
    для трансляции прогресса парсинга в реальном времени.
    """
    input_str = request.args.get('input_str')
    if not input_str:
        return Response(status=400)

    def generate():
        """Функция-генератор, которая будет транслировать события"""
        try:
            try:
                seller_id, brand_id = parse_input(input_str)
            except ValueError as e:
                error_payload = json.dumps({"type": "error", "message": str(e)})
                yield f"data: {error_payload}\n\n"
                return

            for progress_update in stream_parser(seller_id, brand_id):
                yield f"data: {progress_update}\n\n"
                time.sleep(0.05)

        except Exception as e:
            error_payload = json.dumps({
                "type": "error", 
                "message": f"Критическая ошибка на сервере: {str(e)}"
            })
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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port)
