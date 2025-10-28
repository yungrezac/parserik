from flask import Flask, send_from_directory, request, session, jsonify
from main import run_parser, create_excel_file
import os

app = Flask(__name__, static_folder='public', static_url_path='')
app.secret_key = os.urandom(24)

@app.route('/')
def index():
    return send_from_directory('public', 'index.html')

@app.route('/run', methods=['POST'])
def run():
    try:
        req_data = request.get_json()
        seller_id = req_data['seller_id']
        brand_id = req_data['brand_id']
        
        if not seller_id or not brand_id:
            return jsonify({'error': "Seller ID и Brand ID не могут быть пустыми."}), 400

        data = run_parser(seller_id, brand_id)
        
        if data:
            session['data'] = data
            # Get headers from all items to ensure all columns are present
            headers = set()
            for item in data:
                headers.update(item.keys())
            
            # Sort headers for consistent column order
            sorted_headers = sorted(list(headers))
            
            return jsonify({'headers': sorted_headers, 'rows': data})
        else:
            return jsonify({'error': "Не удалось получить данные. Проверьте правильность введенных данных и попробуйте снова."}), 500

    except Exception as e:
        print(f"An error occurred: {e}")
        return jsonify({'error': f"Внутренняя ошибка сервера: {e}"}), 500

@app.route('/download')
def download():
    data = session.get('data')

    if data:
        try:
            filepath = create_excel_file(data)
            if filepath:
                return send_from_directory('downloads', os.path.basename(filepath), as_attachment=True)
        except Exception as e:
            print(f"An error occurred during file creation: {e}")
            return jsonify({'error': f"Ошибка при создании файла: {e}"}), 500
    
    return jsonify({'error': "Нет данных для создания файла. Пожалуйста, запустите парсер снова."}), 404

if __name__ == '__main__':
    app.run(debug=True, port=5001)
