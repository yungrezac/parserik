import sys
import os

# 1. Укажите путь к вашему проекту на PythonAnywhere
# Например: path = '/home/YourUsername/my-project'
path = '/home/axentli/flaskProject'
if path not in sys.path:
    sys.path.insert(0, path)

# 2. Укажите путь к вашему виртуальному окружению (если вы его создали)
# activate_this = '/home/YourUsername/.virtualenvs/my-virtualenv/bin/activate_this.py'
# with open(activate_this) as f:
#     code = compile(f.read(), activate_this, 'exec')
#     exec(code, dict(__file__=activate_this))

# 3. Импорт главного Flask-приложения
from app import app as application
