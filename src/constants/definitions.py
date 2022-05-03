from os import path
from os.path import dirname

ROOT_DIR = dirname(dirname(dirname(__file__)))  # This is your Project Root
LOG_PATH = path.join(ROOT_DIR, 'logs')
