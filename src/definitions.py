import os
from os.path import dirname

ROOT_DIR = dirname(dirname(__file__)) # This is your Project Root
LOG_PATH = os.path.join(ROOT_DIR, 'logs')
