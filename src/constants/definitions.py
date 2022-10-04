from os import path
from os.path import dirname

ROOT_DIR = dirname(dirname(dirname(__file__)))  # This is your Project Root
LOG_PATH = path.join(ROOT_DIR, 'logs')

if __name__ == '__main__':
    print(f"{ROOT_DIR=}")
    print(f"{LOG_PATH=}")
