import subprocess
from src import ROOT_DIR


def capture():
    subprocess.run("pip freeze > requirements.txt", cwd=ROOT_DIR, shell=True)


if __name__ == '__main__':
    capture()
