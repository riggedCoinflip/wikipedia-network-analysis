import subprocess
from src import ROOT_DIR


def install():
    subprocess.run("pip install -r requirements.txt", cwd=ROOT_DIR, shell=True)


if __name__ == '__main__':
    install()
