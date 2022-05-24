from setuptools import setup, find_packages


with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='wikipedia network analysis',
    version='0.2.0',
    description='Network analysis of wikipedia',
    long_description=readme,
    author='riggedCoinflip',
    url='https://github.com/riggedcoinflip/wikipedia-network-analysis',
    license=license,
    packages=find_packages(exclude=('tests', 'docs'))
)
