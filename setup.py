import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

from distutils.core import setup
setup(
  name = 'batch_microsoft_translator',
  packages = ['batch_microsoft_translator'],
  version = '0.0.1',
  license='MIT',
  description = 'Make large amounts of translations using microsoft translator reliably',
    long_description=long_description,
    long_description_content_type="text/markdown",
  author = 'Stefan Selby',
  author_email = 'stefanselby@gmail.com', 
  url = 'https://github.com/TheTallBlonde1/batch_microsoft_translator', 
  download_url = 'https://github.com/TheTallBlonde1/batch_microsoft_translator/archive/v_0_0_1.tar.gz',
  keywords = ['MICROSOFT', 'TRANSLATOR', 'TRANSLATION', 'COGNITIVE SERVICES'],
  install_requires=[
    'pandas',
    'numpy',
    'csv',
    'time',
    'requests',
    'uuid',
    'io',
    'json'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: Public Domain",
        "Operating System :: OS Independent"
    ]
)