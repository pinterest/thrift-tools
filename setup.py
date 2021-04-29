from setuptools import find_packages, setup

import os
import sys


PYTHON3 = sys.version_info > (3, )
HERE = os.path.abspath(os.path.dirname(__file__))


def readme():
    with open(os.path.join(HERE, 'README.rst')) as f:
        return f.read()


def get_version():
    with open(os.path.join(HERE, 'thrift_tools/__init__.py'), 'r') as f:
        content = ''.join(f.readlines())
    env = {}
    if PYTHON3:
        exec(content, env, env)
    else:
        compiled = compile(content, 'get_version', 'single')
        eval(compiled, env, env)
    return env['__version__']


setup(
    name='thrift-tools',
    version=get_version(),
    description='Thrift protocol analyzer',
    long_description=readme(),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Topic :: System :: Distributed Computing',
        'Topic :: System :: Networking',
        ],
    keywords='thrift pcap',
    url='https://github.com/pinterest/thrift-tools',
    author='Raul Gutierrez Segales',
    author_email='rgs@pinterest.com',
    license='Apache',
    packages=find_packages(),
    test_suite='thrift_tools.tests',
    scripts=['bin/thrift-tool', 'bin/thrift-file-reader'],
      install_requires=[
          'ansicolors',
          'dpkt==1.9.2',
          'ptsd==0.2.0',
          'scapy==2.4.5',
          'thrift==0.11.0',
          'tabulate',
      ],
      tests_require=[
          'ansicolors',
          'dpkt',
          'nose',
          'ptsd==0.2.0',
          'scapy==2.4.5',
          'six==1.12.0',
          'thrift==0.11.0',
          'tabulate',
      ],
      extras_require={
          'test': [
              'ansicolors',
              'dpkt',
              'nose',
              'ptsd==0.2.0',
              'scapy==2.4.5',
              'six==1.12.0',
              'thrift==0.11.0',
              'tabulate',
              ],
      },
      include_package_data=True,
      zip_safe=False
)
