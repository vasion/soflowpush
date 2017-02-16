from setuptools import setup

import sys

if sys.version_info[0]==2:
    reqs = ["aiohttp", "asynqp", "pika", "argparse", "watchtower"]
else:
    #for client only
    reqs = ["pika"]

setup(name='soflowpush',
      version='0.1',
      description='Distributed speading out firebase push notifications with AMQP',
      url='',
      author='Momchil Rogelov',
      author_email='momchilrogelov@gmail.com',
      license='MIT',
      packages=["soflowpush"],
      zip_safe=False,
      install_requires=reqs,
      scripts=['bin/soflowpush-worker']
      )
