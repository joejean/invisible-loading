try:
  from setuptools import setup
except ImportError:
  from distutils.core import setup

config = {
  'name': 'Invisible Loader',
  'description': 'System that builds database on the fly',
  'url': 'my project url',
  'download_url': 'url to download project',
  'version': '0.1',
  'install_requires': ['nose'],
  'packages': ['inviloader'],
  'scripts': [],
  'authors': ['Jermsak Jermsurawong','Joe Jean'],
  'author_email':['jermsak.jermsurawong@nyu.edu ', 'joe@joejean.net']
}

setup(**config)
