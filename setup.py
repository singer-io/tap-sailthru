from setuptools import find_packages, setup
from os.path import abspath, dirname, join

requirements_file = "requirements.txt"
ROOT_DIR = abspath(dirname(__file__))


with open(join(ROOT_DIR, "README.md"), encoding="utf-8") as f:
    readme = f.read()

setup(
    name="tap-sailthru",
    version="1.0.1",
    description="Singer.io tap for the SailThru API",
    long_description=readme,
    long_description_content_type='text/markdown',
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_sailthru"],
    install_requires=[
	'requests==2.32.4',
	'singer-python==5.13.2',
    ],
    extras_require= {
          'dev': [
              'pylint==2.7.4',
          ]
      },
    entry_points="""
    [console_scripts]
    tap-sailthru=tap_sailthru:main
    """,
    packages=find_packages(exclude=["tests"]),
    package_data = {
        "schemas": ["tap_sailthru/schemas/*.json"]
    },
    include_package_data=True,
)
