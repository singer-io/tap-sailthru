from setuptools import find_packages, setup

requirements_file = "requirements.txt"

with open("README.md") as f:
    readme = f.read()

setup(
    name="tap-sailthru",
    version="0.1.0",
    description="Singer.io tap for the SailThru API",
    long_description=readme,
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_sailthru"],
    install_requires=open(requirements_file).readlines(),
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
