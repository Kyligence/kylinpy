import codecs
import os.path
import re

from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))

def read(*parts):
    return codecs.open(os.path.join(here, *parts), 'r').read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


setup(
    name='kylinpy',
    version=find_version("kylinpy", "__init__.py"),
    author='Yongjie Zhao',
    author_email='yongjie.zhao@kyligence.io',
    maintainer='Yongjie Zhao',
    maintainer_email='yongjie.zhao@kyligence.io',
    packages=['kylinpy', 'kylinpy.utils'],
    url='https://github.com/Kyligence/kylinpy',
    license='MIT License',
    description='Apache Kylin Python Client Library',
    long_description=open('README.rst').read(),
    install_requires=['six>=1.0.0', 'click>=6.7'],
    setup_requires=['pytest-runner'],
    tests_require=['pytest', 'coverage', 'mock'],
    extras_require={
        'sqlalchemy': ['sqlalchemy>=1.1.0'],
    },
    keywords=['kylin', 'kap', 'cli', 'sqlalchemy dialect'],
    classifiers=[
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    entry_points={
        "sqlalchemy.dialects": ["kylin=kylinpy.sqla_dialect:KylinDialect"],
        'console_scripts': ['kylinpy=kylinpy.cli:main'],
    }
)
