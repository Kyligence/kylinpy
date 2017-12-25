from setuptools import setup

readme = open('README.rst').read()

setup(
    name='kylinpy',
    version='1.0.6',
    author='Yongjie Zhao',
    author_email='yongjie.zhao@kyligence.io',
    maintainer='Yongjie Zhao',
    maintainer_email='yongjie.zhao@kyligence.io',
    packages=['kylinpy', 'kylinpy.utils'],
    url='https://github.com/Kyligence/kylinpy',
    license='MIT License',
    description='Apache Kylin Python Client Library',
    long_description=readme,
    install_requires=['six==1.10.0', 'click==6.7'],
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
