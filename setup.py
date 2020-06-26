from os import path
from setuptools import setup, find_packages

here = path.abspath(path.dirname(__file__))

reqs = [
    'wheel==0.29.0',
    'pytz==2017.2',
    'mysql-connector-python==8.0.15',
    'PyYAML>=5.3.1',
    'minio==2.2.4',
    'influxdb==5.2.1',
    'pyarrow==0.15.1',
    'pympler==0.5',
    'hdfs3==0.3.0',
    'pyspark==3.0.0',
    'msgpack==0.6.1',
    'PyJWT==1.7.1',
    'pandas==1.0.5',
    'texttable',
    'numpy==1.16.1',
    'geopy==1.18.1',
    'Shapely==1.6.4.post2',
    'scikit-learn==0.22.2.post1',
    'plotly==3.10.0',
    'matplotlib',
    'cufflinks==0.16',
    'ipyleaflet',
    'scipy',
    'statsmodels==0.11.1',
    'sqlalchemy'
]

# Get the long description from the README file
with open(path.join(here, 'README.md')) as f:
    long_description = f.read()

setup(
    name="cerebralcortex-kernel",

    version='3.2.1r4',

    package_data={'': ['default.yml']},
    include_package_data=True,

    description='Backend data analytics platform for MD2K software',
    long_description=long_description,

    author='MD2K.org',
    author_email='dev@md2k.org',

    license='BSD2',
    url = 'https://github.com/MD2Korg/CerebralCortex-Kernel/',

    classifiers=[

        'Development Status :: 5 - Production/Stable',

        'Intended Audience :: Healthcare Industry',
        'Intended Audience :: Science/Research',

        'License :: OSI Approved :: BSD License',

        'Natural Language :: English',

        'Programming Language :: Python :: 3',

        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: System :: Distributed Computing'
    ],

    keywords='mHealth machine-learning data-analysis',

    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),

    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed. For an analysis of "install_requires" vs pip's
    # requirements files see:
    # https://packaging.python.org/en/latest/requirements.html
    install_requires=reqs,


    entry_points={
        'console_scripts': [
            'main=main:main'
        ]
    },

    data_files=[('/etc/rsyslog.d', ['cerebralcortex/core/resources/20-cerebralcortex.conf']), ('/etc/logrotate.d', ['cerebralcortex/core/resources/cerebralcortex']),]
)
