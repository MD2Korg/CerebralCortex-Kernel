from os import path
from setuptools import setup, find_packages

here = path.abspath(path.dirname(__file__))

reqs = [
    'wheel',
    'pytz',
    'mysql-connector-python==8.0.15',
    'PyYAML>=5.3.1',
    'SQLAlchemy-Utils==0.36.6',
    'minio',
    'influxdb==5.2.1',
    'pyarrow==3.0.0',
    'pympler',
    'hdfs3==0.3.0',
    'pyspark==3.1.1',
    'msgpack',
    'PyJWT',
    'pandas',
    'texttable',
    'numpy',
    'geopy',
    'scikit-learn',
    'plotly',
    'matplotlib',
    'cufflinks',
    'ipyleaflet',
    'scipy',
    'py-ecg-detectors',
    'statsmodels',
    'sqlalchemy==1.3.17',
    'pennprov==2.2.9',
    'shapely'
]


# Get the long description from the README file.
with open(path.join(here, 'README.md')) as f:
    long_description = f.read()


if __name__ == '__main__':
    setup(
        name="cerebralcortex-kernel",

        version='3.3.7',

        package_data={'': ['default.yml']},

        description='Backend data analytics platform for MD2K software',
        long_description_content_type='text/markdown',
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
        # simple. Or you can use find_packages()
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

    )

