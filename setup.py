from os import path
try: # for pip >= 10
    from pip._internal.req import parse_requirements
except ImportError: # for pip <= 9.0.3
    from pip.req import parse_requirements
from setuptools import setup, find_packages

here = path.abspath(path.dirname(__file__))

install_reqs = parse_requirements("./requirements.txt", session='hack')
reqs = [str(ir.req) for ir in install_reqs]

# Get the long description from the README file
with open(path.join(here, 'README.md')) as f:
    long_description = f.read()

setup(
    name="cerebralcortex-kernel",

    version='3.0.0r8',

    description='Backend data analytics platform for MD2K software',
    long_description=long_description,

    author='MD2K.org',
    author_email='dev@md2k.org',

    license='BSD2',
    url = 'https://github.com/MD2Korg/CerebralCortex/',

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

    #data_files=[('/etc/rsyslog.d', ['cerebralcortex/core/resources/20-cerebralcortex.conf']), ('/etc/logrotate.d', ['cerebralcortex/core/resources/cerebralcortex']),]
)
