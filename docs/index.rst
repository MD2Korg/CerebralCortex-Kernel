CerebralCortex Kernel
=====================

Cerebral Cortex is the big data cloud companion of mCerebrum designed to
support population-scale data analysis, visualization, model
development, and intervention design for mobile sensor data.

You can find more information about MD2K software on our `software
website`_ or the MD2K organization on our `MD2K website`_.

CerebralCortex Kernel is part of our `CerebralCortex cloud platform`_.
CerebralCortex-Kernel is capable of parallelizing tasks and scale a job
to n-number of cores/machines. CerebralCortex Kernel offers some builtin
features as follows:

Installation
------------

Dependencies
~~~~~~~~~~~~

CerebralCortex Kernel requires ``java 8`` to run. Java 8 prior to
version 8u92 support is deprecated as of CerebralCortex-Kernel 3.3.0. -
check java version - ``java -version`` - set ``JAVA_HOME`` to ``java 8``
- OR start python shell with ``JAVA_HOME=/path/to/java/Home python3``

Install using pip
~~~~~~~~~~~~~~~~~

CerebralCortex-Kernel requires minimum `Python3.6`_. To install
CerebralCortex-Kernel as an API:

``pip3 install cerebralcortex-kernel``

-  Note: please use appropriate pip (e.g., pip, pip3, pip3.6 etc.)
   installed on your machine

Install from source code
~~~~~~~~~~~~~~~~~~~~~~~~

-  Clone repo -
   ``git clone https://github.com/MD2Korg/CerebralCortex-Kernel.git``
-  ``cd CerebralCortex-Kernel``
-  ``python3 setup.py install``

Usage
-----

::

   from cerebralcortex.kernel import Kernel
   CC = Kernel(cc_configs="default")

   # to view default configs
   print(CC.config)

   # default data storage path is
   # /user/home/folder/cc_data

By default Kernel will load default configs. Please have a look at all
available `configurations`_ for CerebralCortex-Kernel. You may also load
config files as:

``CC = Kernel(configs_dir_path="dir/path/to/configs/", new_study=True)``

How to use builtin algorithms
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Using builtin algorithms are as easy as loading data, passing it to
algorithm and get the results. Below is an example on how to compute CGM
Glucose Variability Metrics.

-  `Download Glucose Data`_. The device used to collect glucose was the
   `Dexcom G6 Continuous Glucose Monitor`_
-  Install Cerebral Cortex Kernel ``pip install cerebralcortex-kernel``
-  Open terminal and start python3 shell

Python Code
~~~~~~~~~~~

::

   # import packages
   from cerebralcortex.kernel import Kernel
   from cerebralcortex.algorithms.glucose.glucose_variability_metrics import glucose_var

   # Create Kernel object
   CC = Kernel(cc_configs="default", new_study=True)

   # Read sample CSV data
   ds = CC.read_csv("/path/of/the/downloaded/file/sample.csv", stream_name="cgm_glucose_variability_metrics", header=

.. _software website: https://md2k.org/software
.. _MD2K website: https://md2k.org/
.. _CerebralCortex cloud platform: https://github.com/MD2Korg/CerebralCortex
.. _Python3.6: https://www.python.org/downloads/release/python-360/
.. _configurations: https://github.com/MD2Korg/CerebralCortex-Kernel/tree/master/conf
.. _Download Glucose Data: https://github.com/MD2Korg/CerebralCortex-Kernel/blob/master/cerebralcortex/test_suite/sample_data/cgm_glucose_variability_metrics/sample.csv
.. _Dexcom G6 Continuous Glucose Monitor: https://www.dexcom.com/g6-cgm-system