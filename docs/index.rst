.. CerebralCortex documentation master file, created by
   sphinx-quickstart on Wed Jan 16 22:37:55 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to CerebralCortex-Kernel's documentation!
==========================================
Cerebral Cortex is the big data cloud companion of mCerebrum designed to
support population-scale data analysis, visualization, model
development, and intervention design for mobile sensor data.

CerebralCortex-KafkaStreamPreprocessor (CC-KSP) is a apache-spark based
pub/sub system for processing incoming mobile sensor data.

You can find more information about MD2K software on our `software
website`_ or the MD2K organization on our `MD2K website`_.

CerebralCortex Kernel is part of our `CerebralCortex cloud platform`_.
CerebralCortex Kernel is mainly responsible to store/retrieve mobile
sensor data along with it’s metadata.

Note
~~~~

We have renamed following repositories. \* CerebralCortex-Platform ->
CerebralCortex \* CerebralCortex - > CerebralCortex-Kernel

Installation
------------

Minimum requirements: \* `Python3.6`_

To install:

``git clone https://github.com/MD2Korg/CerebralCortex.git``

``sudo python3 setup.py install`` 

# FAQ 
------------
**1 - Do I need whole
CerebralCortex cloud platform to use CerebralCortex-Kernal?**

No! If you want to use CerebralCortex-Kernel independently then you
would need: \* Backend storage (FileSystem/HDFS and MySQL) with some
data. Here is `some sample data`_ to play with. \* Setup the
`configurations`_ \* Use the `examples`_ to start exploring data

**2 - I do not want to use FileSystem/HDFS as NoSQL storage. How can I
change NoSQL storage backend?**

CerebralCortex-Kernel follows component based structure. This makes it
easier to add/remove features. \* Add a new class in `Data
manager-Raw`_. \* New class must have read/write methods. Here is a
sample `skeleton class`_ with mandatory methods required in the new
class. \* Create an object of new class in `Data-Raw`_ with appropriate
parameters. \* Add appropriate configurations in `cerebralcortex.yml`_
in (NoSQL
Storage)[https://github.com/MD2Korg/CerebralCortex-Kernel/blob/master/conf/cerebralcortex.yml#L8]
section.

**3 - How can I replace MySQL with another SQL storage system?**

-  Add a new class in `Data manager-SQL`_.
-  New class must implement all of the methods available in
   (stream_handler.py)[https://github.com/MD2Korg/CerebralCortex-Kernel/blob/master/cerebralcortex/core/data_manager/sql/stream_handler.py]
   class.
-  Create an object

.. _software website: https://md2k.org/software
.. _MD2K website: https://md2k.org/
.. _CerebralCortex cloud platform: https://github.com/MD2Korg/CerebralCortex
.. _Python3.6: https://www.python.org/downloads/release/python-360/
.. _some sample data: TODO
.. _configurations: https://github.com/MD2Korg/CerebralCortex-Kernel/tree/master/conf
.. _examples: TODO
.. _Data manager-Raw: https://github.com/MD2Korg/CerebralCortex-Kernel/blob/master/cerebralcortex/core/data_manager/raw/
.. _skeleton class: https://github.com/MD2Korg/CerebralCortex-Kernel/blob/master/cerebralcortex/core/data_manager/raw/storage_blueprint.py
.. _Data-Raw: https://github.com/MD2Korg/CerebralCortex-Kernel/blob/master/cerebralcortex/core/data_manager/raw/data.py
.. _cerebralcortex.yml: https://github.com/MD2Korg/CerebralCortex-Kernel/blob/master/conf/cerebralcortex.yml
.. _Data manager-SQL: https://github.com/MD2Korg/CerebralCortex-Kernel/tree/master/cerebralcortex/core/data_manager/sql


.. toctree::
   :maxdepth: 2
   :caption: Contents:



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
