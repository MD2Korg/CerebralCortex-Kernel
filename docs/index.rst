.. CerebralCortex documentation master file, created by
   sphinx-quickstart on Wed Jan 16 22:37:55 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to CerebralCortex-Kernel's documentation!
==========================================
Cerebral Cortex is the big data cloud companion of mCerebrum designed to
support population-scale data analysis, visualization, model
development, and intervention design for mobile sensor data.

You can find more information about MD2K software on our `software
website`_ or the MD2K organization on our `MD2K website`_.

CerebralCortex Kernel is part of our `CerebralCortex cloud platform`_.
CerebralCortex Kernel is mainly responsible to store/retrieve mobile
sensor data along with it’s metadata.

**Note**:

We have renamed following repositories. 

- CerebralCortex-Platform -> CerebralCortex 
- CerebralCortex - > CerebralCortex-Kernel

Installation
------------

CerebralCortex-Kernel is a part of CerebralCortex cloud platform. To
test the complete cloud platform, please visit `CerebralCortex`_.

CerebralCortex-Kernel requires minimum `Python3.6`_. To install
CerebralCortex-Kernel as an API:

``git clone https://github.com/MD2Korg/CerebralCortex.git``

``sudo python3 setup.py install``

Code Example
------------

Example folder (``cerebralcortex-kernel/cerebralcortex/example``)
contains basic examples on how to use CerebralCortex-Kernel api.

FAQ
---

**1 - Do I need whole CerebralCortex cloud platform to use
CerebralCortex-Kernal?**

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

-  Add a new class in [Data
   manager-SQL](https://github.com/MD2Korg/CerebralCortex-Kernel/tree/master/cerebralcortex/core

.. _software website: https://md2k.org/software
.. _MD2K website: https://md2k.org/
.. _CerebralCortex cloud platform: https://github.com/MD2Korg/CerebralCortex
.. _CerebralCortex: https://github.com/MD2Korg/CerebralCortex
.. _Python3.6: https://www.python.org/downloads/release/python-360/
.. _some sample data: TODO
.. _configurations: https://github.com/MD2Korg/CerebralCortex-Kernel/tree/master/conf
.. _examples: TODO
.. _Data manager-Raw: https://github.com/MD2Korg/CerebralCortex-Kernel/blob/master/cerebralcortex/core/data_manager/raw/
.. _skeleton class: https://github.com/MD2Korg/CerebralCortex-Kernel/blob/master/cerebralcortex/core/data_manager/raw/storage_blueprint.py
.. _Data-Raw: https://github.com/MD2Korg/CerebralCortex-Kernel/blob/master/cerebralcortex/core/data_manager/raw/data.py
.. _cerebralcortex.yml: https://github.com/MD2Korg/CerebralCortex-Kernel/blob/master/conf/cerebralcortex.yml


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   cc_example
   rst/cerebralcortex.rst
   rst/cerebralcortex.core.config_manager.rst
   rst/cerebralcortex.core.data_manager.object.rst
   rst/cerebralcortex.core.data_manager.raw.rst
   rst/cerebralcortex.core.data_manager.rst
   rst/cerebralcortex.core.data_manager.sql.rst
   rst/cerebralcortex.core.data_manager.time_series.rst
   rst/cerebralcortex.core.datatypes.rst
   rst/cerebralcortex.core.log_manager.rst
   rst/cerebralcortex.core.messaging_manager.rst
   rst/cerebralcortex.core.metadata_manager.rst
   rst/cerebralcortex.core.metadata_manager.stream.rst
   rst/cerebralcortex.core.metadata_manager.user.rst
   rst/cerebralcortex.core.rst
   rst/cerebralcortex.core.test_suite.rst
   rst/cerebralcortex.core.util.rst
   rst/cerebralcortex.examples.rst
   rst/cerebralcortex.examples.util.rst
   rst/cerebralcortex.test_suite.rst
   rst/cerebralcortex.test_suite.util.rst



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
