.. CerebralCortex documentation master file, created by
   sphinx-quickstart on Wed Jan 16 22:37:55 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to CerebralCortex-Kernel's documentation!
====================================================
CerebralCortex Kernel
=====================

Cerebral Cortex is the big data cloud companion of mCerebrum designed to
support population-scale data analysis, visualization, model
development, and intervention design for mobile sensor data.

You can find more information about MD2K software on our `software
website`_ or the MD2K organization on our `MD2K website`_.

CerebralCortex Kernel is part of our `CerebralCortex cloud platform`_.
CerebralCortex Kernel is mainly responsible to store/retrieve mobile
sensor data along with it's metadata.

**Note**:

We have renamed following repositories.

-  CerebralCortex-Platform -> CerebralCortex
-  CerebralCortex - > CerebralCortex-Kernel

Installation
------------

CerebralCortex-Kernel is a part of CerebralCortex cloud platform. To
test the complete cloud platform, please visit `CerebralCortex`_.

CerebralCortex-Kernel requires minimum `Python3.6`_. To install
CerebralCortex-Kernel as an API:

``pip3 install cerebralcortex-kernel``

-  Note: please use appropriate pip (e.g., pip, pip3, pip3.6 etc.)
   installed on your machine

Code Example
------------

Please have a look at `example code`_, if you want to only see an
example on how to call CerebralCortex-Kernel API.

FAQ
---

**1 - Do I need whole CerebralCortex cloud platform to use
CerebralCortex-Kernal?**

No! If you want to use CerebralCortex-Kernel independently then you
would need:

-  Backend storage (FileSystem/HDFS and MySQL) with some data. Here is
   `some sample data`_ to play with.
-  Setup the `configurations`_
-  Use the `examples`_ to start exploring data

**2 - I do not want to use FileSystem/HDFS as NoSQL storage. How can I
change NoSQL storage backend?**

CerebralCortex-Kernel follows component based structure. This makes it
easier to add/remove features.

-  Add a new class in `Data manager-Raw`_.
-  New class must have read/write methods. Here is a sample `skeleton
   class`_ with mandatory methods required in the new class.
-  Create an object of new class in `Data-Raw`_ with appropriate
   parameters.
-  Add appropriate configurations in `cerebralcortex.yml`_ in (NoSQL
   Storage)[https://github.com/MD2Korg/CerebralCortex-Kernel/blob/master/conf/cerebralcortex.yml#L8]
   section.

**3 - How can I replace MySQL with another SQL storage system?**

-  Add a new class in [Data manager-SQ

.. _software website: https://md2k.org/software
.. _MD2K website: https://md2k.org/
.. _CerebralCortex cloud platform: https://github.com/MD2Korg/CerebralCortex
.. _CerebralCortex: https://github.com/MD2Korg/CerebralCortex
.. _Python3.6: https://www.python.org/downloads/release/python-360/
.. _example code: https://github.com/MD2Korg/CerebralCortex-Kernel-Examples
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


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
