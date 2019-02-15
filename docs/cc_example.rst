CerebralCortex-kernel (Examples)
================================

This directory contains some of the following examples on how to
get/save data streams and perform basic operations:

-  Window stream data into 1 minute chunks

Dependencies
------------

-  `Python3.6`_

   -  Note: Python3.7 is not compatible with some of the requirements
   -  Make sure pip version matches Python version

-  spark 2.4

   -  Download and extract `Spark 2.4`_

      -  ``cd ~/``
      -  ``wget http://apache.claz.org/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz``
      -  ``tar -xzf spark*.tar.gz``

   -  Spark requires Java-8 installed
   -  Edit ``SPARK_HOME`` in
      ``CerebralCortex-Kernel-Examples/examples/basic_windowing/run.sh``
      to point to the location you extracted Spark to.

      -  ex: ``export SPARK_HOME=~/spark-2.4.0-bin/hadoop2.7/`` if Spark
         was extracted to your home directory.

   -  ``export PYTHONPATH="${PYTHONPATH}":PATH-OF-MAIN-DIR/CerebralCortex-Kernel-Examples``
      (This is defined in
      ``CerebralCortex-Kernel-Examples/examples/basic_windowing/run.sh``)

-  MySQL > 5.7

   -  You might have to set up a MySQL user.

Installation
------------

-  ``git clone https://github.com/MD2Korg/CerebralCortex-Kernel-Examples.git``

-  ``cd CerebralCortex-Kernel-Examples``

-  ``sudo pip3 install -r requirements.txt``

   -  Note: please use appropriate pip (e.g., pip, pip3, pip3.6 etc.)
      installed on your machine

Configure CerebralCortex-Kernel
-------------------------------

-  Update MySQL settings in
   ``CerebralCortex-Kernel-Examples/conf/cerebralcortex.yml`` file, for
   example, mysql username, password etc.. Please look at the comments
   on what params shall be updated.

How to run the example code?
----------------------------

-  **Import MySQL Database:**

   -  ``cd CerebralCortex-Kernel-Examples/resources/db``
   -  ``mysql -u MySQL-USERNAME -pMySQL-PASSWORD < cerebralcortex.sql``

-  **Run example**

   -  ``cd CerebralCortex-Kernel-Examples/examples/basic_windowing``
   -  ``sh run.sh``

If everything works well then example code will produce similar output
on console as below:

::

    +--------------------+--------------------+--------------------+
    |                user|              window|       battery_level|
    +--------------------+--------------------+--------------------+
    |00000000-afb8-476...|[2019-01-09 11:35...|[100, 100, 100, 1...|
    |00000000-afb8-476...|[2019-01-09 11:41...|[97, 97, 97, 97, ...|
    |00000000-afb8-476...|[2019-01-09 11:43...|[96, 96, 96, 96, ...|
    |00000000-afb8-476...|[2019-01-09 11:38...|[99, 99, 99, 99, ...|
    |00000000-afb8-476...|[2019-01-09 11:39...|[98, 98, 98, 98, ...|
    +--------------------+--------------------+--------------------+
    only showing top 5 rows
    
    BATTERY--org.md2k.phonesensor--PHONE-windowed-data has been stored.

More Examples:
~~~~~~~~~~~~~~

Please have a look at other examples on basic usage of
cerebralcortex-kernel in
``CerebralCortex-Kernel-Examples/examples/datastream_operations``

.. _Python3.6: https://www.python.org/downloads/release/python-360/
.. _Spark 2.4: https://spark.apache.org/downloads.html