# CerebralCortex Kernel
Cerebral Cortex is the big data cloud companion of mCerebrum designed to support population-scale data analysis, visualization, model development, and intervention design for mobile sensor data.

You can find more information about MD2K software on our [software website](https://md2k.org/software) or the MD2K organization on our [MD2K website](https://md2k.org/).

CerebralCortex Kernel is part of our [CerebralCortex cloud platform](https://github.com/MD2Korg/CerebralCortex). CerebralCortex Kernel is mainly responsible to store/retrieve mobile sensor data along with it's metadata. 

**Note**:

We have renamed following repositories.

* CerebralCortex-Platform -> CerebralCortex
* CerebralCortex - >  CerebralCortex-Kernel

## Documentation

- [Source code documentation](https://cerebralcortex-kernel.readthedocs.io/en/3.0.1/)

## Installation
CerebralCortex-Kernel is a part of CerebralCortex cloud platform. To test the complete cloud platform, please visit [CerebralCortex](https://github.com/MD2Korg/CerebralCortex).

CerebralCortex-Kernel requires minimum [Python3.6](https://www.python.org/downloads/release/python-360/). To install CerebralCortex-Kernel as an API:

```pip3 install cerebralcortex-kernel```

- Note: please use appropriate pip (e.g., pip, pip3, pip3.6 etc.) installed on your machine 


## Code Example
Please have a look at [example code](https://github.com/MD2Korg/CerebralCortex-Kernel-Examples), if you want to only see an example on how to call CerebralCortex-Kernel API.
 
## FAQ
**1 - Do I need whole CerebralCortex cloud platform to use CerebralCortex-Kernal?**

No! If you want to use CerebralCortex-Kernel independently then you would need: 
* Backend storage (FileSystem/HDFS and MySQL) with some data. Here is [some sample data](TODO) to play with.
* Setup the [configurations](https://github.com/MD2Korg/CerebralCortex-Kernel/tree/master/conf)
* Use the [examples](TODO) to start exploring data

**2 - I do not want tCerebralCortex-kernel (Examples)
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

.. _Python3.6: https://www.python.org/downloads/release/python-360/
.. _Spark 2.4: https://spark.apache.org/downloads.htmlo use FileSystem/HDFS as NoSQL storage. How can I change NoSQL storage backend?**

CerebralCortex-Kernel follows component based structure. This makes it easier to add/remove features. 
* Add a new class in [Data manager-Raw](https://github.com/MD2Korg/CerebralCortex-Kernel/blob/master/cerebralcortex/core/data_manager/raw/). 
* New class must have read/write methods. Here is a sample [skeleton class](https://github.com/MD2Korg/CerebralCortex-Kernel/blob/master/cerebralcortex/core/data_manager/raw/storage_blueprint.py) with mandatory methods required in the new class.
* Create an object of new class in [Data-Raw](https://github.com/MD2Korg/CerebralCortex-Kernel/blob/master/cerebralcortex/core/data_manager/raw/data.py) with appropriate parameters.
* Add appropriate configurations in [cerebralcortex.yml](https://github.com/MD2Korg/CerebralCortex-Kernel/blob/master/conf/cerebralcortex.yml) in (NoSQL Storage)[https://github.com/MD2Korg/CerebralCortex-Kernel/blob/master/conf/cerebralcortex.yml#L8] section.

**3 - How can I replace MySQL with another SQL storage system?** 

* Add a new class in [Data manager-SQL](https://github.com/MD2Korg/CerebralCortex-Kernel/tree/master/cerebralcortex/core/data_manager/sql). 
* New class must implement all of the methods available in (stream_handler.py)[https://github.com/MD2Korg/CerebralCortex-Kernel/blob/master/cerebralcortex/core/data_manager/sql/stream_handler.py] class.
* Create an object of new class in [Data-SQL](https://github.com/MD2Korg/CerebralCortex-Kernel/blob/master/cerebralcortex/core/data_manager/sql/data.py) with appropriate parameters.
* Add appropriate configurations in [cerebralcortex.yml](https://github.com/MD2Korg/CerebralCortex-Kernel/blob/master/conf/cerebralcortex.yml) in (Relational Storage)[https://github.com/MD2Korg/CerebralCortex-Kernel/blob/master/conf/cerebralcortex.yml#L31] section.

**4 - Where are all the backend storage related classes/methods?**    

In [Data manager-Raw](https://github.com/MD2Korg/CerebralCortex-Kernel/blob/master/cerebralcortex/core/data_manager/). You can add/change any backend storage.


## Contributing
Please read our [Contributing Guidelines](https://md2k.org/contributing/contributing-guidelines.html) for details on the process for submitting pull requests to us.

We use the [Python PEP 8 Style Guide](https://www.python.org/dev/peps/pep-0008/).

Our [Code of Conduct](https://md2k.org/contributing/code-of-conduct.html) is the [Contributor Covenant](https://www.contributor-covenant.org/).

Bug reports can be submitted through [JIRA](https://md2korg.atlassian.net/secure/Dashboard.jspa).

Our discussion forum can be found [here](https://discuss.md2k.org/).

## Versioning

We use [Semantic Versioning](https://semver.org/) for versioning the software which is based on the following guidelines.

MAJOR.MINOR.PATCH (example: 3.0.12)

  1. MAJOR version when incompatible API changes are made,
  2. MINOR version when functionality is added in a backwards-compatible manner, and
  3. PATCH version when backwards-compatible bug fixes are introduced.

For the versions available, see [this repository's tags](https://github.com/MD2Korg/CerebralCortex/tags).

## Contributors

Link to the [list of contributors](https://github.com/MD2Korg/CerebralCortex-Kernel/graphs/contributors) who participated in this project.

## License

This project is licensed under the BSD 2-Clause - see the [license](https://md2k.org/software-under-the-hood/software-uth-license) file for details.

## Acknowledgments

* [National Institutes of Health](https://www.nih.gov/) - [Big Data to Knowledge Initiative](https://datascience.nih.gov/bd2k)
  * Grants: R01MD010362, 1UG1DA04030901, 1U54EB020404, 1R01CA190329, 1R01DE02524, R00MD010468, 3UH2DA041713, 10555SC
* [National Science Foundation](https://www.nsf.gov/)
  * Grants: 1640813, 1722646
* [Intelligence Advanced Research Projects Activity](https://www.iarpa.gov/)
  * Contract: 2017-17042800006