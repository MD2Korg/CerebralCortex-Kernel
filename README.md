# CerebralCortex Kernel
Cerebral Cortex is the big data cloud companion of mCerebrum designed to support population-scale data analysis, visualization, model development, and intervention design for mobile sensor data.

You can find more information about MD2K software on our [software website](https://md2k.org/software) or the MD2K organization on our [MD2K website](https://md2k.org/).

CerebralCortex Kernel is part of our [CerebralCortex cloud platform](https://github.com/MD2Korg/CerebralCortex). CerebralCortex Kernel is mainly responsible to store/retrieve mobile sensor data along with it's metadata. 

**Note**:

We have renamed following repositories.

* CerebralCortex-Platform -> CerebralCortex
* CerebralCortex - >  CerebralCortex-Kernel

## Examples
- [How to use CerebralCortex-Kernel API](https://github.com/MD2Korg/CerebralCortex-kernel-Examples)

## Documentation

- [Source code documentation](https://cerebralcortex-kernel.readthedocs.io/en/latest/)

## Installation
CerebralCortex-Kernel is a part of CerebralCortex cloud platform. To test the complete cloud platform, please visit [CerebralCortex](https://github.com/MD2Korg/CerebralCortex).

CerebralCortex-Kernel requires minimum [Python3.6](https://www.python.org/downloads/release/python-360/). To install CerebralCortex-Kernel as an API:

```pip3 install cerebralcortex-kernel```

- Note: please use appropriate pip (e.g., pip, pip3, pip3.6 etc.) installed on your machine 

### Dependencies

-  `Python3.6`

   -  Note: Python3.7 is not compatible with some of the requirements
   -  Make sure pip version matches Python version
   

## FAQ
**1 - Do I need whole CerebralCortex cloud platform to use CerebralCortex-Kernal?**

No! If you want to use CerebralCortex-Kernel independently then you would need: 
* Backend storage (FileSystem/HDFS and MySQL) with some data. Here is [some sample data](TODO) to play with.
* Setup the [configurations](https://github.com/MD2Korg/CerebralCortex-Kernel/tree/master/conf)
* Use the [examples](TODO) to start exploring data


**2 - How can I change NoSQL storage backend?**

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