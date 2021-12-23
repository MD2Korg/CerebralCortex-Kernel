# CerebralCortex Kernel
Cerebral Cortex is the big data cloud companion of mCerebrum designed to support population-scale 
data analysis, visualization, model development, and intervention design for mobile sensor data.

You can find more information about MD2K software on our 
[software website](https://md2k.org/software) or the MD2K organization on our 
[MD2K website](https://md2k.org/).

CerebralCortex Kernel is part of our 
[CerebralCortex cloud platform](https://github.com/MD2Korg/CerebralCortex).
CerebralCortex-Kernel is capable of parallelizing tasks and scale a job to n-number of cores/machines. CerebralCortex Kernel offers some builtin features as follows:

## Installation
### Dependencies
CerebralCortex Kernel requires `java 8` to run. Java 8 prior to version 8u92 support is deprecated as of CerebralCortex-Kernel 3.3.0.
- check java version - `java -version` 
- set ``JAVA_HOME`` to `java 8`
- OR start python shell with ``JAVA_HOME=/path/to/java/Home python3``


### Install using pip
CerebralCortex-Kernel requires minimum [Python3.6](https://www.python.org/downloads/release/python-360/). To install CerebralCortex-Kernel as an API:

`pip3 install cerebralcortex-kernel`

- Note: please use appropriate pip (e.g., pip, pip3, pip3.6 etc.) installed on your machine 

### Install from source code
- Clone repo - 
` git clone https://github.com/MD2Korg/CerebralCortex-Kernel.git`
- `cd CerebralCortex-Kernel`
- `python3 setup.py install`

## Usage

    from cerebralcortex.kernel import Kernel
    CC = Kernel(cc_configs="default")
    
    # to view default configs
    print(CC.config)
    
    # default data storage path is
    # /user/home/folder/cc_data


By default Kernel will load default configs. Please have a look at all available [configurations](https://github.com/MD2Korg/CerebralCortex-Kernel/tree/master/conf) for CerebralCortex-Kernel. 
You may also load config files as:

`CC = Kernel(configs_dir_path="dir/path/to/configs/", new_study=True)`

### How to use builtin algorithms
Using builtin algorithms are as easy as loading data, passing it to algorithm and get the results. 
Below is an example on how to compute CGM Glucose Variability Metrics.

- [Download Glucose Data](https://github.com/MD2Korg/CerebralCortex-Kernel/blob/master/cerebralcortex/test_suite/sample_data/cgm_glucose_variability_metrics/sample.csv). The device used to collect glucose was the [Dexcom G6 Continuous Glucose Monitor](https://www.dexcom.com/g6-cgm-system)
- Install Cerebral Cortex Kernel `pip install cerebralcortex-kernel`
- Open terminal and start python3 shell

### Python Code
    # import packages
    from cerebralcortex.kernel import Kernel
    from cerebralcortex.algorithms.glucose.glucose_variability_metrics import glucose_var
    
    # Create Kernel object
    CC = Kernel(cc_configs="default", new_study=True)
    
    # Read sample CSV data
    ds = CC.read_csv("/path/of/the/downloaded/file/sample.csv", stream_name="cgm_glucose_variability_metrics", header=True)
    
    # view sample data
    ds.show(2)
    
    # Apply glucose_variability_metrics algorithm on the data
    results = glucose_var(ds)
    
    # view results
    results.show(2)
    
    # save computed data 
    CC.save_stream(results)
     
Please have a look at [jupyter notebook](https://github.com/MD2Korg/CerebralCortex/blob/master/jupyter_demo/datastream_operation.ipynb) for basic operation that could be perform on DataStream object.

### Algorithms to  Analyze Sensor Data
External CerebralCortex-Kernel offers following builtin algorithms to analyze sensor data.

- ECG sensor data quality
- ECG RR Interval Computation
- Heart Rate Variability Feature Computation
- CGM Glucose Variability Metrics
- [GPS Data Clustering](https://github.com/MD2Korg/CerebralCortex/blob/master/jupyter_demo/cc_algorithms.ipynb)
- Sensor Data Interpolation
- Statistical Features Computation
- [List of all available algorithms](https://github.com/MD2Korg/CerebralCortex-Kernel/tree/master/cerebralcortex/algorithms)

### Markers with ML Models
- [Stress Detection using ECG data](https://github.com/MD2Korg/CerebralCortex-Kernel/tree/master/cerebralcortex/markers/ecg_stress)
- [mContain Social Crowding](https://github.com/MD2Korg/CerebralCortex-Kernel/tree/master/cerebralcortex/markers/mcontain)
- Brushing Detection using Accelerometer and Gyro Data (TODO)

### Visualization
- [Basic Plots for Timeseries Data](https://github.com/MD2Korg/CerebralCortex/blob/master/jupyter_demo/plotting_demo.ipynb)
- [Plot GPS Clusters on Map](https://github.com/MD2Korg/CerebralCortex/blob/master/jupyter_demo/cc_algorithms.ipynb)
- [Stress Visualization](https://github.com/MD2Korg/CerebralCortex/blob/master/jupyter_demo/plotting_demo.ipynb)

### Import and Document Data
- [Import CSV Data in CerebralCortex-Kernel Format](https://github.com/MD2Korg/CerebralCortex/blob/master/jupyter_demo/import_and_analyse_data.ipynb)
- [Document imported Data using MetaData Module](https://github.com/MD2Korg/CerebralCortex/blob/master/jupyter_demo/import_and_analyse_data.ipynb)

### External CerebralCortex-Kernel Supported Platforms
- mProv 
- mFlow 


## Examples
- [Google Colab Notebooks](https://github.com/MD2Korg/CerebralCortex-Kernel/tree/master/examples)

## Documentation

- [Source code documentation](https://cerebralcortex-kernel.readthedocs.io/en/latest/)

## Deploy on Cloud
CerebralCortex-Kernel is a part of CerebralCortex cloud platform. To test the complete cloud platform, please visit [CerebralCortex](https://github.com/MD2Korg/CerebralCortex).


## FAQ
**1 - Do I need whole CerebralCortex cloud platform to use CerebralCortex-Kernal?**

No! If you want to use CerebralCortex-Kernel independently.


**2 - How can I change NoSQL backend storage layer?**

CerebralCortex-Kernel follows component based structure. This makes it easier to add/remove features. 
* Add a new class in [Data manager-Raw](https://github.com/MD2Korg/CerebralCortex-Kernel/blob/master/cerebralcortex/core/data_manager/raw/). 
* New class must have read/write methods. Here is a sample [skeleton class](https://github.com/MD2Korg/CerebralCortex-Kernel/blob/master/cerebralcortex/core/data_manager/raw/storage_blueprint.py) with mandatory methods required in the new class.
* Create an object of new class in [Data-Raw](https://github.com/MD2Korg/CerebralCortex-Kernel/blob/master/cerebralcortex/core/data_manager/raw/data.py) with appropriate parameters.
* Add appropriate configurations in [cerebralcortex.yml](https://github.com/MD2Korg/CerebralCortex-Kernel/blob/master/conf/cerebralcortex.yml) in (NoSQL Storage)[https://github.com/MD2Korg/CerebralCortex-Kernel/blob/master/conf/cerebralcortex.yml#L8] section.

**3 - How can I replace MySQL with another SQL storage system?** 

* Add a new class in [Data manager-SQL](https://github.com/MD2Korg/CerebralCortex-Kernel/tree/master/cerebralcortex/core/data_manager/sql). 
* New class must implement all of the methods available in [stream_handler.py](https://github.com/MD2Korg/CerebralCortex-Kernel/blob/master/cerebralcortex/core/data_manager/sql/stream_handler.py) class.
* Create an object of new class in [Data-SQL](https://github.com/MD2Korg/CerebralCortex-Kernel/blob/master/cerebralcortex/core/data_manager/sql/data.py) with appropriate parameters.
* Add appropriate configurations in [cerebralcortex.yml](https://github.com/MD2Korg/CerebralCortex-Kernel/blob/master/conf/cerebralcortex.yml) in [Relational Storage](https://github.com/MD2Korg/CerebralCortex-Kernel/blob/master/conf/cerebralcortex.yml) section.

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
* [Chan Zuckerberg Initiative DAF, an advised fund of Silicon Valley Community Foundation](https://chanzuckerberg.com/eoss/proposals/expanding-the-open-mhealth-platform-to-support-digital-biomarker-discovery/)
  * Grant Number: 2020-218599