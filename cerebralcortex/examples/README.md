# CerebralCortex-kernal (Examples)
This directory contains some of the following examples on how to get/save data streams and perform basic operations:

* Window stream data into 1 minute chunks

## Dependencies
* [Python3.6](https://www.python.org/downloads/release/python-360/)
* spark 2.4 - `sudo pip3 install pyspark`
* MySQL > 5.7

## Installation

* Download CerebralCortex-Kernal on your computer from githug

`wget https://github.com/MD2Korg/CerebralCortex-Kernel/archive/3.0.zip`

`unzip 3.0.zip && rm 3.0.zip`

`cd CerebralCortex-Kernel-3.0`

```sudo python3 setup.py install```

**Note:** If you don't want to install it then simply export the path of the CerebralCortex-Kernel directory:
 
 `wget https://github.com/MD2Korg/CerebralCortex-Kernel/archive/3.0.zip`
 
 `unzip 3.0.zip && rm 3.0.zip`
 
 `export PYTHONPATH="${PYTHONPATH}:PATH-OF-MAIN-DIR/CerebralCortex-Kernel-3.0` (This is defined in `run.sh`)
 
## Configure CerebralCortex-Kernal
To run these examples you just need to update configuration in the following config file:

`CerebralCortex-Kernel-3.0/conf/cerebralcortex.yml`

* Update filesystem storage path (i.e., `filesystem: filesystem_path`)
* Update MySQL settings in config file. 

## How to run the example code?
* **Import MySQL Database:**
    - `cd CerebralCortex-Kernel-3.0/cerebralcortex/examples/db`
    - `mysql -u MySQL-USERNAME -pMySQL-PASSWORD < cerebralcortex.sql `

* **Run example**    
    - `cd CerebralCortex-Kernel-3.0/cerebralcortex/examples`
    - `sh run.sh`

If everything works well then example code will produce similar output on console as below:

``` 
 ********** STREAM VERSION **********
stream-version: 1


 ********** STREAM DATA **********
User-ID: 00000000-afb8-476e-9872-6472b4e66b68 Start-time: 2019-01-09 11:35:00 End-time: 2019-01-09 11:36:00 Average-battery-levels: 100.0
User-ID: 00000000-afb8-476e-9872-6472b4e66b68 Start-time: 2019-01-09 11:41:00 End-time: 2019-01-09 11:42:00 Average-battery-levels: 96.58333333333333
User-ID: 00000000-afb8-476e-9872-6472b4e66b68 Start-time: 2019-01-09 11:43:00 End-time: 2019-01-09 11:44:00 Average-battery-levels: 95.23333333333333
User-ID: 00000000-afb8-476e-9872-6472b4e66b68 Start-time: 2019-01-09 11:38:00 End-time: 2019-01-09 11:39:00 Average-battery-levels: 98.28333333333333
User-ID: 00000000-afb8-476e-9872-6472b4e66b68 Start-time: 2019-01-09 11:39:00 End-time: 2019-01-09 11:40:00 Average-battery-levels: 97.93333333333334


 ********** STORING NEW STREAM DATA **********
BATTERY--org.md2k.phonesensor--PHONE-windowed-data has been stored.
```