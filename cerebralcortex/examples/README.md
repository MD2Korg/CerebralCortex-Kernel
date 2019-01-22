# CerebralCortex-kernal (Examples)
This directory contains some of the following examples on how to get/save data streams and perform basic operations:

* Window stream data into 1 minute chunks

## Dependencies
* [Python3.6](https://www.python.org/downloads/release/python-360/)
* [spark 2.4](https://spark.apache.org/downloads.html)
* MySQL > 5.7

## Installation

Minimum requirements:

* Download CerebralCortex-Kernal on your computer from githug

`wget https://github.com/MD2Korg/CerebralCortex-Kernel/archive/3.0.zip`

`unzip 3.0.zip && rm 3.0.zip`

`cd CerebralCortex-Kernel-3.0`

```sudo python3 setup.py install```

**Note:** If you don't want to install it then simply export the path of the CerebralCortex-Kernel directory:
 
 `wget https://github.com/MD2Korg/CerebralCortex-Kernel/archive/3.0.zip`
 
 `unzip 3.0.zip && rm 3.0.zip`
 
 `export PYTHONPATH="${PYTHONPATH}:PATH-OF-MAIN-DIR/CerebralCortex-Kernel-3.0`
 
### Configure CerebralCortex-Kernal
To run these examples you just need to update configuration in the following config file:

`CerebralCortex-Kernel-3.0/conf/cerebralcortex.yml`

* Update filesystem storage path (i.e., `filesystem: filesystem_path`)
* Update MySQL settings in config file. 

#### How to run the example code?
* **Import MySQL Database:**
    - `cd CerebralCortex-Kernel-3.0/cerebralcortex/examples/db`
    - `mysql -u MySQL-USERNAME -pMySQL-PASSWORD < cerebralcortex.sql `

* **Run example**    
    - `cd CerebralCortex-Kernel-3.0/cerebralcortex/examples`
    - `sh run.sh`

If everything works well then example code will produce following console logs:
