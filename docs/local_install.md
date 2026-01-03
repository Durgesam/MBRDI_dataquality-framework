# Install on local environment

This guide handles how to setup the DQF in a local environment.
At the moment, there is only one guide for the setup on a Windows machine since this requires some special steps and is probably the use case used the most.
Documentations for MacOS and Linux are still pending, but it should be much easier and work right out-of-the-box.

## Example setup using pip and virtualenv on Windows

This is a guide for the installation of the DQF on a Windows machine inside the Daimler company network.
Unfortunately, we need a little bit more effort on Windows to get everything working as desired.
The following is a step-by-step guide which is working for the DQF developers. 
If you have any advice or ideas how to improve the installation, you are welcome to let us know.
As far as we know, many of the following steps are only required to work with Delta Parquet file format.
However, we are using them a lot in the DQF, so we designed this guide such that you can run the DQF with all of its functionalities.

This guide uses the brackets `<` and `>` to denote a placeholder which you need to replace with your own values.
As an example, `<ENV_DIR>` denotes the directory of your virtual Python environment and should be replaced by the actual path, e.g., `C:\Projects\DQF\venv\`.

### 1. Create and set up you virtual environment
Create a fresh virtual environment (using Python 3.8 in this example, but other versions should run fine as well).
Of course you can also re-use an existing environment.
These steps should not be Windows-specific and be the same as for other operating system.

```PowerShell
virtualenv <ENV_DIR> -p=python3.8
 ```
 
 Activate the new virtual environment
 
```PowerShell
<ENV_DIR>\Scripts\activate
```

Install the DQF using your personal token (which has read packages permission)
 
```PowerShell
pip install data-quality-framework-dev --index-url https://<YOUR_TOKEN>@pkgs.dev.azure.com/daimler/_packaging/DataQualityFramework%40Local/pypi/simple/
```

To run the DQF, we need a DQF folder, independent of the operating system.
Create one `<DQF_FOLDER>` folder with a `check_profiles`, `check_results` and `config` subfolder.
Create a `dqf_config.json` inside the `config` folder with the following content:
```JSON
{
    "check_profile_path": "<DQF_FOLDER>\\check_profiles",
    "result_output_path": "<DQF_FOLDER>\\check_results",
    "result_format": "delta"
}
```
Now create an environment variable `DQF_INSTALL_PATH=<DQF_FOLDER>`.

### 2. Windows-specific installation steps
On the local environment, we need some more configuration steps for everything to work properly.
Unfortunately, the official documentation (https://docs.delta.io/latest/quick-start.html#python) does not work properly in our company network and lacks some required information, hence we provide he following steps.

#### 2.1 Delta-spark library
First, we need to install the `delta-spark` library:

```PowerShell
pip install delta-spark
```

#### 2.2 Missing binaries
When running spark now, you will notice an error message stating that `winutils.exe` could not be found.
What spark is not telling us is that we also need a `hadoop.dll` file, so let's download them from https://github.com/cdarlint/winutils.
As you can see, there are different versions available.
To find our which version you need, run `pip list` and check the version of your `pyspark` library.
While writing this guide, the most recent version is `3.1.2`, so we download the files from the respective directory.

Create a folder for your hadoop binaries, e.g. `C:\Files\hadoop\v3.1.2`.
We will refer to this hadoop folder as `<HADOOP_DIR>` from now on.
Create a `bin` folder in `<HADOOP_DIR>` and place `winutils.exe` and `hadoop.dll` in there.

#### 2.3 Create more environment variables

Additionally, on Windows we need to create the following environment variables:
- `HADOOP_HOME=<HADOOP_DIR>`
- `SPARK_HOME=<ENV_DIR>\Lib\site-packages\pyspark`
- `PYSPARK_PYTHON=<ENV_DIR>\Scripts\python.exe`

Please note that `python.exe` might also be located inside `<ENV_DIR>` when using python environments created with Conda instaed of `virtualenv`.

It is up to you to decide where to store these environment variables.
Personally, I added them to the Run Configuration of my DQF project in PyCharm.

Now we also need to add the `<HADOOP_DIR>\bin` folder to the `PATH` variable.
You can simply add it to the Windows PATH variable, but keep in mind that this will then be a valid environment variable for all of your project.

#### 2.4 Missing \*.jar files

When running spark with Delta files now, it will complain about missing `.jar` files.
For this, first create a `jars` folder in your project's working directory.
Now eight `jar` files need to be downloaded into the `jars` folder and renamed.
To simplify this process, you can run the following PowerShell script in your working directory:
```PowerShell
mkdir -Force "jars"
wget "https://repo1.maven.org/maven2/com/ibm/icu/icu4j/58.2/icu4j-58.2.jar" -OutFile "jars/com.ibm.icu_icu4j-58.2.jar"
wget "https://repo1.maven.org/maven2/io/delta/delta-core_2.12/1.0.0/delta-core_2.12-1.0.0.jar" -OutFile "jars/io.delta_delta-core_2.12-1.0.0.jar"
wget "https://repo1.maven.org/maven2/org/abego/treelayout/org.abego.treelayout.core/1.0.3/org.abego.treelayout.core-1.0.3.jar" -OutFile "jars/org.abego.treelayout_org.abego.treelayout.core-1.0.3.jar"
wget "https://repo1.maven.org/maven2/org/antlr/antlr4/4.7/antlr4-4.7.jar" -OutFile "jars/org.antlr_antlr4-4.7.jar"
wget "https://repo1.maven.org/maven2/org/antlr/antlr4-runtime/4.7/antlr4-runtime-4.7.jar" -OutFile "jars/org.antlr_antlr4-runtime-4.7.jar"
wget "https://repo1.maven.org/maven2/org/antlr/antlr-runtime/3.5.2/antlr-runtime-3.5.2.jar" -OutFile "jars/org.antlr_antlr-runtime-3.5.2.jar"
wget "https://repo1.maven.org/maven2/org/antlr/ST4/4.0.8/ST4-4.0.8.jar" -OutFile "jars/org.antlr_ST4-4.0.8.jar"
wget "https://repo1.maven.org/maven2/org/glassfish/javax.json/1.0.4/javax.json-1.0.4.jar" -OutFile "jars/org.glassfish_javax.json-1.0.4.jar"
```

If you are wondering why you need to download specifically these files:
We found this out by installing everythin with the official delta spark documentation on a machine inside the company VPN with deactivated Netskope client.
Those are exactly the files which were downloaded by spark automatically. 
Unfortunately, this automatic download does only work with deactivated Netskop client in the VPN and does not work at all on machines inside the company network.
This is also the reason why we diverge from the official documentation for setting Delta up.

### 3 Summary and verification of successful setup

Now we are ready to test whether everything is working.
First, a small overview of what we did:
- Setting up virtual environment and installing DQF
- Setting up a DQF folder with a `dqf_config.json` and a `DQF_INSTALL_PATH` environment variable
- Installing `delta-spark`, `winutils.exe` and `hadoop.dll`
- Initializing environment variables `HADOOP_DIR`, `SPARK_HOME`, `PYSPARK_PYTHON` and modifying `PATH
- Downloading `jar` files

You can use the following python script to test whether you canwrite delta parquet files with spark now:
```Python
from pathlib import Path
import pyspark

# Spark builder and spark session creation
builder = pyspark.sql.SparkSession.builder.appName("MyApp")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
builder = builder.config("spark.jars", ",".join(str(jar.absolute()) for jar in Path("jars").glob("*.jar")))
spark = builder.getOrCreate()

# Create and save sample dataset
df = spark.createDataFrame([(0, "a"), (1, "b")])
df.write.format("delta").mode("overwrite").save("data")
```


### 4 Trouble shooting

If you encounter any problems, please check the following list of contact us.

#### 4.1 Compatibility of libraries and binaries
The guide was written using the following python library version:
| Library                       | Version       |
| ----------------------------- | ------------- |
| data-quality-framework-dev    | 0.2.353       |
| DBUtils                       | 2.0.1         |
| delta-spark                   | 1.0.0         |
| findspark                     | 1.4.2         |
| py4j                          | 0.10.9        |
| pyspark                       | 3.1.2         |

Make sure that the versions of `winutils.exe` and `hadoop.dll` match the version of `pyspark`.
Also make sure, that the `jar` files have the correct version as given above.

#### 4.2 Start with a simpler setup
Use the following code (or parts of it) which does not require Delta extensions of spark and check if this is running.
```Python
import pyspark
spark = pyspark.sql.SparkSession.builder.appName("MyApp").getOrCreate()
df = spark.createDataFrame([(0, "a"), (1, "b")])
df.write.format("csv").mode("overwrite").save("data")
```

#### 4.3 Try installing `jar` files from outside the company network
If you are outside the company network, for example when working remotely or by using a hotspot on your mobile phone, you can disable the Netskope client and can skip installing the `jar` files manually.
Spark should download them automatically when using the following command:
```Python
from delta import configure_spark_with_delta_pip
from pathlib import Path
import pyspark

# Spark builder and spark session creation
builder = pyspark.sql.SparkSession.builder.appName("MyApp")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Create and save sample dataset
df = spark.createDataFrame([(0, "a"), (1, "b")])
df.write.format("delta").mode("overwrite").save("data")
```
If this was sucessful, take a look into the `.ivy` directory in your user directory.
There should be a `jars` folder containing the required files with the correct versions now.
This, by the way, is effectively how we decided which `jar` files and versions are required.
