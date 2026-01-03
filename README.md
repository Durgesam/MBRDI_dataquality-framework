[![Build Status](https://daimler.visualstudio.com/CoCBigDataAI/_apis/build/status/cocbigdatard.DataQualityFramework?branchName=development)](https://daimler.visualstudio.com/CoCBigDataAI/_build/latest?definitionId=10870&branchName=development)

### Releases

[![data-quality-framework package in DataQualityFramework@Local feed in Azure Artifacts](https://daimler.feeds.visualstudio.com/_apis/public/Packaging/Feeds/25351fcd-14b4-4d9a-9538-d924aa9cfd96@ceda4267-3cc1-4851-8f06-0ac9964fd824/Packages/beefe78b-f46a-429e-b301-2cdf03f81744/Badge)](https://daimler.visualstudio.com/CoCBigDataAI/_packaging?_a=package&feed=25351fcd-14b4-4d9a-9538-d924aa9cfd96%40ceda4267-3cc1-4851-8f06-0ac9964fd824&package=beefe78b-f46a-429e-b301-2cdf03f81744&preferRelease=true)
Release-Version

[![data-quality-framework-dev package in DataQualityFramework feed in Azure Artifacts](https://feeds.dev.azure.com/daimler/_apis/public/Packaging/Feeds/25351fcd-14b4-4d9a-9538-d924aa9cfd96/Packages/bd0a988f-39e6-4764-914d-c7f3baed7060/Badge)](https://dev.azure.com/daimler/CoCBigDataAI/_packaging?_a=package&feed=25351fcd-14b4-4d9a-9538-d924aa9cfd96&package=bd0a988f-39e6-4764-914d-c7f3baed7060&preferRelease=true)
Development-Version

# Data Quality Framework

This framework supports you checking the quality of your data. 
Several checks are provided to build your own configurations of how you want to check your data quality.

Reporting functionalities will give you a better overview of how your data has performed on several runs.
***

## Installation
Data Quality Framework is available as easy to install python-wheel file package. Please find the different version on the top of the page

1. Install on a Databricks Cluster ([Guide](docs/cluster_install.md))
    

2. Install on a local environment ([Guide](docs/local_install.md))

***
## Features
- Perform ([Data Quality Checks](docs/dqf_quality_checks.md)) on..
    - Databricks Cluster
    - Local Environments with PySpark
- Create check results for Quality Dashboards in Delta or CSV format
- Basic visualization of your quality checks
- E-Mail Notifications (paused)
- Check **CSV, ORC, Parquet, Delta-Parquet** files (other formats planned)
- [Statistics Functions](docs/dqf_statistics.md) report for DQF overview and data insights
***

## Usage of Data Quality Framework
The usage of the Data Quality Framework is based on check-profiles which will be stored as json-files on your ADLS or some file-storage. You can look at below notebooks to get started with the DQF.

- [Running DQF with Config](docs/examples/Run_DQF_with_Config.ipynb)
- [Running Individual Checks Directly in the Notebook](docs/examples/Run_Checks_Directly.ipynb)
- [Different Ways of Initializing CheckProject](docs/examples/Different_Ways_of_Using_CheckProject.ipynb)
- [Data Source Load Options](docs/dqf_load_options.md)
- [DQF incremental load functionality](docs/dqf_incremental_load.md)
- [Accessing Failed Data after running Checks](docs/dqf_accessing_failed_data.md)
***
## Contributing
Please check out the [Contribution Guide](docs/contribution_guide.md).
***
## License
Please check the [LICENSE.md](LICENSE.md)
For copyright and license information regarding this repository see [Notice](NOTICE) and [License](LICENSE.md). We make use of Free and Open Source Software whose licenses are documented [here](LICENSES).

- [Apache pySpark](https://github.com/apache/spark/tree/master/python) - unified analytics engine for large-scale data processing
- [DBUtils](https://github.com/WebwareForPython/DBUtils) - Database connections for multi-threaded environments
- [findspark](https://github.com/minrk/findspark) - Provides findspark.init() to make pyspark importable as a regular library.
- [importlib-metadata](https://github.com/python/importlib_metadata) - Library to access the metadata for a Python package.
- [plotly](https://github.com/plotly/plotly.py) - The interactive graphing library for Python (includes Plotly Express)
- [tabulate](https://github.com/astanin/python-tabulate) - Pretty-print tabular data in Python



***
## Release Notes
...pending for production
