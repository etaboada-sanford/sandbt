[![Developer: Edgar](https://img.shields.io/badge/developer-edgar-blue)](mailto:etaboada@sanford.co.nz)

# About the Project

This is Sanford's D365 Finance and Operations DBT project.

## Pre-requisites

To begin working with this project, please ensure that the following tools are available and pre-requisite setup is done:

1. [Visual Studio Code](https://code.visualstudio.com/download)
2. [Git Bash](https://git-scm.com/download/win) - if working on Windows machine, this is to run shell scripts 
3. [Python3](https://www.python.org/downloads/release/python-3120/)
4. [Microsoft C++ Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/)


1. In your VS Code workspace, clone the following repository:
```bash
git clone https://sanfordltd@dev.azure.com/sanfordltd/Data%20and%20Analytics/_git/sandbtfabric-d365
```

2. In your VS Code terminal, launch Git Bash and make sure you are in the project directory. If not, then change to the project directory directory. You should see a (main) prompt indicating that you are in the main branch of the sandbtfabric-d365 project:
```bash
$ cd /c/code/sandbtfabric-d365
MINGW64 /c/code/sandbtfabric-d365 (main)
```

3. Setup your development environment variables by executing the following script:
```bash
MINGW64 /c/code/sandbtfabric-d365 (main)
$ ./developer_setup.sh
```
This script will create your python virtual environment in the .venv folder. A virtual python environment ensures that you have an isolated and controlled environment separate from your local python installation. This controls that you only develop on managed python packages and versions that are expected to be deployed in test/production environment. You need to be working in this virtual environment everytime you do coding. These packages are configured in the requirements.txt file.

4. Activate the virtual environment. When activated, the shell will have a (.venv) prompt displayed
```bash
$ source .venv/Scripts/activate
```

## Getting started

The following are the steps to get started working with this project. If you want to build the whole dbt project from scratch, please run the folloing script:

```bash
dbt deps
./scripts/init.sh
```

The script will do the following:

1. Setup elementary-data tables and views (for monitoring and lineage tracing)
2. Prepare the ancillary tables (_cdmmetadata views and d365_lsn_watermark table)
3. Generate the D365 ChangeFeed source YAML's and create external tables in the LOAD schema (generated file is in ~/models/load/sources.yml)
4. Generate the D365 Table source YAML's and create external tables in the ODS schema (generated file is in ~/models/ods/sources.yml)
5. Generate the D365 base model SQL files for STAGE schema (generated SQL files are in ~/models/stage/src_export_to_datalake/base_d365_{Table Name}.sql)
6. Load the seed files (located in ~/seeds)
7. Perform initial bulk loading of base tables querying the external tables from the ODS schema
8. Perform first incremental loading of base tables querying the external tables from the LOAD schema
