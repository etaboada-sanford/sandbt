[![Developer: Edgar](https://img.shields.io/badge/developer-edgar-blue)](mailto:etaboada@sanford.co.nz)

# About the Project

This is Sanford's D365 Finance and Operations DBT project.

## Pre-requisites

To begin working with this project, please ensure that the following tools are available and pre-requisite setup is done:

1. [Visual Studio Code](https://code.visualstudio.com/download)
2. [Git Bash](https://git-scm.com/download/win) - if working on Windows machine, this is to run shell scripts 
3. [Python3](https://www.python.org/downloads/release/python-3120/)
4. [Microsoft C++ Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/)
5. [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli-windows?tabs=azure-cli)


1. In your VS Code workspace, clone the following repository:
```bash
git clone https://sanfordltd@dev.azure.com/sanfordltd/Data%20and%20Analytics/_git/sandbt
```

2. In your VS Code terminal, launch Git Bash and make sure you are in the project directory. If not, then change to the project directory directory. You should see a (main) prompt indicating that you are in the main branch of the sandbtfabric-d365 project:
```bash
$ cd /c/code/sandbt
MINGW64 /c/code/sandbt (main)
```

3. Setup your development environment variables by executing the following script:
```bash
MINGW64 /c/code/sandbt (main)
$ ./developer_setup.sh [optional: supply the python executable command e.g. py or python (default is py)]
```
This script will create your python virtual environment in the .venv folder. A virtual python environment ensures that you have an isolated and controlled environment separate from your local python installation. This controls that you only develop on managed python packages and versions that are expected to be deployed in test/production environment. You need to be working in this virtual environment everytime you do coding. These packages are configured in the requirements.txt file.

4. Activate the virtual environment. When activated, the shell will have a (.venv) prompt displayed
```bash
$ source .venv/Scripts/activate
```

## Getting started

The following are the steps to get started working with this project. If you want to build the whole D365 dbt fabric project from scratch, inside the fabric_d365 folder, please run the following script:

```bash
(.venv)
MINGW64 /c/code/sandbt/fabric_d365 (main)
$ dbt deps
```

For you to be able to connect to the Fabric lakehouse for development, please login with your credentials using the command:

```bash
(.venv)
MINGW64 /c/code/sandbt/fabric_d365 (main)
$ az login
```

Once authenticated, you can now start to initialize the project by running the command:

```bash
(.venv)
MINGW64 /c/code/sandbt/fabric_d365 (main)
$ ./scripts/init.sh
```

The script will do the following:

1. Read the d365_tables.yml file which contains all the D365 tables required for reporting
2. Connect to the Dataverse lakehouse to query the columns of the table as provided in the the d365_tables.yml
3. Build the dbt sources.yml file and store it in ./models/dw
