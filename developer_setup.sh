#!/bin/bash
export STARTED_DEVSETUP="STARTED  $(date)"
unset DBT_PROJECT
unset SCRIPTS
unset ENV
python_exe=${1:-"python"}
echo "Python executable: " $python_exe

SCRIPT_PATH=$(dirname "$(realpath "$0")")
echo "The full path of the script is: $SCRIPT_PATH"

if [ ! -f .venv/Scripts/activate ]; then
  echo "*** Creating virtual environment."
  $python_exe -m venv .venv
else
  echo "*** A virtual environment is already setup."
fi

# check if the file exists and is readable
if [ -f .venv/Scripts/activate ] && [ -r .venv/Scripts/activate ]; then
  # check if the file contains the string
  if ! grep -q "export DBT_PROJECT" .venv/Scripts/activate; then
    # execute the command
    echo export DBT_PROJECT=\"$(pwd)/dags/fabric_d365\" >> .venv/Scripts/activate
    echo "- DBT_PROJECT environment variable initialized in activate script"
  else
    echo "- DBT_PROJECT environment variable is already initialized in activate script"
  fi

  if ! grep -q "export SCRIPTS" .venv/Scripts/activate; then
    # execute the command
    echo export SCRIPTS=\"$(pwd)/dags/fabric_d365/scripts\" >> .venv/Scripts/activate
    echo "- SCRIPTS environment variable initialized in activate script"
  else
    echo "- SCRIPTS environment variable is already initialized in activate script"
  fi

  if ! grep -q "export ENV" $(pwd)/.venv/Scripts/activate; then
    # execute the command
    echo export ENV=dev >> .venv/Scripts/activate
    echo "- ENV environment variable initialized in activate script"
  else
    echo "- ENV environment variable is already initialized in activate script"
  fi
    
else
  echo "!!! The file does not exist or is not readable."
  exit 1
fi

if [ -f /etc/bash.bashrc ] && [ -r /etc/bash.bashrc ] && [ -w /etc/bash.bashrc ]; then
  if ! grep -q "alias sandbt" /etc/bash.bashrc; then
    # execute the command
    echo "alias sandbt='source $(pwd)/.venv/Scripts/activate;cd $(pwd)/dags/fabric_d365'" >> /etc/bash.bashrc
    echo "- alias sandbt completed setup."
  else
    echo "- alias sandbt is already setup."
  fi
else
  echo "!!! The /etc/bash.bashrc does not exist or is not readable."
  exit 1
fi

if [ -f /etc/bash.bashrc ] && [ -r /etc/bash.bashrc ] && [ -w /etc/bash.bashrc ]; then
  if ! grep -q "sfix()" /etc/bash.bashrc; then
    # execute the command
    echo "sfix() { sqlfluff fix \"\$1\"; }" >> /etc/bash.bashrc
    echo "- sfix: sqlfluff fix function completed setup."
  else
    echo "- sfix is already setup."
  fi
else
  echo "!!! The /etc/bash.bashrc does not exist or is not readable."
  exit 1
fi

if [ -f /etc/bash.bashrc ] && [ -r /etc/bash.bashrc ] && [ -w /etc/bash.bashrc ]; then
  if ! grep -q "slint()" /etc/bash.bashrc; then
    # execute the command
    echo "slint() { sqlfluff lint \"\$1\"; }" >> /etc/bash.bashrc
    echo "- slint: sqlfluff lint function completed setup."
  else
    echo "- slint is already setup."
  fi
else
  echo "!!! The /etc/bash.bashrc does not exist or is not readable."
  exit 1
fi

alias sandbt='source .venv/Scripts/activate'

echo "*** Activating virtual environment."
source $(pwd)/.venv/Scripts/activate

echo "*** Checking environment variables. Make sure that they all have values..."
echo "- DBT_PROJECT = [$DBT_PROJECT]"
echo "- SCRIPTS = [$SCRIPTS]"
echo "- ENV = [$ENV]"

echo "*** Installing sandbt $python_exe packages"
$python_exe.exe -m pip install --upgrade pip
pip install -r dags/requirements.txt

echo "*** $STARTED_DEVSETUP ***"
echo "*** COMPLETED $(date) ***"
