#!/bin/bash

# Define default values for arguments
DB_NAME=""
NUM_QUERIES=60
WRITE_TO_CSV=false
RANDOM_CHOICE=true
CONFIG_NAME="config_file.json"
SYNTHESIS_METHOD="schema_guided"
REFINEMENT_K=3
SPEC="{}"
VENV_PATH="venv"  # Path to your virtual environment
REQUIREMENTS_FILE="requirements.txt"  # Path to your requirements file
SETUP_FILE="setup.py"  # Path to your setup.py file

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
  case $1 in
    --db_name)
      DB_NAME="$2"
      shift
      shift
      ;;
    --num)
      NUM_QUERIES="$2"
      shift
      shift
      ;;
    --write_to_csv)
      WRITE_TO_CSV=true
      shift
      ;;
    --random_choice)
      RANDOM_CHOICE=true
      shift
      ;;
    --config_name)
      CONFIG_NAME="$2"
      shift
      shift
      ;;
    --synthesis_method)
      SYNTHESIS_METHOD="$2"
      shift
      shift
      ;;
    --refinement_k)
      REFINEMENT_K="$2"
      shift
      shift
      ;;
    --spec)
      SPEC="$2"
      shift
      shift
      ;;
    *)
      echo "Unknown option $1"
      exit 1
      ;;
  esac
done

# Detect the operating system
OS="$(uname -s)"
case $OS in
  Linux*)     MACHINE="Linux";;
  Darwin*)    MACHINE="Mac";;
  CYGWIN*|MINGW*|MSYS*) MACHINE="Windows";;
  *)          MACHINE="Unknown";;
esac

# Create the virtual environment if it doesn't exist
if [ ! -d "$VENV_PATH" ]; then
  python3 -m venv "$VENV_PATH"
fi

# Activate the virtual environment based on the operating system
if [[ "$MACHINE" == "Windows" ]]; then
  source "$VENV_PATH/Scripts/activate"
else
  source "$VENV_PATH/bin/activate"
fi

echo "#################"

# Install dependencies from requirements.txt
if [ -f "$REQUIREMENTS_FILE" ]; then
  python3 -m pip install -r "$REQUIREMENTS_FILE"
else
  echo "requirements.txt not found. Skipping dependency installation."
fi

# Install the package using setup.py
if [ -f "$SETUP_FILE" ]; then
  python3 "$SETUP_FILE" install --user || exit 1
else
  echo "setup.py not found. Skipping package installation."
fi

echo "%%%%%%%%%%%%%%%%%%%%%%"
# Run the Python script with the parsed arguments
python3 -m sql_benchmark.query_generation.query_generator_from_specifications \
  --db_name "$DB_NAME" \
  --num "$NUM_QUERIES" \
  --write_to_csv "$WRITE_TO_CSV" \
  --random_choice "$RANDOM_CHOICE" \
  --config_name "$CONFIG_NAME" \
  --synthesis_method "$SYNTHESIS_METHOD" \
  --refinement_k "$REFINEMENT_K" \
  --spec "$SPEC"

# Deactivate the virtual environment
deactivate
