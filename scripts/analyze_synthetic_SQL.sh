#!/bin/bash

# Define usage function
usage() {
  echo "Usage: $0 --synthesis_method [synthesis_method] [--run_synthesize] [--db_name db_name] [--num_queries num] [--write_to_csv] [--random_choice] [--config_name config] [--refinement_k k]"
  exit 1
}

# Parse command line arguments
RUN_SYNTHESIZE=false
DB_NAME=""
NUM_QUERIES=60
WRITE_TO_CSV=false
RANDOM_CHOICE=true
CONFIG_NAME="config_file.json"
SYNTHESIS_METHOD="schema_guided"
REFINEMENT_K=3
SPEC_FILE=""
while [[ "$#" -gt 0 ]]; do
  case $1 in
    --db_name)
      DB_NAME="$2"
      echo "Database Name set to: $DB_NAME"
      shift 2
      ;;
    --num_queries)
      NUM_QUERIES="$2"
      echo "Number of Queries set to: $NUM_QUERIES"
      shift 2
      ;;
    --write_to_csv)
      WRITE_TO_CSV="true"
      echo "Write to CSV enabled."
      shift 1
      ;;
    --random_choice)
      RANDOM_CHOICE="$2"
      echo "Random choice set to: $RANDOM_CHOICE"
      shift 2
      ;;
    --config_name)
      CONFIG_NAME="$2"
      echo "Config name set to: $CONFIG_NAME"
      shift 2
      ;;
    --refinement_k)
      REFINEMENT_K="$2"
      echo "Refinement K set to: $REFINEMENT_K"
      shift 2
      ;;
    --synthesis_method)
      SYNTHESIS_METHOD="$2"
      echo "Synthesis Method set to: $SYNTHESIS_METHOD"
      shift 2
      ;;
    --run_synthesis)
      RUN_SYNTHESIZE=true
      echo "Synthesis mode enabled."
      shift 1
      ;;
    *)
      usage
      ;;
  esac
done
# echo "Running analysis for synthesis method: $SYNTHESIS_METHOD"
# Check if synthesis method is provided
if [ -z "$SYNTHESIS_METHOD" ]; then
  usage 
fi
# Set folder paths based on environment
if [ -d "data/synthetic-queries" ]; then
  FOLDER_PATH="data/synthetic-queries/$SYNTHESIS_METHOD"
elif [ -d "data/synthetic-queries" ]; then
  FOLDER_PATH="=data/synthetic-queries/$SYNTHESIS_METHOD"
else
  echo "Data folder not found."
  exit 1
fi

# Set the output folder
OUTPUT_FOLDER="outputs/experiments/SyntheticBenchmarkEvaluation/$SYNTHESIS_METHOD"
mkdir -p "$OUTPUT_FOLDER"

# Set default values if not provided
DB_NAME=${DB_NAME:-"All Databases"}
NUM_QUERIES=${NUM_QUERIES:-60}
WRITE_TO_CSV=${WRITE_TO_CSV:-false}
RANDOM_CHOICE=${RANDOM_CHOICE:-true}
CONFIG_NAME=${CONFIG_NAME:-"config_file.json"}
REFINEMENT_K=${REFINEMENT_K:-3}


# If synthesize flag is set, run query generation script
# if [ "$RUN_SYNTHESIZE" = true ]; then
#   echo "Running query generation for synthesis method: $SYNTHESIS_METHOD"
#   python3 query_generation/query_generator_from_specifications.py --synthesis_method "$SYNTHESIS_METHOD" --db_name "$DB_NAME" --num "$NUM_QUERIES" --write_to_csv "$WRITE_TO_CSV" --random_choice "$RANDOM_CHOICE" --config_name "$CONFIG_NAME" --refinement_k "$REFINEMENT_K"
#   if [ $? -ne 0 ]; then
#     echo "Query generation script execution failed."
#     exit 1
#   else
#     echo "Query generation script executed successfully."
#   fi
# fi
# Run Python script with the correct method argument
python3 analysis/synthetic_statistics.py --synthesis_method "$SYNTHESIS_METHOD" --output_file "$OUTPUT_FOLDER/overview.txt" 
echo "HIII"

# # # Check if script execution was successful
# # if [ $? -ne 0 ]; then
# #   echo "Python script execution failed."
# #   exit 1
# # else
# #   echo "Python script executed successfully. Results are saved in $OUTPUT_FOLDER"
# # fi

# # # Provide a summary of the results
# # if [ -f "$OUTPUT_FOLDER/overview.txt" ]; then
# #   echo "Summary of the results:"
# #   cat "$OUTPUT_FOLDER/overview.txt"
# # else
# #   echo "Overview file not found in the output folder."
# # fi
