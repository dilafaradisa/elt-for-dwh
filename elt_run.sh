#!/bin/bash

echo "========== Start Orcestration Process =========="

# Virtual Environment Path
VENV_PATH="/Users/dilafaradisa/Documents/disa/05_pacmann/03_into_to_devops/storage/dataset-olist/olist_project/bin/activate"


# Activate Virtual Environment
source "$VENV_PATH"

# Set Python script
PYTHON_SCRIPT="/Users/dilafaradisa/Documents/disa/05_pacmann/03_into_to_devops/storage/dataset-olist/elt_main.py"

# Run Python Script 
python3 "$PYTHON_SCRIPT"


echo "========== End of Orcestration Process =========="