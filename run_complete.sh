#!/bin/bash

# CLASSIFICATION
./run_classification_assemble.sh 10  2
./run_classification_clean.sh 20  2
./run_classification_clean.sh 50  2
./run_classification_clean.sh 100  2
./run_classification_clean.sh 150  2

./run_classification_clean.sh 11  2
./run_classification_clean.sh 12  2

# IDENTIFICATION
./run_identification_assemble.sh 11 small 2
./run_identification_clean.sh 11 middle 2
./run_identification_clean.sh 11 large 2

./run_identification_clean.sh 12 small 2
./run_identification_clean.sh 12 middle 2
./run_identification_clean.sh 12 large 2
