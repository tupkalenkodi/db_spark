#!/bin/bash

# Script to count graphs with specific constraints using nauty
# Usage: ./count_graphs.sh <n>

set -e  # Exit on error

# Check if n is provided
if [ $# -ne 1 ]; then
    echo "Usage: $0 <n>"
    echo "Example: $0 18"
    exit 1
fi

n=$1

# Calculate parameters
max_edges=$(( (n * (n + 2)) / 8 ))
max_degree=$(( n / 2 ))

echo "========================================="
echo "Counting graphs with:"
echo "  n = $n vertices"
echo "  edges = $max_edges (exactly)"
echo "  max degree ≤ $max_degree"
echo "  min degree ≥ 1"
echo "  connected"
echo "========================================="

# Number of parallel jobs (adjust based on CPU cores)
cores=7
echo "Using $cores parallel jobs..."

# Create temporary directory for output
temp_dir="temp_graphs_${n}"
mkdir -p "$temp_dir"
cd "$temp_dir"

# Function to clean up on exit
cleanup() {
    echo "Cleaning up..."
    cd ..
    rm -rf "$temp_dir"
}
trap cleanup EXIT

# Run parallel jobs
echo "Starting parallel generation..."
start_time=$(date +%s)

for i in $(seq 0 $((cores-1))); do
    echo "  Starting job $i/$cores..."
    nauty-geng "$n" -c "${max_edges}:${max_edges}" -d1 "-D${max_degree}" -lu "$i/$cores" > "log_${i}.txt" 2>&1 &
done

# Wait for all jobs to complete
wait

# Check for errors in the output
errors=$(grep -l "error\|Error\|ERROR\|geng: must have" log_*.txt 2>/dev/null || true)
if [ -n "$errors" ]; then
    echo "ERROR: Some jobs failed. Check the log files in $temp_dir"
    echo "Error files: $errors"
    exit 1
fi

# Calculate total from logs - FIXED: Use $2 instead of $1
total_count=$(grep "graphs generated" log_*.txt 2>/dev/null | awk '{sum += $2} END {print sum+0}')

end_time=$(date +%s)
duration=$((end_time - start_time))

echo "========================================="
echo "RESULTS:"
echo " TOTAL: $(printf "%'d" $total_count)"
echo " Time: ${duration}s"
echo "========================================="

# Save result to file
result_file="../graph_count_${n}.txt"
echo "n=$n, edges=$max_edges, max_degree=$max_degree, count=$total_count" > "$result_file"
echo "Results saved to: $result_file"
