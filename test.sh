#!/bin/bash

# Script to process SQL files and generate YAML output
# Usage: ./process_files.sh [directory_with_sql_files]

# Set default directory to current directory if no argument provided
DIRECTORY=${1:-.}
OUTPUT_FILE="my.yaml"

# Function to extract first word from SQL content
get_first_sql_word() {
    local file="$1"
    # Read file, remove comments and empty lines, get first word
    first_word=$(grep -v '^--' "$file" | grep -v '^$' | head -1 | awk '{print tolower($1)}')
    echo "$first_word"
}

# Function to get first part of filename (before first dot or full name if no dot)
get_filename_prefix() {
    local filepath="$1"
    local filename=$(basename "$filepath")
    # Get everything before the first dot, or full name if no dot
    echo "${filename%%.*}"
}

# Initialize output file (create new or overwrite existing)
> "$OUTPUT_FILE"

# Process all SQL files in the specified directory
find "$DIRECTORY" -name "*.sql" -type f | while read -r sqlfile; do
    echo "Processing: $sqlfile"

    # Get first SQL word
    first_word=$(get_first_sql_word "$sqlfile")

    # Get filename prefix
    filename_prefix=$(get_filename_prefix "$sqlfile")

    # Create ID by combining first word and filename prefix
    id="${first_word}_${filename_prefix}"

    # Read SQL content
    sql_content=$(cat "$sqlfile")

    # Append to YAML file
    cat >> "$OUTPUT_FILE" << EOF
- id: $id
  sql: |-
    $sql_content
  meta:
    db: fin

EOF

    echo "Added entry with id: $id"
done

echo "Processing complete. Output written to $OUTPUT_FILE"
