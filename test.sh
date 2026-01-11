#!/bin/bash

echo "Processing .pl files in current directory..."
echo "----------------------------------------"

processed=0
for file in *.pl; do
    # Check if file exists
    if [ ! -f "$file" ]; then
        continue
    fi
    
    echo "Processing: $file"
    
    # Remove variable assignment wrapper
    sed -i '
        1s/^[[:space:]]*\$[a-zA-Z_][a-zA-Z0-9_]*[[:space:]]*=[[:space:]]*"//
        $s/";[[:space:]]*$//
    ' "$file"
    
    ((processed++))
done

if [ $processed -eq 0 ]; then
    echo "No .pl files found"
else
    echo "----------------------------------------"
    echo "Processed $processed files"
fi
