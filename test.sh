#!/bin/bash

echo "Removing -- comments from .pl files..."
echo "----------------------------------------"

processed=0
for file in *.pl; do
    # Check if file exists
    if [ ! -f "$file" ]; then
        continue
    fi
    
    echo "Processing: $file"
    sed -i 's/--.*$//' "$file"
    ((processed++))
done

if [ $processed -eq 0 ]; then
    echo "No .pl files found"
else
    echo "----------------------------------------"
    echo "Processed $processed files"
fi
