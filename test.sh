#!/bin/bash

# Check if both parameters are provided
if [ $# -ne 2 ]; then
    echo "Usage: $0 <oldtext> <newtext>"
    echo "Example: $0 'old_table' 'new_table'"
    exit 1
fi

OLDTEXT="$1"
NEWTEXT="$2"

echo "Replacing '$OLDTEXT' with '$NEWTEXT' in .pl files..."
echo "----------------------------------------"

processed=0
for file in *.pl; do
    # Check if file exists
    if [ ! -f "$file" ]; then
        continue
    fi
    
    # Check if file contains the old text
    if grep -q "$OLDTEXT" "$file"; then
        echo "Processing: $file"
        sed -i "s/$OLDTEXT/$NEWTEXT/g" "$file"
        ((processed++))
    fi
done

if [ $processed -eq 0 ]; then
    echo "No .pl files found containing '$OLDTEXT'"
else
    echo "----------------------------------------"
    echo "Processed $processed files"
fi
