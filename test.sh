#!/bin/bash

OUTPUT_FILE="global.yaml"
INPUT_FILES=("my1.yaml" "my2.yaml" "my3.yaml")

# Create header
cat > "$OUTPUT_FILE" << EOF
# =============================================================================
# GLOBAL CONFIGURATION FILE  
# Generated: $(date)
# Sources: ${INPUT_FILES[*]}
# =============================================================================

EOF

# Process each input file
for file in "${INPUT_FILES[@]}"; do
    if [[ -f "$file" ]]; then
        echo "" >> "$OUTPUT_FILE"
        echo "# -----------------------------------------------------------------------------" >> "$OUTPUT_FILE"
        echo "# Configuration from $file" >> "$OUTPUT_FILE"  
        echo "# -----------------------------------------------------------------------------" >> "$OUTPUT_FILE"
        
        # Add filename as top-level key
        filename=$(basename "$file" .yaml)
        echo "${filename}_config:" >> "$OUTPUT_FILE"
        
        # Indent and append content
        sed 's/^/  /' "$file" >> "$OUTPUT_FILE"
    else
        echo "Warning: $file not found"
    fi
done

echo "Merged configuration saved to $OUTPUT_FILE"
