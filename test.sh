#!/bin/bash

# Check if variable name and script file are provided
if [ $# -lt 2 ]; then
    echo "Usage: $0 <variable_name> <script_file> [output_directory]"
    echo "Example: $0 queryselect my_script.pl"
    echo "Example: $0 queryselect my_script.pl ./definitions"
    exit 1
fi

VARIABLE="$1"
SCRIPT_FILE="$2"
OUTPUT_DIR="${3:-./variable_definitions}"

# Remove $ from variable name if provided
CLEAN_VAR=$(echo "$VARIABLE" | sed 's/^\$//')

# Create output directory
mkdir -p "$OUTPUT_DIR"

echo "Extracting definitions for variable: \$${CLEAN_VAR}"
echo "From file: $SCRIPT_FILE"
echo "Output directory: $OUTPUT_DIR"
echo "----------------------------------------"

# Use awk to process the file
awk -v var="$CLEAN_VAR" -v outdir="$OUTPUT_DIR" '
BEGIN {
    in_definition = 0
    definition = ""
    start_line = 0
    definition_count = 0
}

{
    # Check if this line starts a new definition of our variable
    if ($0 ~ "\\$" var "\\s*=") {
        # If we were already in a definition, save it first
        if (in_definition && definition != "") {
            definition_count++
            filename = outdir "/line_" start_line "_def_" definition_count ".pl"
            print definition > filename
            close(filename)
            print "Created: " filename " (lines " start_line "-" (NR-1) ")"
        }
        
        # Start new definition
        in_definition = 1
        definition = $0
        start_line = NR
        
        # Check if definition ends on same line
        if ($0 ~ ";\\s*$") {
            in_definition = 0
            definition_count++
            filename = outdir "/line_" start_line "_def_" definition_count ".pl"
            print definition > filename
            close(filename)
            print "Created: " filename " (line " start_line ")"
            definition = ""
        }
    }
    else if (in_definition) {
        # Continue building the definition
        definition = definition "\n" $0
        
        # Check if definition ends on this line
        if ($0 ~ ";\\s*$") {
            in_definition = 0
            definition_count++
            filename = outdir "/line_" start_line "_def_" definition_count ".pl"
            print definition > filename
            close(filename)
            print "Created: " filename " (lines " start_line "-" NR ")"
            definition = ""
        }
    }
}

END {
    # Handle case where file ends while in definition (incomplete definition)
    if (in_definition && definition != "") {
        definition_count++
        filename = outdir "/line_" start_line "_def_" definition_count "_incomplete.pl"
        print definition > filename
        close(filename)
        print "Created: " filename " (lines " start_line "-" NR ") - INCOMPLETE (no ending semicolon)"
    }
    
    if (definition_count == 0) {
        print "No definitions found for variable \$" var
    } else {
        print "----------------------------------------"
        print "Total definitions extracted: " definition_count
    }
}
' "$SCRIPT_FILE"

echo "Done!"
