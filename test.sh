#!/bin/bash

# Check if method name is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <method_name> [script_file]"
    echo "Example: $0 query my_script.pl"
    echo "Example: $0 do_query my_script.pl"
    exit 1
fi

METHOD="$1"
SCRIPT_FILE="${2:-*.pl}"  # Default to all .pl files if not specified

echo "Extracting parameters for method: $METHOD"
echo "From file(s): $SCRIPT_FILE"
echo "----------------------------------------"

grep -E "\\\$sybase\\s*->\\s*${METHOD}\\s*\\(" $SCRIPT_FILE | \
awk -F"${METHOD}\\\\(" '{
    for(i=2; i<=NF; i++) {
        # Extract everything until the matching closing parenthesis
        content = $i
        gsub(/\).*$/, "", content)  # remove from first ) to end
        
        # Split on commas and extract parameter => value pairs
        n = split(content, params, /,/)
        for(j=1; j<=n; j++) {
            # Clean up whitespace
            gsub(/^[[:space:]]+|[[:space:]]+$/, "", params[j])
            # If it contains =>, print the whole thing
            if(params[j] ~ /=>/) {
                print params[j]
            }
        }
    }
}' | \
sort | uniq
