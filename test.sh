#!/bin/bash

extract_sql() {
    local file="$1"
    local id="$2"
    local temp_file=$(mktemp)
    
    # Find the block for our ID and extract it
    awk -v target="$id" '
    BEGIN { found=0; printing=0 }
    /^[[:space:]]*-[[:space:]]*id:/ { 
        if ($0 ~ target) { 
            found=1; printing=1 
        } else { 
            printing=0 
        } 
    }
    /^[[:space:]]*-[[:space:]]*id:/ && printing && found && $0 !~ target { 
        printing=0 
    }
    printing { print }
    ' "$file" > "$temp_file"
    
    # Extract just the SQL part
    if [[ -s "$temp_file" ]]; then
        # Find sql line and extract everything after it that's indented
        grep -A 1000 "sql:[[:space:]]*|" "$temp_file" | \
        tail -n +2 | \
        while read -r line; do
            if [[ "$line" =~ ^[[:space:]]+.+ ]]; then
                echo "$line" | sed 's/^[[:space:]]*//'
            elif [[ "$line" =~ ^[[:space:]]*$ ]]; then
                echo ""
            else
                break
            fi
        done
    else
        echo "No SQL found for id: $id" >&2
        rm "$temp_file"
        return 1
    fi
    
    rm "$temp_file"
}

# Usage
extract_sql "$1" "$2"
