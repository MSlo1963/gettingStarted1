#!/bin/bash

extract_sql() {
    local file="$1"
    local target_id="$2"
    
    if [[ ! -f "$file" ]]; then
        echo "Error: File '$file' not found" >&2
        return 1
    fi
    
    local found_id=false
    local in_sql_block=false
    local sql_content=""
    local base_indent=0
    
    while IFS= read -r line; do
        # Look for exact id match
        if [[ "$line" =~ ^[[:space:]]*id:[[:space:]]*${target_id}[[:space:]]*$ ]]; then
            found_id=true
            continue
        fi
        
        # If we found our ID, look for the sql block
        if [[ "$found_id" == true ]]; then
            # Check if we've moved to a different item (new id:)
            if [[ "$line" =~ ^[[:space:]]*id:[[:space:]]*.+$ ]] && [[ ! "$line" =~ ${target_id} ]]; then
                # We've moved to a different item, stop searching
                break
            fi
            
            # Look for sql: |- or sql: |
            if [[ "$line" =~ ^[[:space:]]*sql:[[:space:]]*\|[\-]?[[:space:]]*$ ]]; then
                in_sql_block=true
                continue
            fi
            
            # If we're in the SQL block, collect indented content
            if [[ "$in_sql_block" == true ]]; then
                if [[ "$line" =~ ^[[:space:]]+.+ ]]; then
                    # This is part of the SQL content (indented)
                    sql_line=$(echo "$line" | sed 's/^[[:space:]][[:space:]]*//')
                    if [[ -n "$sql_content" ]]; then
                        sql_content="${sql_content}"$'\n'"${sql_line}"
                    else
                        sql_content="${sql_line}"
                    fi
                elif [[ "$line" =~ ^[[:space:]]*$ ]]; then
                    # Empty line within SQL block
                    if [[ -n "$sql_content" ]]; then
                        sql_content="${sql_content}"$'\n'
                    fi
                else
                    # Non-indented line or new field - end of SQL block
                    if [[ "$line" =~ ^[[:space:]]*[a-zA-Z_]+:[[:space:]]* ]]; then
                        break
                    fi
                fi
            fi
        fi
    done < "$file"
    
    if [[ -n "$sql_content" ]]; then
        echo "$sql_content"
        return 0
    else
        echo "No SQL found for id: $target_id" >&2
        return 1
    fi
}

# Main script
if [[ $# -ne 2 ]]; then
    echo "Usage: $0 <yaml_file> <id>"
    echo "Example: $0 queries.yaml get_tr_data"
    exit 1
fi

extract_sql "$1" "$2"
