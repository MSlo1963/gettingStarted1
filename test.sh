#!/bin/bash

get_sql_by_id() {
    local file_path="$1"
    local target_id="$2"
    
    if [[ ! -f "$file_path" ]]; then
        echo "Error: File '$file_path' not found" >&2
        return 1
    fi
    
    # Use yq (YAML processor) if available
    if command -v yq &> /dev/null; then
        yq eval ".[] | select(.id == \"$target_id\") | .sql" "$file_path" 2>/dev/null
    else
        # Fallback to python
        python3 -c "
import yaml, sys
with open('$file_path', 'r') as f:
    data = yaml.safe_load(f)
items = data if isinstance(data, list) else [data]
for item in items:
    if isinstance(item, dict) and item.get('id') == '$target_id':
        print(item.get('sql', ''))
        break
"
    fi
}

# Usage
if [[ $# -ne 2 ]]; then
    echo "Usage: $0 <yaml_file> <id>"
    exit 1
fi

sql_content=$(get_sql_by_id "$1" "$2")
if [[ -n "$sql_content" ]]; then
    echo "$sql_content"
else
    echo "No SQL found for id: $2" >&2
    exit 1
fi
