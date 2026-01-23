#!/usr/bin/awk -f

BEGIN {
    if (target_id == "") {
        print "Usage: awk -f extract_sql.awk -v target_id=<id> <yaml_file>"
        exit 1
    }
    found_id = 0
    in_sql = 0
    sql_content = ""
}

# Look for exact id match
/^[[:space:]]*id:[[:space:]]*/ {
    # Extract the id value
    gsub(/^[[:space:]]*id:[[:space:]]*/, "")
    gsub(/[[:space:]]*$/, "")
    
    if ($0 == target_id) {
        found_id = 1
        in_sql = 0
    } else {
        found_id = 0
        in_sql = 0
    }
    next
}

# If we found our target id, look for sql block
/^[[:space:]]*sql:[[:space:]]*\|[\-]?[[:space:]]*$/ && found_id {
    in_sql = 1
    next
}

# Collect SQL content when in sql block
in_sql && found_id {
    if (/^[[:space:]]+/) {
        # Remove base indentation and collect
        gsub(/^[[:space:]]+/, "")
        if (sql_content == "") {
            sql_content = $0
        } else {
            sql_content = sql_content "\n" $0
        }
    } else if (/^[[:space:]]*$/) {
        # Empty line in SQL block
        if (sql_content != "") {
            sql_content = sql_content "\n"
        }
    } else if (/^[[:space:]]*[a-zA-Z_]+:[[:space:]]*/) {
        # Hit another field, end of SQL
        in_sql = 0
    }
}

END {
    if (sql_content != "") {
        print sql_content
    } else {
        print "No SQL found for id: " target_id > "/dev/stderr"
        exit 1
    }
}
