
# Find all .sql files and sort them numerically
# Using printf to handle the case where no files are found
sql_files=$(find "$SEARCH_DIR" -maxdepth 1 -name "*.sql" -type f 2>/dev/null)

if [ -z "$sql_files" ]; then
    echo "No SQL files found in '$SEARCH_DIR'"
    exit 0
fi

# Sort files numerically by extracting the numeric part from filenames
# This handles cases like: 1.sql, 2.sql, 11.sql, 22.sql, 41.sql
echo "$sql_files" | \
sed 's|.*/||' | \
sort -t. -k1,1n | \
while read -r file; do
    # Print the full path if searching in a different directory
    if [ "$SEARCH_DIR" != "." ]; then
        echo "$SEARCH_DIR/$file"
    else
        echo "$file"
    fi
done
