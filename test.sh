grep -E '\$sybase\s*->\s*query\s*\(' your_script.pl | \
grep -oE '[a-zA-Z_][a-zA-Z0-9_]*\s*=>' | \
sed 's/\s*=>.*//' | \
sort | uniq
