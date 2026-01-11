grep -E '\$sybase\s*->\s*query\s*\(' your_script.pl | \
awk -F'query\\(' '{
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
