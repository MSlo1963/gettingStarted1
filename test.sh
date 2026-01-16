#!/bin/bash

# SQL Section Extractor
# Extracts SQL sections from files with [section_name] headers into separate .sql files
# Handles both Unix (LF) and Windows (CRLF) line endings
#
# Usage: ./sql_extractor.sh <input_file1> [input_file2] ...
# Example: ./sql_extractor.sh queries.sql reports.sql

VERSION="1.0"

# Colors for output (if terminal supports it)
if [ -t 1 ]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    BLUE='\033[0;34m'
    BOLD='\033[1m'
    NC='\033[0m' # No Color
else
    RED=''
    GREEN=''
    YELLOW=''
    BLUE=''
    BOLD=''
    NC=''
fi

# Function to display usage
show_usage() {
    echo -e "${BOLD}SQL Section Extractor v${VERSION}${NC}"
    echo ""
    echo "Usage: $0 <input_file1> [input_file2] [input_file3] ..."
    echo "       $0 *.sql"
    echo ""
    echo "Examples:"
    echo "  $0 queries.sql"
    echo "  $0 file1.sql file2.sql file3.sql"
    echo "  $0 *.sql"
    echo ""
    echo -e "${BOLD}Input File Format:${NC}"
    echo "  [section_name]"
    echo "  SELECT * FROM table1;"
    echo "  "
    echo "  [another_section]"
    echo "  UPDATE table2 SET column = value;"
    echo ""
    echo -e "${BOLD}Output:${NC}"
    echo "  Creates separate .sql files in 'extracted_sql/' directory"
    echo "  Each [section_name] becomes section_name.sql"
    echo ""
    echo -e "${BOLD}Options:${NC}"
    echo "  -h, --help     Show this help message"
    echo "  -v, --version  Show version information"
}

# Function to process a single file
process_file() {
    local input_file="$1"
    local file_basename=$(basename "$input_file" .sql)

    echo -e "Processing: ${BLUE}$input_file${NC}"

    # Create subdirectory for this file's extracts if processing multiple files
    local output_dir="extracted_sql"
    if [ $# -gt 1 ]; then
        output_dir="extracted_sql/${file_basename}"
    fi
    mkdir -p "$output_dir"

    # Count files before processing
    local files_before=$(find "$output_dir" -name "*.sql" 2>/dev/null | wc -l)

    # Use awk to process the file with proper line ending handling
    awk -v output_dir="$output_dir" '
    BEGIN {
        current_section = ""
        content = ""
        RS = "\r?\n"  # Handle both Unix and Windows line endings
    }

    # Remove any trailing carriage returns
    {
        gsub(/\r$/, "")
    }

    # Match section headers like [sql1], [sql2], etc.
    /^\[.*\]$/ {
        # Save previous section if exists
        if (current_section != "" && content != "") {
            # Remove leading/trailing whitespace
            gsub(/^[ \t\n\r]*|[ \t\n\r]*$/, "", content)
            if (content != "") {
                print "  ✓ Extracting: " current_section > "/dev/stderr"
                print content > (output_dir "/" current_section ".sql")
            }
        }

        # Start new section - remove brackets and whitespace
        section_name = $0
        gsub(/^\[/, "", section_name)
        gsub(/\]$/, "", section_name)
        gsub(/^[ \t]*|[ \t]*$/, "", section_name)

        current_section = section_name
        content = ""
        print "  Found: [" current_section "]" > "/dev/stderr"
        next
    }

    # Add content lines to current section
    current_section != "" {
        if (content == "") {
            content = $0
        } else {
            content = content "\n" $0
        }
    }

    END {
        # Handle the last section
        if (current_section != "" && content != "") {
            gsub(/^[ \t\n\r]*|[ \t\n\r]*$/, "", content)
            if (content != "") {
                print "  ✓ Extracting: " current_section > "/dev/stderr"
                print content > (output_dir "/" current_section ".sql")
            }
        }
    }
    ' "$input_file"

    # Count files after processing to determine how many were created
    local files_after=$(find "$output_dir" -name "*.sql" 2>/dev/null | wc -l)
    local sections_extracted=$((files_after - files_before))

    echo -e "  ${GREEN}→ $sections_extracted sections extracted${NC}"
    echo ""

    return $sections_extracted
}

# Main script starts here

# Check for help or version flags
case "$1" in
    -h|--help)
        show_usage
        exit 0
        ;;
    -v|--version)
        echo "SQL Section Extractor v${VERSION}"
        exit 0
        ;;
    "")
        echo -e "${RED}Error: No input files specified${NC}"
        echo ""
        show_usage
        exit 1
        ;;
esac

echo -e "${BOLD}SQL Section Extractor v${VERSION}${NC}"
echo "==============================="
echo ""

# Create main output directory
mkdir -p extracted_sql

# Counters for summary
total_files=0
total_sections=0
processed_files=()
failed_files=()

# Process each input file
for input_file in "$@"; do
    if [ ! -f "$input_file" ]; then
        echo -e "${YELLOW}Warning: File '$input_file' not found, skipping...${NC}"
        failed_files+=("$input_file")
        continue
    fi

    # Check if file has .sql extension (warning only)
    if [[ ! "$input_file" =~ \.sql$ ]]; then
        echo -e "${YELLOW}Note: '$input_file' doesn't have .sql extension${NC}"
    fi

    # Process the file
    process_file "$input_file"
    sections_count=$?

    if [ "$sections_count" -gt 0 ]; then
        total_files=$((total_files + 1))
        total_sections=$((total_sections + sections_count))
        processed_files+=("$input_file")
    else
        failed_files+=("$input_file")
    fi
done

# Display summary
echo "==============================="
echo -e "${BOLD}SUMMARY${NC}"
echo "==============================="
echo -e "Files processed: ${GREEN}$total_files${NC}"
echo -e "Sections extracted: ${GREEN}$total_sections${NC}"
echo ""

if [ ${#processed_files[@]} -gt 0 ]; then
    echo -e "${GREEN}Successfully processed:${NC}"
    for file in "${processed_files[@]}"; do
        echo "  ✓ $file"
    done
    echo ""
fi

if [ ${#failed_files[@]} -gt 0 ]; then
    echo -e "${YELLOW}Skipped/Failed:${NC}"
    for file in "${failed_files[@]}"; do
        echo "  ✗ $file"
    done
    echo ""
fi

# Show extracted files
if [ $total_sections -gt 0 ]; then
    echo -e "${BOLD}Extracted files:${NC}"
    find extracted_sql -name "*.sql" | sort | while read -r file; do
        lines=$(wc -l < "$file" 2>/dev/null || echo "0")
        bytes=$(wc -c < "$file" 2>/dev/null || echo "0")
        echo "  $file ($lines lines, $bytes bytes)"
    done
    echo ""
    echo -e "${GREEN}All files saved in 'extracted_sql/' directory${NC}"
else
    echo -e "${RED}No sections were extracted!${NC}"
    echo ""
    echo "Please check your input file format. Expected:"
    echo "  [section_name]"
    echo "  SQL content here..."
    echo "  "
    echo "  [another_section]"
    echo "  More SQL content..."

    if [ ${#processed_files[@]} -gt 0 ]; then
        echo ""
        echo "Debugging info for first file:"
        first_file="${processed_files[0]}"
        echo "File: $first_file"
        echo "Size: $(wc -c < "$first_file") bytes"
        echo "Lines: $(wc -l < "$first_file") lines"
        echo "First 5 lines:"
        head -5 "$first_file" | cat -n
    fi
fi

echo ""
echo -e "${BOLD}Done!${NC}"
