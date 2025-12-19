#!/usr/bin/perl
use strict;
use warnings;
use Getopt::Long;

# Enhanced SQL Variable Extractor - Multi-Assignment Version
# Extracts ALL SQL statements stored in Perl variables, including multiple assignments
# to the same variable name, and outputs them in YAML format with line number references

# Command line options
my @target_variables = ();
my $output_file = 'extracted_sql.yaml';
my $list_only = 0;
my $help = 0;

GetOptions(
    'variable|v=s@' => \@target_variables,
    'output|o=s'    => \$output_file,
    'list-variables|l' => \$list_only,
    'help|h'        => \$help,
) or show_usage();

show_usage() if $help;

if (@ARGV == 0) {
    print "Error: Input file required\n\n";
    show_usage();
}

my $input_file = $ARGV[0];

# Check if input file exists
unless (-f $input_file) {
    die "Error: Input file '$input_file' not found!\n";
}

sub show_usage {
    print <<'USAGE';
SQL Variable Extractor v2.2 - Extract ALL SQL statements from Perl variables (Multi-Assignment)

SYNOPSIS:
    sql_variable_extractor_multi.pl [options] <input_file>

OPTIONS:
    -v, --variable <name>    Filter by specific variable name (can be used multiple times)
    -o, --output <file>      Output YAML file (default: extracted_sql.yaml)
    -l, --list-variables     List all SQL variables found without extracting
    -h, --help              Show this help message

FEATURES:
    - Captures MULTIPLE assignments to the same variable name
    - Each assignment gets a unique identifier with line number
    - Supports all common Perl quoting styles
    - Formatted SQL output with proper indentation

EXAMPLES:
    # Extract all SQL variables (including multiple assignments)
    perl sql_variable_extractor_multi.pl script.pl

    # Extract only specific variables (all assignments)
    perl sql_variable_extractor_multi.pl -v query -v select_stmt script.pl

    # List all variables without extracting
    perl sql_variable_extractor_multi.pl --list-variables script.pl

    # Specify output file
    perl sql_variable_extractor_multi.pl -o my_queries.yaml script.pl

USAGE
    exit(@_ ? 1 : 0);
}

sub extract_sql_variables_multi {
    my $filename = shift;
    my @found_assignments = ();

    # Read file content
    open my $fh, '<', $filename or die "Cannot open $filename: $!";
    my $content = do { local $/; <$fh> };
    close $fh;

    # Remove comments to avoid false matches
    my $clean_content = remove_comments($content);

    # Pattern to match ALL variable assignments with SQL
    # This captures both 'my $var =' and '$var =' patterns
    my $var_assignment_pattern = qr/
        (?:^|\n|\;)\s*              # Start of line or after statement
        (?:my\s+)?                  # Optional 'my' declaration
        (\$\w+)                     # Variable name (capture group 1)
        \s*=\s*                     # Assignment operator
        (?:
            (["'`])                 # Quote character (capture group 2)
            ((?:[^\\]|\\.)*?)       # SQL content with escaped chars (capture group 3)
            \2                      # Matching quote
        |
            q\{                     # q{} quoting
            ((?:[^\\}]|\\.)*?)      # SQL content (capture group 4)
            \}
        |
            q\(                     # q() quoting
            ((?:[^\\)]|\\.)*?)      # SQL content (capture group 5)
            \)
        |
            q\[                     # q[] quoting
            ((?:[^\\]]|\\.)*?)      # SQL content (capture group 6)
            \]
        |
            qq\{                    # qq{} quoting
            ((?:[^\\}]|\\.)*?)      # SQL content (capture group 7)
            \}
        |
            qq\(                    # qq() quoting
            ((?:[^\\)]|\\.)*?)      # SQL content (capture group 8)
            \)
        )
        \s*;?                       # Optional semicolon
    /xms;

    # Find ALL variable assignments (global match with 'g' flag)
    my $match_count = 0;
    while ($clean_content =~ /$var_assignment_pattern/g) {
        $match_count++;
        my $var_name = $1;
        my $quote_char = $2;
        my $sql_content = $3 || $4 || $5 || $6 || $7 || $8 || '';

        # Skip if no SQL content
        next unless $sql_content;

        # Check if it looks like SQL (more comprehensive check)
        next unless $sql_content =~ /\b(?:SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|TRUNCATE|GRANT|REVOKE|WITH|BEGIN|COMMIT|ROLLBACK|CALL|EXECUTE|SHOW|DESCRIBE|EXPLAIN)\b/i;

        # Find line number using original content (with comments)
        my $match_pos = pos($clean_content) - length($&);
        my $line_number = find_line_number($content, $clean_content, $match_pos, $sql_content);

        # Clean and format SQL
        my $cleaned_sql = clean_and_format_sql($sql_content);
        my $sql_type = detect_sql_type($cleaned_sql);

        # Create unique identifier for this assignment
        my $base_var_name = $var_name;
        $base_var_name =~ s/^\$//; # Remove $ prefix for cleaner names
        my $unique_id = sprintf("%s_line_%d", $base_var_name, $line_number);

        # Store assignment info with unique identifier
        push @found_assignments, {
            unique_id => $unique_id,
            variable_name => $base_var_name,
            original_var => $var_name,
            sql => $cleaned_sql,
            type => $sql_type,
            line_number => $line_number,
            quote_style => $quote_char || 'special',
            assignment_number => count_previous_assignments(\@found_assignments, $base_var_name) + 1,
        };
    }

    return \@found_assignments;
}

sub count_previous_assignments {
    my ($assignments_ref, $var_name) = @_;
    my $count = 0;
    foreach my $assignment (@$assignments_ref) {
        $count++ if $assignment->{variable_name} eq $var_name;
    }
    return $count;
}

sub remove_comments {
    my $content = shift;

    # Remove single-line comments (# style) but preserve in strings
    $content =~ s/(?<!["'])#.*$//gm;

    # Remove C-style single-line comments (//)
    $content =~ s/(?<!["'])\/\/.*$//gm;

    # Remove C-style multi-line comments (/* ... */)
    $content =~ s/\/\*.*?\*\///gs;

    # Remove SQL-style comments (-- style)
    $content =~ s/(?<!["'])--.*$//gm;

    return $content;
}

sub find_line_number {
    my ($original_content, $clean_content, $clean_pos, $sql_snippet) = @_;

    # Find a unique part of the SQL to locate in original content
    my $search_snippet = substr($sql_snippet, 0, 30);
    $search_snippet =~ s/^\s+|\s+$//g; # trim

    # Search for this snippet in the original content
    my $pos = index($original_content, $search_snippet);
    if ($pos == -1) {
        # Fallback: try to estimate based on clean content position
        my $before_clean = substr($clean_content, 0, $clean_pos);
        my $line_count = ($before_clean =~ tr/\n//) + 1;
        return $line_count;
    }

    # Count newlines before the found position
    my $before_match = substr($original_content, 0, $pos);
    my $line_number = ($before_match =~ tr/\n//) + 1;

    return $line_number;
}

sub clean_and_format_sql {
    my $sql = shift;

    # Remove leading/trailing whitespace
    $sql =~ s/^\s+|\s+$//g;

    # Normalize whitespace
    $sql =~ s/\s+/ /g;

    # Basic SQL formatting
    $sql = format_sql($sql);

    return $sql;
}

sub format_sql {
    my $sql = shift;

    # Handle SELECT statements
    if ($sql =~ /^SELECT\b/i) {
        $sql =~ s/\bFROM\b/\n  FROM/gi;
        $sql =~ s/\bWHERE\b/\n WHERE/gi;
        $sql =~ s/\bORDER\s+BY\b/\n ORDER BY/gi;
        $sql =~ s/\bGROUP\s+BY\b/\n GROUP BY/gi;
        $sql =~ s/\bHAVING\b/\n HAVING/gi;
        $sql =~ s/\bLIMIT\b/\n LIMIT/gi;
        $sql =~ s/\bOFFSET\b/\n OFFSET/gi;
        $sql =~ s/\bUNION(\s+ALL)?\b/\nUNION$1/gi;

        # Handle JOINs
        $sql =~ s/\b((?:INNER|LEFT|RIGHT|OUTER|FULL|CROSS)\s+)?JOIN\b/\n  $1JOIN/gi;
        $sql =~ s/\bON\b/\n     ON/gi;

        # Format AND/OR in WHERE clauses
        $sql =~ s/\b(AND|OR)\b(?=\s+\w)/\n       $1/gi;

        # Format field lists (if long)
        if ($sql =~ /SELECT\s+(.+?)\s+FROM/is) {
            my $fields = $1;
            if ($fields =~ /,/ && length($fields) > 60) {
                $fields =~ s/\s*,\s*/,\n       /g;
                $sql =~ s/SELECT\s+.+?\s+FROM/SELECT $fields\n  FROM/is;
            }
        }
    }

    # Handle INSERT statements
    elsif ($sql =~ /^INSERT\b/i) {
        $sql =~ s/\bINTO\b/\n  INTO/gi;
        $sql =~ s/\bVALUES\b/\n VALUES/gi;
        $sql =~ s/\bSELECT\b/\n SELECT/gi;

        # Format column lists
        if ($sql =~ /\(([^)]+)\)\s+VALUES/i) {
            my $cols = $1;
            if ($cols =~ /,/) {
                $cols =~ s/\s*,\s*/,\n       /g;
                $sql =~ s/\([^)]+\)\s+VALUES/(\n       $cols\n     )\n VALUES/i;
            }
        }
    }

    # Handle UPDATE statements
    elsif ($sql =~ /^UPDATE\b/i) {
        $sql =~ s/\bSET\b/\n   SET/gi;
        $sql =~ s/\bWHERE\b/\n WHERE/gi;

        # Format SET clauses
        if ($sql =~ /SET\s+(.+?)(?:\s+WHERE|\s*$)/is) {
            my $sets = $1;
            if ($sets =~ /,/) {
                $sets =~ s/\s*,\s*/,\n       /g;
                $sql =~ s/SET\s+.+?(?=\s+WHERE|\s*$)/SET $sets/is;
            }
        }
    }

    # Handle DELETE statements
    elsif ($sql =~ /^DELETE\b/i) {
        $sql =~ s/\bFROM\b/\n  FROM/gi;
        $sql =~ s/\bWHERE\b/\n WHERE/gi;
        $sql =~ s/\b(AND|OR)\b/\n       $1/gi;
    }

    # Handle CREATE TABLE statements
    elsif ($sql =~ /^CREATE\s+TABLE\b/i) {
        $sql =~ s/\s*\(\s*/\n(\n/g;
        $sql =~ s/\s*\)\s*/\n)/g;

        # Format column definitions
        if ($sql =~ /CREATE\s+TABLE[^(]+\((.+)\)/is) {
            my $table_def = $1;
            my @parts = split /,(?![^()]*\))/, $table_def;
            my $formatted_def = "";
            foreach my $part (@parts) {
                $part =~ s/^\s+|\s+$//g;
                $formatted_def .= "    $part,\n" if $part;
            }
            $formatted_def =~ s/,\n$/\n/;
            $sql =~ s/\((.+)\)/(\n$formatted_def)/is;
        }
    }

    # Clean up formatting
    $sql =~ s/\n\s*\n/\n/g;  # Remove empty lines
    $sql =~ s/^\n+//;        # Remove leading newlines
    $sql =~ s/\n+$//;        # Remove trailing newlines

    return $sql;
}

sub detect_sql_type {
    my $sql = shift;

    if ($sql =~ /^\s*SELECT\b/i) { return 'SELECT'; }
    elsif ($sql =~ /^\s*INSERT\b/i) { return 'INSERT'; }
    elsif ($sql =~ /^\s*UPDATE\b/i) { return 'UPDATE'; }
    elsif ($sql =~ /^\s*DELETE\b/i) { return 'DELETE'; }
    elsif ($sql =~ /^\s*CREATE\b/i) { return 'CREATE'; }
    elsif ($sql =~ /^\s*DROP\b/i) { return 'DROP'; }
    elsif ($sql =~ /^\s*ALTER\b/i) { return 'ALTER'; }
    elsif ($sql =~ /^\s*TRUNCATE\b/i) { return 'TRUNCATE'; }
    elsif ($sql =~ /^\s*GRANT\b/i) { return 'GRANT'; }
    elsif ($sql =~ /^\s*REVOKE\b/i) { return 'REVOKE'; }
    elsif ($sql =~ /^\s*WITH\b/i) { return 'WITH'; }
    elsif ($sql =~ /^\s*BEGIN\b/i) { return 'BEGIN'; }
    elsif ($sql =~ /^\s*COMMIT\b/i) { return 'COMMIT'; }
    elsif ($sql =~ /^\s*ROLLBACK\b/i) { return 'ROLLBACK'; }
    elsif ($sql =~ /^\s*CALL\b/i) { return 'CALL'; }
    elsif ($sql =~ /^\s*EXECUTE\b/i) { return 'EXECUTE'; }
    elsif ($sql =~ /^\s*SHOW\b/i) { return 'SHOW'; }
    elsif ($sql =~ /^\s*DESCRIBE\b/i) { return 'DESCRIBE'; }
    elsif ($sql =~ /^\s*EXPLAIN\b/i) { return 'EXPLAIN'; }
    else { return 'UNKNOWN'; }
}

sub filter_assignments {
    my ($assignments_ref, $target_vars_ref) = @_;

    return $assignments_ref unless @$target_vars_ref;

    my @filtered = ();
    foreach my $assignment (@$assignments_ref) {
        foreach my $target (@$target_vars_ref) {
            if ($assignment->{variable_name} eq $target) {
                push @filtered, $assignment;
                last;
            }
        }
    }

    return \@filtered;
}

sub list_variables {
    my $assignments_ref = shift;

    print "SQL Variable Assignments found in $input_file:\n";
    print "=" x 60, "\n";

    if (@$assignments_ref == 0) {
        print "No SQL variable assignments found.\n";
        return;
    }

    # Group by variable name for display
    my %var_groups = ();
    foreach my $assignment (@$assignments_ref) {
        push @{$var_groups{$assignment->{variable_name}}}, $assignment;
    }

    foreach my $var_name (sort keys %var_groups) {
        my $assignments = $var_groups{$var_name};
        print "\nVariable: \$$var_name (" . scalar(@$assignments) . " assignment" . (@$assignments > 1 ? "s" : "") . ")\n";
        print "-" x 40, "\n";

        foreach my $assignment (sort { $a->{line_number} <=> $b->{line_number} } @$assignments) {
            printf "  Line %-4d: %-8s (Assignment #%d)\n",
                   $assignment->{line_number},
                   $assignment->{type},
                   $assignment->{assignment_number};
        }
    }

    print "\nSummary:\n";
    print "Total unique variables: " . scalar(keys %var_groups) . "\n";
    print "Total assignments: " . scalar(@$assignments_ref) . "\n";
}

sub save_to_yaml {
    my ($assignments_ref, $output_file) = @_;

    open my $fh, '>', $output_file or die "Cannot write to $output_file: $!";

    # Count variables and assignments
    my %var_counts = ();
    my %type_counts = ();
    foreach my $assignment (@$assignments_ref) {
        $var_counts{$assignment->{variable_name}}++;
        $type_counts{$assignment->{type}}++;
    }

    # YAML header
    print $fh "---\n";
    print $fh "metadata:\n";
    print $fh "  source_file: \"$input_file\"\n";
    print $fh "  extracted_at: \"" . scalar(localtime()) . "\"\n";
    print $fh "  total_unique_variables: " . scalar(keys %var_counts) . "\n";
    print $fh "  total_assignments: " . scalar(@$assignments_ref) . "\n";
    print $fh "  extractor_version: \"2.2\"\n";

    if (@target_variables) {
        print $fh "  filtered_variables:\n";
        foreach my $var (@target_variables) {
            print $fh "    - \"$var\"\n";
        }
    }

    print $fh "  variable_assignment_counts:\n";
    foreach my $var_name (sort keys %var_counts) {
        print $fh "    $var_name: $var_counts{$var_name}\n";
    }

    print $fh "sql_assignments:\n";

    if (@$assignments_ref == 0) {
        print $fh "  # No SQL variable assignments found\n";
        close $fh;
        return;
    }

    foreach my $assignment (sort { $a->{line_number} <=> $b->{line_number} } @$assignments_ref) {
        print $fh "  $assignment->{unique_id}:\n";
        print $fh "    variable_name: \"$assignment->{variable_name}\"\n";
        print $fh "    assignment_number: $assignment->{assignment_number}\n";
        print $fh "    line_number: $assignment->{line_number}\n";
        print $fh "    sql_type: \"$assignment->{type}\"\n";
        print $fh "    quote_style: \"$assignment->{quote_style}\"\n";
        print $fh "    sql: |\n";

        # Format SQL with proper indentation
        my @sql_lines = split /\n/, $assignment->{sql};
        foreach my $line (@sql_lines) {
            print $fh "      $line\n";
        }
    }

    close $fh;
}

# Main execution
print "SQL Variable Extractor v2.2 (Multi-Assignment)\n";
print "Analyzing: $input_file\n";

# Extract all SQL variable assignments
my $all_assignments = extract_sql_variables_multi($input_file);

if ($list_only) {
    list_variables($all_assignments);
    exit 0;
}

# Filter assignments if specific variables were requested
my $assignments_to_output = filter_assignments($all_assignments, \@target_variables);

if (@target_variables && @$assignments_to_output == 0) {
    print "No SQL variable assignments found matching: " . join(', ', @target_variables) . "\n";
    print "Available variables:\n";
    list_variables($all_assignments);
    exit 1;
}

# Show summary
my %unique_vars = ();
foreach my $assignment (@$assignments_to_output) {
    $unique_vars{$assignment->{variable_name}}++;
}

if (@target_variables) {
    printf "Found %d assignment(s) for %d variable(s)\n",
           scalar(@$assignments_to_output), scalar(keys %unique_vars);
} else {
    printf "Found %d assignment(s) for %d variable(s)\n",
           scalar(@$assignments_to_output), scalar(keys %unique_vars);
}

# Save to YAML
save_to_yaml($assignments_to_output, $output_file);

print "SQL variable assignments extracted and saved to: $output_file\n";

# Show detailed summary
print "\nDetailed Summary:\n";
my %var_summary = ();
my %type_summary = ();

foreach my $assignment (@$assignments_to_output) {
    $var_summary{$assignment->{variable_name}}++;
    $type_summary{$assignment->{type}}++;
}

print "By Variable:\n";
foreach my $var (sort keys %var_summary) {
    print "  \$$var: $var_summary{$var} assignment" . ($var_summary{$var} > 1 ? "s" : "") . "\n";
}

print "\nBy SQL Type:\n";
foreach my $type (sort keys %type_summary) {
    print "  $type: $type_summary{$type} assignment" . ($type_summary{$type} > 1 ? "s" : "") . "\n";
}
