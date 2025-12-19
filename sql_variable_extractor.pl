#!/usr/bin/perl
use strict;
use warnings;
use Getopt::Long;

# Enhanced SQL Variable Extractor
# Extracts SQL statements stored in Perl variables and outputs them in YAML format
# with line number references and variable name filtering

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
SQL Variable Extractor v2.1 - Extract SQL statements from Perl variables

SYNOPSIS:
    sql_variable_extractor.pl [options] <input_file>

OPTIONS:
    -v, --variable <name>    Filter by specific variable name (can be used multiple times)
    -o, --output <file>      Output YAML file (default: extracted_sql.yaml)
    -l, --list-variables     List all SQL variables found without extracting
    -h, --help              Show this help message

EXAMPLES:
    # Extract all SQL variables
    perl sql_variable_extractor.pl script.pl

    # Extract only specific variables
    perl sql_variable_extractor.pl -v select_users -v insert_query script.pl

    # List all variables without extracting
    perl sql_variable_extractor.pl --list-variables script.pl

    # Specify output file
    perl sql_variable_extractor.pl -o my_queries.yaml script.pl

USAGE
    exit(@_ ? 1 : 0);
}

sub extract_sql_variables {
    my $filename = shift;
    my %found_variables = ();

    # Read file content
    open my $fh, '<', $filename or die "Cannot open $filename: $!";
    my $content = do { local $/; <$fh> };
    close $fh;

    # Pattern to match variable assignments with SQL
    # Matches: my $var = "SQL..."; or $var = 'SQL...'; or $var = q{SQL...}; etc.
    my $var_assignment_pattern = qr/
        (?:my\s+)?              # Optional 'my' declaration
        \$(\w+)                 # Variable name (capture group 1)
        \s*=\s*                 # Assignment operator
        (?:
            (["'`])             # Quote character (capture group 2)
            (.*?)               # SQL content (capture group 3)
            \2                  # Matching quote
        |
            q\{                 # q{} quoting
            (.*?)               # SQL content (capture group 4)
            \}
        |
            q\(                 # q() quoting
            (.*?)               # SQL content (capture group 5)
            \)
        |
            q\[                 # q[] quoting
            (.*?)               # SQL content (capture group 6)
            \]
        |
            qq\{                # qq{} quoting
            (.*?)               # SQL content (capture group 7)
            \}
        |
            qq\(                # qq() quoting
            (.*?)               # SQL content (capture group 8)
            \)
        )
        \s*;?                   # Optional semicolon
    /xims;

    # Find all variable assignments
    while ($content =~ /$var_assignment_pattern/g) {
        my $var_name = $1;
        my $quote_char = $2;
        my $sql_content = $3 || $4 || $5 || $6 || $7 || $8 || '';

        # Skip if no SQL content or if it doesn't look like SQL
        next unless $sql_content;
        next unless $sql_content =~ /\b(?:SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|TRUNCATE|GRANT|REVOKE|WITH|BEGIN|COMMIT|ROLLBACK)\b/i;

        # Find line number
        my $match_pos = pos($content) - length($&);
        my $before_match = substr($content, 0, $match_pos);
        my $line_number = ($before_match =~ tr/\n//) + 1;

        # Clean and format SQL
        my $cleaned_sql = clean_and_format_sql($sql_content);
        my $sql_type = detect_sql_type($cleaned_sql);

        # Store variable info
        $found_variables{$var_name} = {
            name => $var_name,
            sql => $cleaned_sql,
            type => $sql_type,
            line_number => $line_number,
            quote_style => $quote_char || 'special',
        };
    }

    # Also look for multi-line assignments (common pattern)
    my $multiline_pattern = qr/
        (?:my\s+)?              # Optional 'my' declaration
        \$(\w+)                 # Variable name
        \s*=\s*                 # Assignment operator
        (["'`])                 # Opening quote
        \s*\n                   # Newline after quote
        (.*?)                   # Multi-line SQL content
        \n\s*\2                 # Closing quote on new line
        \s*;?                   # Optional semicolon
    /xims;

    while ($content =~ /$multiline_pattern/g) {
        my $var_name = $1;
        my $quote_char = $2;
        my $sql_content = $3;

        # Skip if no SQL content or if it doesn't look like SQL
        next unless $sql_content;
        next unless $sql_content =~ /\b(?:SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|TRUNCATE|GRANT|REVOKE|WITH|BEGIN|COMMIT|ROLLBACK)\b/i;

        # Find line number
        my $match_pos = pos($content) - length($&);
        my $before_match = substr($content, 0, $match_pos);
        my $line_number = ($before_match =~ tr/\n//) + 1;

        # Clean and format SQL
        my $cleaned_sql = clean_and_format_sql($sql_content);
        my $sql_type = detect_sql_type($cleaned_sql);

        # Store variable info (don't overwrite if already found)
        unless (exists $found_variables{$var_name}) {
            $found_variables{$var_name} = {
                name => $var_name,
                sql => $cleaned_sql,
                type => $sql_type,
                line_number => $line_number,
                quote_style => 'multiline_' . $quote_char,
            };
        }
    }

    return \%found_variables;
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

    # Basic SQL formatting similar to pgFormatter

    # Handle SELECT statements
    if ($sql =~ /^SELECT\b/i) {
        # Format SELECT clause
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

        # Format field lists (basic)
        if ($sql =~ /SELECT\s+(.+?)\s+FROM/is) {
            my $fields = $1;
            if ($fields =~ /,/ && length($fields) > 50) {
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
    }

    # Handle UPDATE statements
    elsif ($sql =~ /^UPDATE\b/i) {
        $sql =~ s/\bSET\b/\n   SET/gi;
        $sql =~ s/\bWHERE\b/\n WHERE/gi;
    }

    # Handle DELETE statements
    elsif ($sql =~ /^DELETE\b/i) {
        $sql =~ s/\bFROM\b/\n  FROM/gi;
        $sql =~ s/\bWHERE\b/\n WHERE/gi;
    }

    # Clean up extra whitespace
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
    else { return 'UNKNOWN'; }
}

sub filter_variables {
    my ($variables_ref, $target_vars_ref) = @_;

    return $variables_ref unless @$target_vars_ref;

    my %filtered = ();
    foreach my $target (@$target_vars_ref) {
        if (exists $variables_ref->{$target}) {
            $filtered{$target} = $variables_ref->{$target};
        }
    }

    return \%filtered;
}

sub list_variables {
    my $variables_ref = shift;

    print "SQL Variables found in $input_file:\n";
    print "=" x 50, "\n";

    if (keys %$variables_ref == 0) {
        print "No SQL variables found.\n";
        return;
    }

    foreach my $var_name (sort keys %$variables_ref) {
        my $var_info = $variables_ref->{$var_name};
        printf "Variable: \$%-20s Line: %-4d Type: %s\n",
               $var_name, $var_info->{line_number}, $var_info->{type};
    }

    print "\nTotal: " . scalar(keys %$variables_ref) . " SQL variables\n";
}

sub save_to_yaml {
    my ($variables_ref, $output_file) = @_;

    open my $fh, '>', $output_file or die "Cannot write to $output_file: $!";

    # YAML header
    print $fh "---\n";
    print $fh "metadata:\n";
    print $fh "  source_file: \"$input_file\"\n";
    print $fh "  extracted_at: \"" . scalar(localtime()) . "\"\n";
    print $fh "  total_variables: " . scalar(keys %$variables_ref) . "\n";
    print $fh "  extractor_version: \"2.1\"\n";

    if (@target_variables) {
        print $fh "  filtered_variables:\n";
        foreach my $var (@target_variables) {
            print $fh "    - \"$var\"\n";
        }
    }

    print $fh "sql_variables:\n";

    if (keys %$variables_ref == 0) {
        print $fh "  # No SQL variables found\n";
        close $fh;
        return;
    }

    foreach my $var_name (sort keys %$variables_ref) {
        my $var_info = $variables_ref->{$var_name};

        print $fh "  $var_name:\n";
        print $fh "    variable_name: \"$var_info->{name}\"\n";
        print $fh "    line_number: $var_info->{line_number}\n";
        print $fh "    sql_type: \"$var_info->{type}\"\n";
        print $fh "    quote_style: \"$var_info->{quote_style}\"\n";
        print $fh "    sql: |\n";

        # Format SQL with proper indentation
        my @sql_lines = split /\n/, $var_info->{sql};
        foreach my $line (@sql_lines) {
            print $fh "      $line\n";
        }
    }

    close $fh;
}

# Main execution
print "SQL Variable Extractor v2.1\n";
print "Analyzing: $input_file\n";

# Extract all SQL variables
my $all_variables = extract_sql_variables($input_file);

if ($list_only) {
    list_variables($all_variables);
    exit 0;
}

# Filter variables if specific ones were requested
my $variables_to_output = filter_variables($all_variables, \@target_variables);

if (@target_variables && keys %$variables_to_output == 0) {
    print "No SQL variables found matching: " . join(', ', @target_variables) . "\n";
    print "Available variables:\n";
    list_variables($all_variables);
    exit 1;
}

# Show summary
if (@target_variables) {
    print "Found " . scalar(keys %$variables_to_output) . " matching SQL variable(s)\n";
} else {
    print "Found " . scalar(keys %$variables_to_output) . " SQL variable(s)\n";
}

# Save to YAML
save_to_yaml($variables_to_output, $output_file);

print "SQL variables extracted and saved to: $output_file\n";

# Show summary by type
my %type_counts = ();
foreach my $var_info (values %$variables_to_output) {
    $type_counts{$var_info->{type}}++;
}

if (keys %type_counts > 0) {
    print "\nSummary by SQL type:\n";
    foreach my $type (sort keys %type_counts) {
        print "  $type: $type_counts{$type} variable(s)\n";
    }
}
