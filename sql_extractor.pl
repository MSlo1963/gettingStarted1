#!/usr/bin/perl
use strict;
use warnings;
# Removed YAML::XS dependency - using manual YAML generation

# SQL Extractor - Extracts SQL commands from files and saves to YAML
# Usage: perl sql_extractor.pl <input_file> [output_file.yaml]

my %sql_patterns = (
    # Standard SQL statements
    'select' => qr/\b(SELECT\s+.+?(?:;|\z))/ims,
    'insert' => qr/\b(INSERT\s+(?:INTO\s+)?.+?(?:;|\z))/ims,
    'update' => qr/\b(UPDATE\s+.+?(?:;|\z))/ims,
    'delete' => qr/\b(DELETE\s+.+?(?:;|\z))/ims,
    'create' => qr/\b(CREATE\s+(?:TABLE|VIEW|INDEX|DATABASE|SCHEMA)\s+.+?(?:;|\z))/ims,
    'drop' => qr/\b(DROP\s+(?:TABLE|VIEW|INDEX|DATABASE|SCHEMA)\s+.+?(?:;|\z))/ims,
    'alter' => qr/\b(ALTER\s+(?:TABLE|VIEW|INDEX|DATABASE|SCHEMA)\s+.+?(?:;|\z))/ims,
    'truncate' => qr/\b(TRUNCATE\s+(?:TABLE\s+)?.+?(?:;|\z))/ims,
    'grant' => qr/\b(GRANT\s+.+?(?:;|\z))/ims,
    'revoke' => qr/\b(REVOKE\s+.+?(?:;|\z))/ims,
    'commit' => qr/\b(COMMIT(?:\s+TRANSACTION)?(?:;|\z))/ims,
    'rollback' => qr/\b(ROLLBACK(?:\s+TRANSACTION)?(?:;|\z))/ims,
    'begin' => qr/\b(BEGIN(?:\s+TRANSACTION)?(?:;|\z))/ims,
);

sub clean_sql {
    my $sql = shift;

    # Remove leading/trailing whitespace
    $sql =~ s/^\s+|\s+$//g;

    # Replace multiple whitespace with single space
    $sql =~ s/\s+/ /g;

    # Remove trailing semicolon if present
    $sql =~ s/;$//;

    return $sql;
}

sub extract_variable_name {
    my ($content, $match_pos) = @_;

    # Look backwards from the match position to find variable assignment
    my $before_match = substr($content, 0, $match_pos);
    my @lines = split /\n/, $before_match;
    my $current_line = $lines[-1] || '';

    # Check if current line contains variable assignment
    if ($current_line =~ /\$(\w+)\s*=\s*["`']/) {
        return $1;
    }

    # Check previous lines for assignment (multi-line case)
    for my $i (reverse 0..$#lines) {
        if ($lines[$i] =~ /my\s+\$(\w+)\s*=/) {
            return $1;
        }
        if ($lines[$i] =~ /\$(\w+)\s*=/) {
            return $1;
        }
    }

    return "unknown";
}

sub pretty_print_sql {
    my $sql = shift;

    # Remove extra whitespace and normalize
    $sql =~ s/\s+/ /g;
    $sql =~ s/^\s+|\s+$//g;

    # pgFormatter style formatting

    # Handle SELECT statements (pgFormatter style)
    if ($sql =~ /^SELECT\b/i) {
        # Format SELECT clause with comma-first style
        $sql =~ s/SELECT\s+/SELECT /i;

        # Handle field list - extract fields between SELECT and FROM
        if ($sql =~ /SELECT\s+(.+?)\s+FROM/is) {
            my $fields = $1;
            if ($fields =~ /,/) {
                # Split fields and format with comma-first style
                my @field_list = split /\s*,\s*/, $fields;
                my $formatted_fields = shift @field_list;  # First field on same line
                foreach my $field (@field_list) {
                    $formatted_fields .= "\n     , $field";
                }
                $sql =~ s/SELECT\s+.+?\s+FROM/SELECT $formatted_fields\nFROM/is;
            }
        }

        # Major clauses on new lines with proper indentation
        $sql =~ s/\bFROM\b/\nFROM/gi;
        $sql =~ s/\bWHERE\b/\nWHERE/gi;
        $sql =~ s/\bORDER\s+BY\b/\nORDER BY/gi;
        $sql =~ s/\bGROUP\s+BY\b/\nGROUP BY/gi;
        $sql =~ s/\bHAVING\b/\nHAVING/gi;
        $sql =~ s/\bLIMIT\b/\nLIMIT/gi;
        $sql =~ s/\bOFFSET\b/\nOFFSET/gi;
        $sql =~ s/\bUNION(\s+ALL)?\b/\nUNION$1/gi;

        # Handle JOINs (pgFormatter indents JOINs)
        $sql =~ s/\b((?:INNER|LEFT|RIGHT|OUTER|FULL|CROSS)\s+JOIN)\b/\n     $1/gi;
        $sql =~ s/\bJOIN\b/\n     JOIN/gi;
        $sql =~ s/\bON\b/\n          ON/gi;

        # Handle logical operators in WHERE/HAVING clauses
        $sql =~ s/\b(AND|OR)\b(?![^()]*\))/\n       $1/gi;
    }

    # Handle INSERT statements (pgFormatter style)
    elsif ($sql =~ /^INSERT\b/i) {
        $sql =~ s/\bINTO\b/\n  INTO/gi;
        $sql =~ s/\bVALUES\b/\nVALUES/gi;

        # Format column lists in INSERT
        if ($sql =~ /INTO\s+\w+\s*\(([^)]+)\)/i) {
            my $cols = $1;
            if ($cols =~ /,/) {
                $cols =~ s/\s*,\s*/\n     , /g;
                $sql =~ s/(INTO\s+\w+\s*)\([^)]+\)/$1(\n       $cols\n     )/i;
            }
        }
    }

    # Handle UPDATE statements (pgFormatter style)
    elsif ($sql =~ /^UPDATE\b/i) {
        $sql =~ s/\bSET\b/\n   SET/gi;
        $sql =~ s/\bWHERE\b/\n WHERE/gi;

        # Format SET clauses with comma-first style
        if ($sql =~ /SET\s+(.+?)(?:\s+WHERE|\s*$)/is) {
            my $sets = $1;
            if ($sets =~ /,/) {
                my @set_list = split /\s*,\s*/, $sets;
                my $formatted_sets = shift @set_list;
                foreach my $set (@set_list) {
                    $formatted_sets .= "\n     , $set";
                }
                $sql =~ s/SET\s+.+?(?=\s+WHERE|\s*$)/SET $formatted_sets/is;
            }
        }
    }

    # Handle DELETE statements (pgFormatter style)
    elsif ($sql =~ /^DELETE\b/i) {
        $sql =~ s/\bFROM\b/\n  FROM/gi;
        $sql =~ s/\bWHERE\b/\n WHERE/gi;
        $sql =~ s/\b(AND|OR)\b/\n       $1/gi;
    }

    # Handle CREATE TABLE statements (pgFormatter style)
    elsif ($sql =~ /^CREATE\s+TABLE\b/i) {
        # Format table definition
        $sql =~ s/\s*\(\s*/\n(\n/g;
        $sql =~ s/\s*\)\s*/\n)/g;

        # Format column definitions
        if ($sql =~ /CREATE\s+TABLE[^(]+\((.+)\)/is) {
            my $table_def = $1;
            # Split by comma but be careful with function calls
            my @parts = split /,(?![^()]*\))/, $table_def;
            my $formatted_def = "";
            foreach my $part (@parts) {
                $part =~ s/^\s+|\s+$//g;  # trim
                $formatted_def .= "    $part,\n" if $part;
            }
            $formatted_def =~ s/,\n$/\n/;  # Remove last comma
            $sql =~ s/\((.+)\)/(\n$formatted_def)/is;
        }
    }

    # Handle ALTER TABLE statements (pgFormatter style)
    elsif ($sql =~ /^ALTER\s+TABLE\b/i) {
        $sql =~ s/\b(ADD|DROP|MODIFY|CHANGE|ALTER)(\s+COLUMN)?/\n    $1$2/gi;
    }

    # Handle GRANT/REVOKE statements (pgFormatter style)
    elsif ($sql =~ /^(GRANT|REVOKE)\b/i) {
        $sql =~ s/\bON\b/\n    ON/gi;
        $sql =~ s/\b(TO|FROM)\b/\n    $1/gi;
    }

    # Clean up formatting
    $sql =~ s/\n\s*\n/\n/g;        # Remove empty lines
    $sql =~ s/^\n+//;              # Remove leading newlines
    $sql =~ s/\n+$//;              # Remove trailing newlines
    $sql =~ s/[ \t]+$//gm;         # Remove trailing spaces/tabs on each line

    return $sql;
}

sub remove_comments {
    my $content = shift;

    # Remove single-line comments (# style)
    $content =~ s/^[ \t]*#.*$//gm;

    # Remove C-style single-line comments (//)
    $content =~ s/\/\/.*$//gm;

    # Remove C-style multi-line comments (/* ... */)
    $content =~ s/\/\*.*?\*\///gs;

    # Remove SQL-style comments (-- style)
    $content =~ s/--.*$//gm;

    return $content;
}

sub is_sql_in_comment {
    my ($content, $sql_match, $match_pos) = @_;

    # Get the line containing the match
    my $before_match = substr($content, 0, $match_pos);
    my @lines_before = split /\n/, $before_match;
    my $line_with_match = $lines_before[-1] || '';

    # Find the complete line by getting text after match too
    my $after_match = substr($content, $match_pos);
    my $newline_pos = index($after_match, "\n");
    my $rest_of_line = $newline_pos >= 0 ? substr($after_match, 0, $newline_pos) : $after_match;
    my $complete_line = $line_with_match . $rest_of_line;

    # Check if this line starts with comment markers (ignoring whitespace)
    if ($complete_line =~ /^\s*#/ ||           # Perl/Shell style
        $complete_line =~ /^\s*\/\// ||        # C++ style
        $complete_line =~ /^\s*--/ ||          # SQL style
        $complete_line =~ /^\s*\/\*.*\*\//) {  # C style single line
        return 1;
    }

    # Check if we're inside a multi-line C-style comment
    my $comment_start = rindex($before_match, '/*');
    my $comment_end = rindex($before_match, '*/');

    if ($comment_start >= 0 && ($comment_end < 0 || $comment_start > $comment_end)) {
        # We're inside a multi-line comment
        return 1;
    }

    return 0;
}

sub extract_sql_from_file {
    my $filename = shift;
    my @sql_commands = ();
    my $id_counter = 1;

    # Read file content
    open my $fh, '<', $filename or die "Cannot open $filename: $!";
    my $content = do { local $/; <$fh> };
    close $fh;

    # Extract SQL commands using patterns
    foreach my $type (sort keys %sql_patterns) {
        my $pattern = $sql_patterns{$type};

        while ($content =~ /$pattern/g) {
            my $sql_command = $1;
            my $match_pos = pos($content) - length($sql_command);

            # Skip if SQL is inside a comment
            next if is_sql_in_comment($content, $sql_command, $match_pos);

            my $cleaned_sql = clean_sql($sql_command);

            # Skip very short matches (likely false positives)
            next if length($cleaned_sql) < 10;

            my $line_number = extract_line_number($content, $sql_command);
            my $variable_name = extract_variable_name($content, $match_pos);
            my $pretty_sql = pretty_print_sql($cleaned_sql);

            # Create SQL entry with new ID format
            my $sql_entry = {
                id => sprintf("%s_%d_%s", lc($type), $line_number, $variable_name),
                type => uc($type),
                sql => $pretty_sql,
                meta => {
                    line_number => $line_number,
                    variable_name => $variable_name,
                    source => "direct_match"
                }
            };

            push @sql_commands, $sql_entry;
            $id_counter++;
        }
    }

    # Also look for quoted SQL strings (common in programming languages)
    my @quoted_patterns = (
        qr/"(\s*(?:SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|TRUNCATE|GRANT|REVOKE)\s+.+?)"/ims,
        qr/'(\s*(?:SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|TRUNCATE|GRANT|REVOKE)\s+.+?)'/ims,
        qr/`(\s*(?:SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|TRUNCATE|GRANT|REVOKE)\s+.+?)`/ims,
    );

    foreach my $pattern (@quoted_patterns) {
        while ($content =~ /$pattern/g) {
            my $sql_command = $1;
            my $match_pos = pos($content) - length($sql_command);

            # Skip if SQL is inside a comment
            next if is_sql_in_comment($content, $sql_command, $match_pos);

            my $cleaned_sql = clean_sql($sql_command);

            next if length($cleaned_sql) < 10;

            # Determine SQL type
            my $sql_type = "unknown";
            if ($cleaned_sql =~ /^\s*(\w+)/i) {
                $sql_type = lc($1);
            }

            my $line_number = extract_line_number($content, $sql_command);
            my $variable_name = extract_variable_name($content, $match_pos);
            my $pretty_sql = pretty_print_sql($cleaned_sql);

            my $sql_entry = {
                id => sprintf("%s_%d_%s", $sql_type, $line_number, $variable_name),
                type => uc($sql_type),
                sql => $pretty_sql,
                meta => {
                    line_number => $line_number,
                    variable_name => $variable_name,
                    source => "quoted_string"
                }
            };

            push @sql_commands, $sql_entry;
            $id_counter++;
        }
    }

    return @sql_commands;
}

sub extract_line_number {
    my ($content, $sql_match) = @_;

    # Find approximate line number where SQL was found
    my $pos = index($content, substr($sql_match, 0, 20));  # Use first 20 chars for matching
    return 1 if $pos == -1;

    my $before_match = substr($content, 0, $pos);
    my $line_count = ($before_match =~ tr/\n//) + 1;

    return $line_count;
}

sub save_to_yaml {
    my ($sql_commands_ref, $output_file) = @_;

    # Create YAML manually (no external dependencies)
    open my $fh, '>', $output_file or die "Cannot write to $output_file: $!";

    print $fh "---\n";
    print $fh "metadata:\n";
    print $fh "  extracted_at: \"" . scalar(localtime()) . "\"\n";
    print $fh "  total_commands: " . scalar(@$sql_commands_ref) . "\n";
    print $fh "  extractor_version: \"2.0\"\n";
    print $fh "sql_commands:\n";

    foreach my $cmd (@$sql_commands_ref) {
        print $fh "  - id: $cmd->{id}\n";

        # Always use multi-line format for SQL with proper indentation
        print $fh "    sql: |\n";
        my @lines = split /\n/, $cmd->{sql};
        foreach my $line (@lines) {
            $line =~ s/^\s*//; # Remove leading whitespace
            print $fh "      $line\n";
        }

        print $fh "    meta:\n";
        print $fh "      type: \"$cmd->{type}\"\n";
        print $fh "      line_number: $cmd->{meta}->{line_number}\n";
        print $fh "      variable_name: \"$cmd->{meta}->{variable_name}\"\n";
        print $fh "      source: \"$cmd->{meta}->{source}\"\n";
    }

    close $fh;
}

sub show_usage {
    print "SQL Extractor - Extracts SQL commands from files and saves to YAML\n\n";
    print "Usage:\n";
    print "  perl sql_extractor.pl <input_file> [output_file.yaml]\n";
    print "  perl sql_extractor.pl --help\n\n";
    print "Examples:\n";
    print "  perl sql_extractor.pl script.pl\n";
    print "  perl sql_extractor.pl database_script.sql extracted_sql.yaml\n";
    print "  perl sql_extractor.pl application.py sql_commands.yaml\n\n";
    print "Supported SQL Commands:\n";
    print "  SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER,\n";
    print "  TRUNCATE, GRANT, REVOKE, COMMIT, ROLLBACK, BEGIN\n\n";
    print "Output: YAML file with id and sql fields for each extracted command\n";
}

# Main execution
if (@ARGV == 0 || $ARGV[0] eq '--help' || $ARGV[0] eq '-h') {
    show_usage();
    exit 0;
}

my $input_file = $ARGV[0];
my $output_file = $ARGV[1] || 'extracted_sql.yaml';

# Check if input file exists
unless (-f $input_file) {
    die "Error: Input file '$input_file' not found!\n";
}

print "Extracting SQL commands from: $input_file\n";

# Extract SQL commands
my @sql_commands = extract_sql_from_file($input_file);

if (@sql_commands == 0) {
    print "No SQL commands found in the file.\n";
    exit 0;
}

print "Found " . scalar(@sql_commands) . " SQL command(s)\n";

# Save to YAML
save_to_yaml(\@sql_commands, $output_file);

print "SQL commands extracted and saved to: $output_file\n";

# Show summary
print "\nSummary:\n";
my %type_counts = ();
foreach my $cmd (@sql_commands) {
    $type_counts{$cmd->{type}}++;
}

foreach my $type (sort keys %type_counts) {
    print "  $type: $type_counts{$type} command(s)\n";
}
