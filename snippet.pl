#!/usr/bin/perl
use strict;
use warnings;

# Simple Code Snippet
# Usage: snippet.pl

my %snippets = (
    'sub' => 'sub function_name {
        my ($param1, $param2) = @_;
        # Your code here
        return $result;
    }',
    'hash' => 'my %hash = (
        "key1" => "value1",
        "key2" => "value2",
        "key3" => "value3"
    );',

    'array' => 'my @array = qw(item1 item2 item3 item4);',

    'loop' => 'foreach my $item (@array) {
        print "Processing: $item\n";
    }',

    'fileread' => 'open my $fh, "<", $filename or die "Cannot open $filename: $!";
        while (my $line = <$fh>) {
            chomp $line;
            # Process line
        }
        close $fh;',

    'class' => 'package MyClass;
        use strict;
        use warnings;

        sub new {
            my $class = shift;
            my $self = {
                name => shift,
                value => shift
            };
            bless $self, $class;
            return $self;
        }
1;',

    'html_basic' => '<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Document</title>
</head>
<body>

</body>
</html>',

    'js_function' => 'function functionName(param1, param2) {
    // Your code here
    return result;
}'
);

sub show_menu {
    print "\n=== Perl Code Snippet(s) ===\n";

    my $counter = 1;
    foreach my $key (sort keys %snippets) {
        print "$counter. $key\n";
        $counter++;
    }

    print "\nCommands:\n";
    print "list\n";
    print "show <snippet_name>\n";
    print "quit - Exit\n\n";
}

sub show_snippet {
    my $snippet_name = shift;

    if (exists $snippets{$snippet_name}) {
        print "\nSnippet '$snippet_name':\n";
        print "-" x 40 . "\n";
        print $snippets{$snippet_name} . "\n";
        print "-" x 40 . "\n\n";
    } else {
        print "Error: Snippet '$snippet_name' not found!\n\n";
    }
}

sub read_file_lines {
    my $filename = shift;
    my @lines = ();

    open my $fh, '<', $filename or die "Cannot open $filename: $!";
    while (my $line = <$fh>) {
        push @lines, $line;
    }
    close $fh;

    return @lines;
}

sub write_file_lines {
    my ($filename, @lines) = @_;

    open my $fh, '>', $filename or die "Cannot write to $filename: $!";
    foreach my $line (@lines) {
        print $fh $line;
    }
    close $fh;
}

sub interactive_mode {
    show_menu();

    while (1) {
        print "snippet> ";
        my $input = <STDIN>;
        chomp $input;

        my @parts = split /\s+/, $input, 4;
        my $command = lc($parts[0] // '');

        if ($command eq 'quit' || $command eq 'exit' || $command eq 'q') {
            print "Goodbye!\n";
            last;
        }
        elsif ($command eq 'list' || $command eq 'menu') {
            show_menu();
        }
        elsif ($command eq 'show') {
            my $snippet_name = $parts[1] // '';
            if ($snippet_name) {
                show_snippet($snippet_name);
            } else {
                print "Usage: show <snippet_name>\n\n";
            }
        }

        elsif ($command eq 'help' || $command eq 'h') {
            show_menu();
        }
        elsif ($command ne '') {
            print "Unknown command: $command\n";
            print "Type 'help' for available commands.\n\n";
        }
    }
}

# Main execution
if (@ARGV == 0) {
    # Interactive mode
    interactive_mode();
} elsif (@ARGV >= 2) {
    # Command line mode
    my $command = lc($ARGV[0]);

    if ($command eq 'insert' && @ARGV >= 3) {
        my $filename = $ARGV[1];
        my $snippet_name = $ARGV[2];
        my $line_number = $ARGV[3] // '';
        insert_snippet($filename, $snippet_name, $line_number);
    }
    elsif ($command eq 'show') {
        show_snippet($ARGV[1]);
    }
    elsif ($command eq 'list') {
        show_menu();
    }
    else {
        print "Usage:\n";
        print "  perl simple_snippet.pl";
        print "  perl simple_snippet.pl show <snippet>\n";
        print "  perl simple_snippet.pl list\n";
    }
} else {
    print "Use 'perl simple_snippet.pl' for interactive mode\n";
}
