#!/usr/bin/perl
use strict;
use warnings FATAL => 'all';
use Data::Dumper;
use Lingua::Sentence;

sub doc_to_line() {
    my ($fname, $out_fh) = @_;

    # assume text is English
    my $splitter = Lingua::Sentence->new("en");

    # slurp file
    open my $th, $fname or die "cannot read $fname";
    $/ = undef;
    my $text = <$th>;
    close $th;

    # remove Chinese and other unreadable stuff
    $text =~ s/[\p{Han}]/ /g;
    $text =~ s/[^\x00-\x7f]/ /g;
    # replace empty lines by a dot
    $text =~ s/^\s*$/./g;
    # replace all whitespace including \n by space
    $text =~ s/\s+/ /g;

    my $sentences = $splitter->split($text);

    print $out_fh $sentences;
}

sub get_file_names() {
    my @dirs = @_;
    my @fnames = ();
    foreach my $some_dir(@dirs) {
        opendir( my $dh, $some_dir ) || die "Can't opendir $some_dir: $!";
        my @f= grep { /.*\.txt/ && -f "$some_dir/$_" } readdir( $dh );
        closedir $dh;
        @f = map { "$some_dir/$_" } @f;
        push (@fnames, @f);
    }
    return @fnames;
}

# all text for w2v training
my @files = &get_file_names("data/provided/txt", "data/provided/unlabeled_txt", "data/provided/submission_txt");
my $num_files = scalar(@files);
print("found $num_files items to process\n");

open(my $fh, ">data/derived/all_text.txt") or die;
my $i = 1;
foreach my $fname(@files) {
    print("processing $i: $fname\n");
    &doc_to_line($fname, $fh);
    $i++;
}
close $fh;

