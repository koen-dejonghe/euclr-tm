#!/usr/bin/perl -w

use strict;

my $fname = $ARGV[0];

open my $fh, '<', $fname or die;
$/ = undef;
my $data = <$fh>;
close $fh;

my @c = $data =~ /\W/g;
my @d = $data =~ /./g;

my $q = @c / @d;

print ( "$fname\n") if ($q > .9);

