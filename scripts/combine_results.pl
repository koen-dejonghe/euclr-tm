#!/usr/bin/perl

use strict;
use warnings FATAL => 'all';

use Data::Dumper;
use Text::CSV;

my $csv = Text::CSV->new({ sep_char => ',' });

my $folder = $ARGV[0];
$folder =~ s/\/$//;

my @h = ("ISIN","ISSUER.NAME","GA.NAME","OPS.CURR","ZCP.FL","ROC","MIN.TRAD.AMT","MLT.TRAD.AMT");

opendir(my $dh, $folder) || die "Can't opendir $folder: $!";
my @part_files = grep { /^part/ && -f "$folder/$_" } readdir($dh);
closedir $dh;

my %content = ();
foreach my $part(@part_files) {
    my $file = "$folder/$part";

    open F, $file or die;
    while(<F>){
        chomp;
        my $line = $_;
        if ($csv->parse($line)) {
            my ($isin, $label) = $csv->fields();
            $content{$isin} = $label;
        }
    }
    close F;
}

my $fcsv = $folder;
$fcsv =~ s/\//_/g;
open F, ">$folder/$fcsv.csv" or die;
print F join (",", map { qq("$_") } @h);
print F "\n";
foreach my $isin(sort keys %content){
    my $v = $content{$isin};
    print F qq("$isin");
    print F ",";
    print F qq("$v");
    print F ",,,,,0,0\n";
}
close F;
