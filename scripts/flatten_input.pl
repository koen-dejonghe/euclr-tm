#!/usr/bin/perl
#
use strict;
use warnings;
use Data::Dumper;
use Text::CSV;

my $csv = Text::CSV->new({ sep_char => ',' });

my %labels_by_isin = ();
my $first = 0;
my $fh;

#########################################################
# ISIN
$first = 0;
# ISIN,ISSUER.NAME,ZCP.FL,MIN.TRAD.AMT,MLT.TRAD.AMT,OPS.CURR
open ($fh, "input/ISIN_train.csv") or die;
while(my $l = <$fh>){
    $first++ or next;
    chomp $l;

    if ($csv->parse($l)) {
        my ($isin, $issuer_name, $zcp_fl, $min_trad_amt, $mlt_trad_amt, $ops_curr) = $csv->fields();
        $issuer_name =~ s/"//g;
        $labels_by_isin{$isin} = {
            issuer_name => $issuer_name,
            zcp_fl => $zcp_fl,
            min_trad_amt => $min_trad_amt,
            mlt_trad_amt => $mlt_trad_amt,
            ops_curr => $ops_curr
        };
    } else {
        warn "Line could not be parsed: $l\n";
    }
}
close $fh;
# print (Dumper(\%labels_by_isin));

#########################################################
# GUARANTOR
$first = 0;
# ISIN,GA.NAME
open ($fh, "input/guarantor_train.csv") or die;
while(my $l = <$fh>){
    $first++ or next;
    chomp $l;
    if ($csv->parse($l)) {
        my ($isin, $ga_name) = $csv->fields();
        $ga_name =~ s/"//g;
        $labels_by_isin{$isin}{ga_name} = $ga_name ;
    } else {
        warn "Line could not be parsed: $l\n";
    }
}
close $fh;
# print (Dumper(\%labels_by_isin));

#########################################################
# ROC
$first = 0;
# ISIN,ROC
open ($fh, "input/ROC_train.csv") or die;
while(my $l = <$fh>){
    $first++ or next;
    chomp $l;
    if ($csv->parse($l)) {
        my ($isin, $roc) = $csv->fields();
        $roc =~ s/"//g;
        $labels_by_isin{$isin}{roc}{$roc}++;
    }
}
close $fh;

#########################################################
# DOCID
open ($fh, "input/docid_train.csv") or die;
while(my $l = <$fh>){
    $first++ or next;
    chomp $l;
    $l =~ s/"//g;
    my ($fid, $isin) = split(",", $l);
    $labels_by_isin{$isin}{fid}{$fid}++;
}
close $fh;
# print (Dumper(\%labels_by_isin));

#########################################################
#
# open($fh, '>:encoding(UTF-8)', "input/labels_text.csv") or die;
open($fh, ">input/labels_text.csv") or die;

# ISIN,ISSUER.NAME,ZCP.FL,MIN.TRAD.AMT,MLT.TRAD.AMT,OPS.CURR,GA.NAME,ROC,TEXT
my $h = "ISIN,ISSUER_NAME,ZCP_FL,MIN_TRAD_AMT,MLT_TRAD_AMT,OPS_CURR,GA_NAME,ROC,TEXT";
my $q_h = join(",", map{ qq("$_") } split(",", $h));
print $fh "$q_h\n";

foreach my $isin (keys %labels_by_isin) {

    # print("processing $isin\n");

    my $record = $labels_by_isin{$isin};

    my @fids = keys($record->{fid});

    my $fname = "";
    foreach my $fid(@fids) {
        if ( -f "input/txt/$fid.txt") {
            $fname = "input/txt/$fid.txt";
            last;
        }
    }

    if (! $fname) {
        next;
    }
    open my $th, '<:utf8', $fname or die;
    $/ = undef;
    my $text = <$th>;
    close $th;

    $text =~ s/[\p{Han}]/ /g;
    $text =~ s/[^\x00-\x7f]/ /g;
    $text =~ s/"/ /g;
    $text =~ s/^\s*$/./g;
    $text =~ s/\n+/ /g;
    $text =~ s/\s+/ /g;

    my $rocs = join("|", sort keys %{$record->{roc}});
    my $q_rocs = qq("$rocs");

    my $q_isin = qq("$isin");
    my $q_issuer = qq("$record->{issuer_name}");
    my $q_zcp_fl = qq("$record->{zcp_fl}");
    my $q_min_trad_amt = $record->{min_trad_amt};
    my $q_mlt_trad_amt = $record->{mlt_trad_amt};
    my $q_ops_curr = qq("$record->{ops_curr}");
    my $q_ga_name = qq("$record->{ga_name}");
    my $q_text = qq("$text");
    my $line = "$q_isin,$q_issuer,$q_zcp_fl,$q_min_trad_amt,$q_mlt_trad_amt,$q_ops_curr,$q_ga_name,$q_rocs,$q_text\n";

    print $fh $line;
}
close $fh;

# test output file
print("testing file...\n");
open($fh, "input/labels_text.csv") or die;
while(my $line = <$fh>) {
    if ($csv->parse($line)) {
        my @fields = $csv->fields();
        if (@fields != 9){
            warn "Line could not be parsed: $line\n";
        }
    } else {
        warn "Line could not be parsed: $line\n";
    }
}
close $fh;

