#!/usr/bin/env perl

## usage:
# perl mysql2csv_by_range.pl -d my_db -u root -p xxx -t my_table -o my_csv.csv

use strict;
use warnings;
use DBI;
use Getopt::Long;
use Text::CSV_XS;

$| = 1;

my ($database, $host, $port, $user, $pw, $table, $id_col, $outname, $range);
Getopt::Long::GetOptions(
    'h=s'     => \$host,
    'port=i'  => \$port,
    'd=s'     => \$database,
    'u=s'     => \$user,
    'p=s'     => \$pw,
    't=s'     => \$table,
    'col=s'   => \$id_col,
    'o=s'     => \$outname,
    'range=i' => \$range,
);

die "d|t|o is required\n" unless $database and $table and $outname;

$id_col ||= 'id';
$range  ||= 10000;

my $dsn = "dbi:mysql:db=$database";
$dsn .= ";=host=$host" if $host;
$dsn .= ";=port=$port" if $port;

my $dbh = DBI->connect( $dsn, $user, $pw, { AutoCommit => 0, RaiseError => 1 } )
  or die "Unable to connect to mysql DB_NAME on host $host: $DBI::errstr\n";

my ($min_id) = $dbh->selectrow_array("SELECT MIN($id_col) FROM $table");
my ($max_id) = $dbh->selectrow_array("SELECT MAX($id_col) FROM $table");

my @cols;
my $col_sth = $dbh->prepare("SHOW columns FROM $table");
$col_sth->execute();
while (my $r = $col_sth->fetchrow_hashref) {
    push @cols, $r->{'Field'};
}

my $csv = Text::CSV_XS->new({ binary => 1 }) or
    die "Cannot use CSV: " . Text::CSV_XS->error_diag();
open(my $fh, '>', $outname) or die "Can't write $outname: $!\n";

$csv->print($fh, \@cols); print $fh "\n";

my $sth = $dbh->prepare("SELECT " . join(',', @cols) . " FROM $table WHERE $id_col >= ? AND $id_col < ? ORDER BY $id_col");
while (1) {
    last if $min_id > $max_id;

    print "# on $min_id\n";
    $sth->execute($min_id, $min_id + $range);
    while (my $r = $sth->fetchrow_arrayref) {
        $csv->print($fh, $r); print $fh "\n";
    }

    $min_id += $range;
}

close($fh);
$sth->finish;
$dbh->disconnect;
