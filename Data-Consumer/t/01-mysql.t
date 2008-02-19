#!perl -T
use Data::Consumer::Mysql;
use strict;
use warnings;
use DBI;

my $table = 'TEMP_DATA_CONSUMER_TEST_TABLE';

my $drop = <<"ENDOFSQL";
DROP TABLE `$table`
ENDOFSQL

my $create = <<"ENDOFSQL";
CREATE TABLE `$table` ( 
    `id` int(11) NOT NULL auto_increment, 
    `n` int(11) NOT NULL default '0', 
    `done` tinyint(3) unsigned NOT NULL default '0', 
    PRIMARY KEY  (`id`) 
)
ENDOFSQL

# 100 rows
my $insert = <<"ENDOFSQL";
INSERT INTO `$table` (done) VALUES 
	(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),
        (0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),
        (0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),
        (0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),
        (0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),
        (0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),
        (0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),
        (0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),
        (0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),
        (0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0)
ENDOFSQL

my @connect = ("DBI:mysql:dev", 'test', 'test');

INIT:{
    my $dbh = DBI->connect(@connect) 
	or die "Could not connect to database: $DBI::errstr";
    local $dbh->{PrintError};
    local $dbh->{PrintWarn};
    local $dbh->{RaiseError} = 0;
    $dbh->do($drop);
    $dbh->{RaiseError} = 1;
    $dbh->do($create);
    $dbh->do($insert);
        
}
my $debug=0;

my $child;
my $procs = 4;
$debug  and warn "$$ Spawning children!\n";
my $pid=$$;
do {
    $child = fork;
    die "Fork failed!" if !defined $child;
    $debug  and warn "$$: Spawned child process $child\n" if $child;
} while $child and --$procs > 0;

if ( $child ) {
    $debug  and warn("### Using test more $$\n");
    eval 'use Test::More tests => 2; ok(1); 1;' 
        or die $@;
} else {
   sleep(1);
}

$child and warn "\nThis will take around 30 seconds\n";
$debug and warn "$$: starting processing\n";
$Data::Consumer::Debug=5 if $debug;
my $consumer = Data::Consumer::Mysql->new(
    connect => \@connect,
    table => $table, 
    flag_field => 'done', 
    unprocessed => 0, 
    working => 1,
    processed => 2,
    failed => 3,
);

$consumer->consume(sub { 
    my ($id,$consumer) = @_; 
    $debug  and warn("$$: Processing $id"); 
    sleep(1);
    $consumer->dbh->do("UPDATE `$table` SET `n` = `n` + 1 WHERE `id` = ?",undef,$id);
});


if ( $child ) {
    sleep(1);
    my $recs = $consumer->dbh->selectall_arrayref("SELECT * FROM `$table` WHERE `n` != 1");
    use Data::Dumper;    
    my $num = 0 + @$recs;
    is($num,0) or warn Dumper($recs);
    $debug  and warn("### $$ Found $num multiple processed items.\n");
} else {
    undef $consumer;
}

