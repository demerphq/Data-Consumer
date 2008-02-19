#!perl -T
use Data::Consumer::Mysql;
use strict;
use warnings;
use DBI;

my $debug = 0;
#exit;
our %process_state;
if (!%process_state) {
    %process_state = (
	unprocessed => 0,
	working     => 1,
	processed   => 2,
	failed      => 3,
    );
}

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


my $child;
my $procs = 4;
$debug  and Data::Consumer->debug_warn("Spawning children!\n");
my $pid = $$;
my @child;
do {
    $child = fork;
    if (!defined $child) {
        die "Fork failed!";
    } elsif ($child) {
        push @child,$child;
    }
} while $child and --$procs > 0;

if ( $child ) {
    $debug  and $debug and Data::Consumer->debug_warn("Using test more\n");
    eval 'use Test::More tests => 2; ok(1); 1;' 
        or die $@;
} else {
   sleep(1);
}

$child and warn "\nThis will take around 30 seconds\n";
$debug and Data::Consumer->debug_warn(0,"starting processing\n");
$Data::Consumer::Debug=5 if $debug;

my $consumer = Data::Consumer::Mysql->new(
    connect     => \@connect,
    table       => $table,
    flag_field  => 'done',
    %process_state,
);

$consumer->consume(sub { 
    my ($consumer,$id,$dbh) = @_; 
    $debug  and $consumer->debug_warn(0,"*** processing '$id'"); 
    sleep(1);
    $dbh->do("UPDATE `$table` SET `n` = `n` + 1 WHERE `id` = ?", undef, $id);
});


if ( $child ) {
    use POSIX ":sys_wait_h";
    while (@child) {
        @child=grep { waitpid($_,WNOHANG)==0 } @child;
        sleep(1);
    }
        
    my $recs = $consumer->dbh->selectall_arrayref(
        "SELECT * FROM `$table` WHERE NOT(`n` = ? AND `done` = ?)",
        undef, 1, $process_state{processed},
    );
    my $num = 0 + @$recs;
    $debug and $consumer->debug_warn(0,"Found $num incorrectly processed items.\n");
    is($num, 0, 'should be 0 incorrectly processed items')
        or do { warn map {  "[@{$recs->[$_]}] " . ( 7 == $_ % 8 ? "\n" : "" ) } (0..$#$recs)  };
} else {
    undef $consumer;
}

1;
