##!perl -T
use Data::Consumer::MySQL;
use strict;
use warnings;
use DBI;
my $debug = @ARGV ? shift : $ENV{TEST_DEBUG};
our @fake_error;
our @expect_fail;
our %process_state;
our @connect_args;
our $table;

my $conf_file = 'mysql.pldat';
use Cwd;
warn cwd;
if (-e $conf_file) {
    # eval @connect_args into existance
    my $ok = do $conf_file;
    defined $ok or die "Error loading $conf_file: ", $@||$!;

    unless (@connect_args) {
        my $reason='no mysql connection details available';
        eval 'use Test::More skip_all => ; 1;'
            or die $@;
    }
}
if (!%process_state) {
    %process_state = (
	unprocessed => 0,
	working     => 1,
	processed   => 2,
	failed      => 3,
    );
}

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
        (0),(0),(0),(0),(0),(0),(0),(0),(0),(0),
        (0),(0),(0),(0),(0),(0),(0),(0),(0),(0),
        (0),(0),(0),(0),(0),(0),(0),(0),(0),(0),
        (0),(0),(0),(0),(0),(0),(0),(0),(0),(0),
        (0),(0),(0),(0),(0),(0),(0),(0),(0),(0)
ENDOFSQL

$insert.=",($_)" for @fake_error; 

$connect_args[0]=("DBI:mysql:$connect_args[0]");

{
    my $dbh = DBI->connect(@connect_args) 
	or die "Could not connect to '$connect_args[0]' : $DBI::errstr";
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
    eval "use Test::More tests => @{[2+@expect_fail]}; ok(1); 1;" 
        or die $@;
} else {
   sleep(1);
}

$child and diag("This will take around 30 seconds\n");
$debug and Data::Consumer->debug_warn(0,"starting processing\n");
$Data::Consumer::Debug=5 if $debug;

my $consumer = Data::Consumer::MySQL->new(
    connect     => \@connect_args,
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
    my $expect = 0+@expect_fail;
    $debug and $consumer->debug_warn($expect,"Found $num incorrectly processed items expected $expect.\n");
    my $ok=!!is($num, $expect, "should be $expect incorrectly processed items");
    foreach my $idx (0..$#expect_fail) {
        $ok+=!!is("@{$recs->[$idx]}","@{$expect_fail[$idx]}");
    }
    $ok or do { warn map {  "[@{$recs->[$_]}] " . ( 7 == $_ % 8 ? "\n" : "" ) } (0..$#$recs)  };
} else {
    undef $consumer;
}

1;
