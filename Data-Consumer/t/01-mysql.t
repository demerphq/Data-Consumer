#!perl -T

use Test::More tests => 2;

BEGIN {
	use_ok( 'Data::Consumer::Mysql' );
}
use strict;
use warnings;
my $child;
my $procs=4;
warn "$$ Spawning children!\n";
my $pid=$$;
do {
    $child = fork;
    die "Fork failed!" if !defined $child;
    warn "$$: Spawned child process $child\n" if $child;
} while $child and --$procs > 0;

warn "$$: starting processing\n";
#exit;
my $dbh = DBI->connect(
    'DBI:mysql:dev', 'test', 'test'
) || die "Could not connect to database: $DBI::errstr";
my $count=0;
my $num=0;
while ( !$num and $count++ < 2 ) {
    my $lock;
    ($num, $lock)= $dbh->selectrow_array('select count(*),GET_LOCK(?,0) from T where done=0',undef,$0);
    if (!$num and $lock) {
	$dbh->do('insert into T (done) values 
	(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),
	(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),
	(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),
	(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),
	(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0),(0)
	');
    } elsif (!$num) {
        sleep(2);
    }
}
    
my $consumer = Data::Consumer::Mysql->new(
    dbh => $dbh,
    table => 'T', 
    flag_field => 'done', 
    unprocessed => 0, 
    working => 1,
    processed => 2,
    failed => 3,
);

$consumer->consume(sub { 
    my $id = shift; 
    warn("$$: Processing $id"); 
    sleep(1+rand(2));
    $dbh->do('update T set n=n+1 where id=?',undef,$id);
});
if (!$child){
    ($num)= $dbh->selectrow_array('select count(*) from T where n>1');
    is($num,0);
}
print("### Found $num multiple processed items.");
