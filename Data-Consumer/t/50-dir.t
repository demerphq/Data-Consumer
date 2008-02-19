#!perl -T
use Data::Consumer::Dir;
use strict;
use warnings;
use DBI;

my $debug = 1;

our %process_state;
if (!%process_state) {
    %process_state = (
        root => 't/dir-test',
        create => 1,   
    );
}

mkdir 't/dir-test' and mkdir 't/dir-test/working' if !-d 't/dir-test/working';
for (1..50) {
    open my $fh,">","t/dir-test/$_" 
        or die "failed to create test file t/dir-test/$_:$!";
    print $fh $_;
    close $fh;
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

my $consumer = Data::Consumer::Dir->new(
    %process_state,
);

$consumer->consume(sub { 
    my ($id,$consumer) = @_; 
    $debug  and $consumer->debug_warn(0,"*** processing '$id'"); 
    sleep(1);
});


if ( $child ) {
    use POSIX ":sys_wait_h";
    while (@child) {
        @child=grep { waitpid($_,WNOHANG)==0 } @child;
        sleep(1);
    }
        
} else {
    undef $consumer;
}

1;
