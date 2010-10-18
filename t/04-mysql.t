use strict;
use warnings;
use Cwd;
our %process_state = (
    unprocessed => 0,
    processed   => 2,
);
%process_state = %process_state; # silence warnings on 5.6.2

my $file='t/01-mysql.t';
my $res = do $file;
if (!defined $res) {
    die "Error executing '$file': ",$@||$!,"\nCwd=". cwd(),"\n";
}


