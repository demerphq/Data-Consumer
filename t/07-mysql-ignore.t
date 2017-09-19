use strict;
use warnings;
use Cwd;
our %process_state = (
    unprocessed => 94,
    processed   => 10,
    working => 123,
    failed => 666,
);

our $object='Data::Consumer';
our @expect_fail=([3,0,94]);
our %ignored=(3=>1);

%ignored = %ignored; #silence warnings on 5.6.2
@expect_fail = @expect_fail; #silence warnings on 5.6.2
$object = $object; #silence warnings on 5.6.2
%process_state = %process_state; # silence warnings on 5.6.2

my $file='t/01-mysql.t';
my $res = do $file;
if (!defined $res) {
    die "Error executing '$file': ",$@||$!,"\nCwd=". cwd(),"\n";
}


