use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME                => 'Data::Consumer',
    AUTHOR              => 'Yves Orton <YVES@cpan.org>',
    VERSION_FROM        => 'lib/Data/Consumer.pm',
    ABSTRACT_FROM       => 'lib/Data/Consumer.pm',
    PL_FILES            => {},
    PREREQ_PM => {
        'Test::More' => 0,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'Data-Consumer-*' },
);
