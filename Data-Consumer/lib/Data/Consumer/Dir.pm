package Data::Consumer::Dir;

use warnings;
use strict;
use DBI;
use Carp qw(confess);
use warnings FATAL => 'all';
use base 'Data::Consumer';
use File::Spec;
use File::Path;
use Fcntl ’:flock’; # import LOCK_* constants

BEGIN {
    __PACKAGE__->register();
}

=head1 NAME

Data::Consumer::Mysql - Data::Consumer implementation for a directory of files resource

=head1 VERSION

Version 0.01

=cut

our $VERSION = '0.01';


=head1 SYNOPSIS

    use Data::Consumer::Dir;
    my $consumer = Data::Consumer::dir->new(
	dbh => $dbh,
	table => 'T', 
        id_field= > 'id',
	flag_field => 'done', 
	unprocessed => 0, 
	working => 1,
	processed => 2,
	failed => 3,
    );
    $consumer->consume(sub {
        my $id = shift;
        print "processed $id\n";
    });


=head1 FUNCTIONS

=head2 new

Constructor for a Data::Consumer::Mysql instance.

Either the 'root' option must be provided or both 'unprocessed' and 'processed'
arguments must be defined. Will die if the directories do not exist unless the
'create' is defined.

=over 4 

=item unprocessed => $path_spec

Directory within which unprocessed files will be found.

=item working => $path_spec

Files will be moved to this directory prior to be processed.

=item processed => $path_spec

Once sucessfully processed the files will be moved to this directory.

=item failed => $path_spec

If processing fails then the files will be moved to this directory.

=item root => $path_spec

Automatically creates any of the unprocessed, working, processed, or failed
directories below a specified root. Only those directories not explicitly 
defined will be automatically created so this can be used in conjunction
with the other options

=item create => $bool

=item create_mode => $bool

If true then directories specified by not existing will be created. 
If create_mode is specified then the directories will be created with that mode.

=back

=cut
{ 
my @keys=qw(unprocessed working processed failed);
sub new {
    my ($class, %opts)=@_;
    my $self = $class->SUPER::new(); # let Data::Consumer bless the hash

    if ($opts{root}) {
        my ($v,$p)= File::Spec->splitpath($opts{root},'nofile');
        for my $type (@keys) {
            $opts{$type} ||= File::Spec->catpath($v,File::Spec->catdir($p,$type));
        }
    }
    ($opts{unprocessed} and $opts{processed}) or 
        confess "Arguments 'unprocessed' and 'processed' are mandatory";
    
    if ($opts{create}) {
        for (@keys) {
            next unless exists $opts{$_};
            next if -d $opts{$_};
            mkpath($opts{$_}, $Data::Consumer::Debug, $opts{create_mode} || ());
        }
    }

    %$self = %opts;
    return $self;
}
}


=head2  $object->reset()

Reset the state of the object.

=head2 $object->acquire()

Aquire an item to be processed. 

returns an identifier to be used to identify the item acquired.

=head2 $object->release()

Release any locks on the currently held item.

Normally there is no need to call this directly. 

=cut


sub reset { 
    my $self = shift;
    $self->debug_warn(5,"reset");
    $self->release();
    opendir my $dh, $self->{unprocessed} 
        or die "Failed to opendir '$self->{unprocessed}': $!"
    my @files = sort grep { -f $_ } readdir($dh);
    $self->{files}=\@files;
    return $self;
}

sub _cf { # cat file
    my ($r,$f) =@_;

    my ($v,$p)= File::Spec->splitpath($r,'nofile');
    return File::Spec->catpath($v,$d,$f);
}

sub acquire { 
    my $self = shift;
    my $dbh = $self->{dbh};

    $self->reset if !@{ $self->{files} || [] };

    my $files = $self->{files};
    while (@$files) {
        my $file = shift @$files;
        my $spec = _cf($self->{unprocessed},$file);
        my $fh;
        if (open $fh,"<",$spec and flock($fh,LOCK_EX,LOCK_NB)) {
            $self->{lock_fh} = $fh;
            $self->{lock_spec} = $spec;
            $self->debug_warn(5,"acquired '$id'");
            $self->{last_id} = $file;
            return $file;
        }
    }
    $self->debug_warn(5,"acquire failed -- resource has been exhausted");
    return
}

sub release {
    my $self = shift;
    
    flock($self->{lock_fh},LOCK_UN) if $self->{lock_fh};
    delete $self->{lock_fh};
    delete $self->{lock_spec};
    delete $self->{last_id};
    return 1;
}

sub _mark_as {
    my ($self, $key,$id)=@_;

    if ($self->{$key}) {
        
    }
}

=head2 $object->dbh

returns the database handle the object is using to communicate to the db with.

=cut

sub dbh { $_[0]->{dbh} }

sub DESTROY {
    my $self = shift;
    $self->release() if $self
}


=head1 AUTHOR

Yves Orton, C<< <YVES at cpan.org> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-data-consumer at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Data-Consumer>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.


=head1 ACKNOWLEDGEMENTS


=head1 COPYRIGHT & LICENSE

Copyright 2008 Yves Orton, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

=cut

1; # End of Data::Consumer::Mysql
