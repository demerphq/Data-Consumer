package Data::Consumer::Mysql;

use warnings;
use strict;
use DBI;
use Carp qw(confess);
use warnings FATAL => 'all';
use base 'Data::Consumer';

BEGIN {
    __PACKAGE__->register();
}

=head1 NAME

Data::Consumer - The great new Data::Consumer!

=head1 VERSION

Version 0.01

=cut

our $VERSION = '0.01';


=head1 SYNOPSIS

Quick summary of what the module does.

Perhaps a little code snippet.

    use Data::Consumer;

    my $foo = Data::Consumer->new();
    ...

=head1 EXPORT

A list of functions that can be exported.  You can delete this section
if you don't export anything, such as for a purely object-oriented module.

=head1 FUNCTIONS

=head2 new

Must be overriden

=cut

sub new {
    my ($class, %opts)=@_;
    my $self = $class->SUPER::new(); # let Data::Consumer bless the hash
    
    if (!$opts{dbh} and $opts{connect}) {
        $opts{dbh} = DBI->connect( @{$opts{connect}} ) 
            or confess "Could not connect to database '$opts{connect}[0]' as '$opts{user}[1]': $DBI::errstr\n";
    }
    $opts{dbh} 
        or confess "Must have a database handle!";
    $opts{dbh}->isa('DBI::db') 
        or die "First argument must be a DBI handle! $opts{dbh}\n";

    $self->{dbh} = $opts{dbh}; 

    $opts{id_field}   ||= 'id';
    $opts{flag_field} ||= 'process_state';
    $opts{unprocessed} =0 unless defined $opts{unprocessed};
    $opts{init_id}    = 0 unless defined $opts{init_id};
    $opts{lock_prefix} ||= $0;
    
    $opts{select_sql} ||= do {
        local $_ = '
	    SELECT 
            $id_field
            FROM $table 
	    WHERE
	    $flag_field = ?
	    AND GET_LOCK( CONCAT_WS("=", ?, $id_field ), 0) != 0 
            AND $id_field > ?
	    LIMIT 1
        ';
        s/^\s+//mg;
        s/\$(\w+)/$opts{$1} || confess "Option $1 is mandatory"/ge;
        $_;
    };

    $opts{update_sql} ||= do {
        local $_ = '
	    UPDATE $table 
	    SET $flag_field = ?
	    WHERE
	    $id_field = ?
        ';
        s/^\s+//mg;
        s/\$(\w+)/$opts{$1} || confess "Option $1 is mandatory"/ge;
        $_;
    };
    $opts{release_sql} ||= do {
        local $_ = '
            SELECT RELEASE_LOCK( CONCAT_WS("=", ?, ? ) ) 
        ';
        s/^\s+//mg;
        s/\$(\w+)/$opts{$1} || confess "Option $1 is mandatory"/ge;
        $_;
    };

    %$self=%opts;
    return $self
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
    $self->{last_id} = $self->{init_id};
    return $self;
}


sub acquire { 
    my $self = shift;
    my $dbh = $self->{dbh};

    $self->reset if !defined $self->{last_id};

    my ($id) = $dbh->selectrow_array($self->{select_sql}, undef, 
        $self->{unprocessed}, $self->{lock_prefix}, $self->{last_id});
    if (defined $id) {
        $self->{last_lock} = $id;
        $self->debug_warn(5,"acquired '$id'");
    } else {
        $self->debug_warn(5,"acquire failed -- resource has been exhausted");
    }
    
    $self->{last_id} = $id;

    return $id;
}

sub release {
    my $self = shift;
    
    return 0 unless exists $self->{last_lock};

    my $res = $self->{dbh}->do($self->{release_sql},undef,$self->{lock_prefix},$self->{last_lock});
    defined $res or 
        $self->error("Failed to execute '$self->{release_sql}' with args '$self->{last_lock}': " . $self->{dbh}->errstr());

    $self->debug_warn(5,"release lock '$self->{last_lock}' status: $res"); # XXX
    delete $self->{last_lock};
    return 1;
}

sub _mark_as {
    my ($self, $key,$id)=@_;

    if ($self->{$key}) {
        $self->debug_warn(5,"marking '$id' as '$key'");
        my $res = $self->{dbh}->do($self->{update_sql},undef,$self->{$key},$id)
            or $self->error("Failed to execute '$self->{update_sql}' with args '$self->{$key}','$id': " . 
                    $self->{dbh}->errstr());
        0+$res or $self->error("Update resulted in 0 records changing!");
        
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
