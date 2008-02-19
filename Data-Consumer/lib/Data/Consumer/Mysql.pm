package Data::Consumer::Mysql;

use warnings;
use strict;
use DBI;
use Carp qw(confess);
use warnings FATAL => 'all';
use base 'Data::Consumer';

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
    
    if (!$opts{dbh} and $opts{db_connect}) {
        $opts{dbh} = DBI->connect( @{$self->{connect}} ) 
            or confess "Could not connect to database: $DBI::errstr\n";
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

sub reset { 
    my $self = shift;
    $self->{last_id} = $self->{init_id};
    return $self;
}


sub acquire { 
    my $self = shift;
    my $dbh = $self->{dbh};

    $self->reset if !defined $self->{last_id};

    my ($id) = $dbh->selectrow_array($self->{select_sql}, undef, 
        $self->{unprocessed}, $self->{lock_prefix}, $self->{last_id});

    $self->{last_lock} = $id if defined $id;
    $self->{last_id} = $id;

    return $id;
}

sub last_id {
    my $self=shift;
    return $self->{last_id};
}

sub _update {
    my ($self, $key,$id)=@_;
    $id = $self->last_id if @_<3;
    defined $id 
        or confess "Undefined last_id. Nothing acquired yet?";
    if ($self->{$key}) {
        my $res = $self->{dbh}->do($self->{update_sql},undef,$self->{$key},$id)
            or $self->error("Failed to execute '$self->{update_sql}' with args '$self->{$key}','$id': " . 
                    $self->{dbh}->errstr());
        0+$res or $self->error("Update resulted in 0 records changing!");
    }
}

sub process {
    my $self = shift;
    my $callback = shift;
    my $id = $self->last_id;
    defined $id 
        or $self->error("Undefined last_id. Nothing acquired yet?");
    $self->_update('working');
    if ( eval { $callback->($id);  1; } ) {
        $self->_update('processed');
    } else {
        my $error = "Processing failed: $@";
        $self->_update('failed');
        $self->error($error);
    }
    return 1;
}

sub release {
    my $self = shift;
    
    return 0 unless exists $self->{last_lock};

    my $res = $self->{dbh}->do($self->{release_sql},undef,$self->{lock_prefix},$self->{last_lock});
    defined $res or 
        $self->error("Failed to execute '$self->{release_sql}' with args '$self->{last_lock}': " . $self->{dbh}->errstr());

    warn "$$: release lock '$self->{last_lock}' status: $res\n"; # XXX
    delete $self->{last_lock};
    return 1;
}


sub DESTROY {
    my $self = shift;
    $self->release() if $self
}

#sub _check { confess "abstract method must be overriden by subclass\n"; }
#sub error { confess "abstract method must be overriden by subclass\n"; }



=head2 function2

=head1 AUTHOR

Yves Orton, C<< <YVES at cpan.org> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-data-consumer at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Data-Consumer>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.




=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Data::Consumer


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=Data-Consumer>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/Data-Consumer>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/Data-Consumer>

=item * Search CPAN

L<http://search.cpan.org/dist/Data-Consumer>

=back


=head1 ACKNOWLEDGEMENTS


=head1 COPYRIGHT & LICENSE

Copyright 2008 Yves Orton, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.


=cut

1; # End of Data::Consumer
