package Data::Consumer::MySQL2;

use warnings;
use strict;
use DBI;
use Carp qw(confess);
use warnings FATAL => 'all';
use base 'Data::Consumer';
use vars qw/$Debug $VERSION $Cmd $Fail/;

# This code was formatted with the following perltidy options:
# -ple -ce -bbb -bbc -bbs -nolq -l=100 -noll -nola -nwls='=' -isbc -nolc -otr -kis
# If you patch it please use the same options for your patch.

*Debug= *Data::Consumer::Debug;
*Cmd= *Data::Consumer::Cmd;
*Fail= *Data::Consumer::Fail;

BEGIN {
    __PACKAGE__->register();
}

=head1 NAME

Data::Consumer::MySQL2 - Data::Consumer implementation for a mysql database table resource

=head1 VERSION

Version 0.16

=cut

$VERSION= '0.16';

=head1 SYNOPSIS

    use Data::Consumer::MySQL2;

    my $consumer = Data::Consumer::MySQL2->new(
        dbh => $dbh,
        table => 'T',
        id_field= > 'id',
        flag_field => 'done',
        unprocessed => 0,
        working => 1,
        processed => 2,
        failed => 3,
    );

    $consumer->consume( sub {
        my $id = shift;
        print "processed $id\n";
    } );


=head1 FUNCTIONS

=head2 CLASS->new(%opts)

Constructor for a L<Data::Consumer::MySQL2> instance.

Options are as follows:

=over 4

=item connect => \@connect_args

Will use C<@connect_args> to connect to the database using
C<DBI->connect()>.  This argument is mandatory if the C<dbh> argument is
not provided.

=item dbh => $dbh

Use C<$dbh> as the database connection object. If this argument is
provided then connect will be ignored.

=item table => 'some_table_name'

Process records in the specified table.

=item id_field => 'id'

The column name of the primary key of the table being processed

=item flag_field => 'process_state'

The column name in the table being processed which shows whether
an object is processed or not.

=item unprocessed => 0

The value of the C<flag_field> which indicates that an item is not
processed. If not provided defaults to C<0>.

=item working => 1

The value of the C<flag_field> which indicates that an item is currently
being processed. If not provided defaults to C<1>.

=item processed => 2

The value of the C<flag_field> which indicates that an item has been
successfully processed. If not provided defaults to C<2>.

=item failed => 3

The value of the C<flag_field> which indicates that processing of an
item has failed. If not provided defaults to C<3>.

=item init_id => 0

The value which the first acquired record's C<id_field> must be greater
than. Should be smaller than any legal id in the table. Defaults to C<0>.

=item select_sql

=item select_args

These arguments are optional, and will be synthesized from the other values if not provided.

SQL select query which can be executed to acquire an item to be processed. Should
return a single record with a single column contain the id to be processed, at the
same time it should ensure that a lock on the id is created.

The query will be executed with the id of the last processed item, followed by the arguments
provided by the C<select_args> property.

=item acquire_sql

=item acquire_args

These arguments are optional, and will be synthesized from the other values if
not provided.

SQL update query that claims item in the table. If number of rows updated by
this query is 0, item will be skipped.

The query will be executed with the id of the item returned by select_sql,
followed by the arguments provided by the C<acquire_args> property.

=item update_sql

=item update_args

These arguments are optional, and will be synthesized from the other values if not provided.

SQL update query which can be used to change the status the record being processed.

Will be executed with the arguments provided in update_args followed the new status,
and the id.

=item release_sql

=item release_args

These arguments are optional.

SQL select query which can be used to clear the currently held lock.

Will be called with the arguments provided in release_args, plust the id.

=back

=cut

sub new {
    my ( $class, %opts )= @_;
    my $self= $class->SUPER::new();    # let Data::Consumer bless the hash

    my @bad;
    foreach my $opt (qw(unprocessed processed working failed)) {
        if (ref $opts{$opt}) {
            push @bad, "option '$opt' is not allowed to be a ref in DC::MySQL2";
        } elsif (!defined $opts{$opt}) {
            push @bad, "option '$opt' is not allowed to be missing or undefined in DC::MySQL2";
        }
    }
    if (@bad) {
        confess "Bad option in $class->new(): " . join "\n", @bad;
    }

    if ( !$opts{dbh} and $opts{connect} ) {
        $opts{dbh}= DBI->connect( @{ $opts{connect} } )
          or confess
          "Could not connect to database '$opts{connect}[0]' as '$opts{user}[1]': $DBI::errstr\n";
    }
    $opts{dbh}
      or confess "Must have a database handle!";
    $opts{dbh}->isa('DBI::db')
      or die "First argument must be a DBI handle! $opts{dbh}\n";

    $self->{dbh}= $opts{dbh};

    $opts{id_field}   ||= 'id';
    $opts{flag_field} ||= 'process_state';
    $opts{init_id}= 0 unless exists $opts{init_id};

    unless ( $opts{select_sql} ) {
        $opts{select_sql}= do {
            local $_= '
                SELECT
                $id_field
                FROM $table
                WHERE
                $id_field > ?
                AND $flag_field = ?
                LIMIT 1
            ';
            s/^\s+//mg;
            s/\$(\w+)/$opts{$1} || confess "Option $1 is mandatory"/ge;
            $_;
        };
        $opts{select_args}= [ $opts{unprocessed} ];
    }

    unless ( $opts{acquire_sql} ) {
        $opts{acquire_sql}= do {
            local $_= '
                UPDATE $table
                SET $flag_field = ?
                WHERE
                $flag_field = ?
                AND $id_field = ?
            ';
            s/^\s+//mg;
            s/\$(\w+)/$opts{$1} || confess "Option $1 is mandatory"/ge;
            $_;
        };
        $opts{acquire_args}= [ $opts{working}, $opts{unprocessed} ];
    }

    $opts{update_sql} ||= do {
        local $_= '
            UPDATE $table
            SET $flag_field = ?
            WHERE
            $id_field = ?
        ';
        s/^\s+//mg;
        s/\$(\w+)/$opts{$1} || confess "Option $1 is mandatory"/ge;
        $_;
    };

    %$self= %opts;

    return $self;
}

=head2  $object->reset()

Reset the state of the object.

=head2 $object->acquire()

Aquire an item to be processed.

Returns an identifier to be used to identify the item acquired.

=head2 $object->release()

Release any locks on the currently held item.

Normally there is no need to call this directly.

=cut

sub reset {
    my $self= shift;
    $self->debug_warn( 5, "reset" );
    $self->release();
    $self->{last_id}= $self->{init_id};
    return $self;
}

sub _do_callback {
    my ( $self, $callback )= @_;
    if ( eval { $callback->( $self, @{$self}{qw(last_id dbh)} ); 1; } ) {
        return;
    } else {
        return "Callback failed: $@";
    }
}

sub acquire {
    my $self= shift;
    my $dbh= $self->{dbh};

    $self->reset if !defined $self->{last_id};
    while (1) {
        $self->debug_warn( 5, "last_id was $self->{last_id}");
        my ($id)= $dbh->selectrow_array( $self->{select_sql}, undef, $self->{last_id}, @{ $self->{select_args} || [] } );
        if ( defined $id ) {
            next if $self->is_ignored($id);
            my $claimed = $dbh->do( $self->{acquire_sql}, undef, @{ $self->{acquire_args} || [] }, $id );
            if ($claimed != 1) {
                $self->debug_warn(5, "failed to claim '$id', acquire_sql updated $claimed rows");
                next;
            }
            $self->debug_warn( 5, "acquired '$id'" );
        } else {
            $self->debug_warn( 5, "acquire failed -- resource has been exhausted" );
        }
        $self->{last_id}= $id;
        last;
    }
    return $self->{last_id};
}


sub release {
    my $self= shift;

    return 0 unless exists $self->{last_id} && defined $self->{release_sql};

    my $res=
      $self->{dbh}
      ->do( $self->{release_sql}, undef, @{ $self->{release_args} || [] }, $self->{last_lock} );
    defined $res
      or $self->error( "Failed to execute '$self->{release_sql}' with args '$self->{last_lock}': "
          . $self->{dbh}->errstr() );

    $self->debug_warn( 5, "release lock '$self->{last_lock}' status: $res" );    # XXX
    delete $self->{last_lock};
    return 1;
}

sub _mark_as {
    my ( $self, $key, $id )= @_;
    $self->debug_warn(5, "$key => $id");
    if ( defined $self->{$key} ) {
        $self->debug_warn( 5, "marking '$id' as '$key' ($self->{$key})" );
        my $res= $self->{dbh}->do( $self->{update_sql}, undef, @{ $self->{update_args} || [] }, $self->{$key}, $id )
          or
          $self->error( "Failed to execute '$self->{update_sql}' with args '$self->{$key}','$id': "
              . $self->{dbh}->errstr() );
        0 + $res or $self->error("Update resulted in 0 records changing!");
        $self->debug_warn( 5, "result: $res");

    }
}

=head2 $object->dbh()

returns the database handle the object is using to communicate to the db with.

=cut

sub dbh { $_[0]->{dbh} }

sub DESTROY {
    my $self= shift;
    $self->release() if $self;
}

=head1 AUTHOR

Yves Orton, C<< <YVES at cpan.org> >>

=head1 BUGS

Please report any bugs or feature requests to
C<bug-data-consumer at rt.cpan.org>, or through the web interface at
L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Data-Consumer>.

I will be notified, and then you'll automatically be notified of progress on
your bug as I make changes.

=head1 ACKNOWLEDGEMENTS

Igor Sutton <IZUT@cpan.org> for ideas, testing and support.

=head1 COPYRIGHT & LICENSE

Copyright 2008, 2010, 2011 Yves Orton, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

=cut

1;    # End of Data::Consumer::MySQL2

