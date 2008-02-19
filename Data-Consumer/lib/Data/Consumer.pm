package Data::Consumer;

use warnings;
use strict;
use Carp;
use vars qw/$Debug $VERSION $Fail $Cmd/;

# This code was formatted with the following perltidy options:
# -ple -ce -bbb -bbc -bbs -nolq -l=100 -noll -nola -nwls='=' -isbc -nolc -otr -kis
# If you patch it please use the same options for your patch.

=head1 NAME

Data::Consumer - Repeatedly consume a data resource in a robust way

=head1 VERSION

Version 0.08

=cut

$VERSION= '0.08';

=head1 SYNOPSIS

    use Data::Consumer;
    my $consumer = Data::Consumer->new(
        type        => $consumer_name,
        %consumer_args,
        unprocessed => $unprocessed,
        working     => $working,
        processed   => $processed,
        failed      => $failed,
        max_passes  => $num_or_undef,
        max_process => $num_or_undef,
        max_elapsed => $seconds_or_undef,
    );
    $consumer->consume(sub {
        my $id = shift;
        print "processed $id\n";
    });


=head1 METHODS

=head2 CLASS->new(%opts)

Constructor. Normally Data::Consumer's constructor is not
called directly, instead the constructor of a subclass is used.
However to make it easier to have a data driven load process  
Data::Consumer accepts the 'type' argument which should specify the
the short name of the subclass (the part after Data::Consumer::) or
the full name of the subclass.

Thus

    Data::Consumer->new(type=>'MySQL',%args);

is exactly equivalent to calling

    Data::Consumer::MySQL->new(%args);

except that the former will automatically require or use the appropriate module 
and the latter necessitates that you do so yourself.

Every Data::Consumer subclass constructor supports the following arguments on 
top of any that are subclass specific. Additionally some arguments are universally
used, but have different meaning depending on the subclass. 

=over 4

=item unprocessed

How to tell if the item is unprocessed. 

How this argument is interpreted depends on the Data::Consumer subclass involved.

=item working

How to tell if the item is currently being worked on.

How this argument is interpreted depends on the Data::Consumer subclass involved.

=item processed

How to tell if the item has already been worked on.

How this argument is interpreted depends on the Data::Consumer subclass involved.

=item failed

How to tell if processing failed while handling the item.

How this argument is interpreted depends on the Data::Consumer subclass involved.

=item max_passes => $num_or_undef

Normally consume() will loop through the data set until it is exhausted.
By setting this parameter you can control the maximum number of iterations,
for instance setting it to 1 will result in a single pass through the data
per invocation. If 0 (or any other false value) is treated as meaning
"loop until exhausted".

=item max_processed => $num_or_undef

Maximum number of items to process per invocation.

If set to a false value there is no limit.

=item max_failed => $num_or_undef

Maximum number of failed process attempts that may occur before consume will stop.
If set to a false value there is no limit. Setting this to 1 will cause processing
to stop after the first failure.

=item max_elapsed => $seconds_or_undef

Maximum amount of time that may have elapsed when starting a new process. If
more than this value has elapsed then no further processing occurs. If 0 (or
any false value) then there is no time limit.

=item proceed => $code_ref

This is a callback that may be used to control the looping process in consume
via the proceed() method. See the documentation of consume() and proceed()

=item sweep => $bool

If this parameter is true, and there are four modes defined (unprocessed,
working, processed, failed) then consume will perform a "sweep up" after
every pass, which is responsible for moving "abandonded" files from the
working directory (such as from a previous process that segfaulted during
processing). Generally this should not be necessary.

=back


=head2 CLASS->register(@alias)

Used by subclasses to register themselves as a Data::Consumer subclass
and register any additional aliases that the class may be identified as.

Will throw an exception if any of the aliases are already associated to a
different class.

When called on a subclass in list context returns a list of the subclasses
registered aliases,

If called on Data::Consumer in list context returns a list of all alias
class mappings.

=cut

=head2 $class_or_object->debug_warn($level,@debug_lines)

If Debug is enabled and above $level then print @debug_lines to stdout
in a specific format that includes the class name of the caller and process id.

=cut

sub debug_warn {
    my $self= shift;
    my $level= shift;
    if ( $Debug and $Debug >= $level ) {
        warn ref($self) || $self, "\t$$\t>>> $_\n" for @_;
    }
}

BEGIN {
    my %alias2class;
    my %class2alias;
    $Debug and $Debug >= 5 and warn "\n";

    sub register {
        my $class= shift;

        ref $class
          and confess "register() is a class method and cannot be called on an object\n";
        my $pack= __PACKAGE__;

        if ( $class eq $pack ) {
            return wantarray ? %alias2class : 0 + keys %alias2class;
        }

        ( my $std_name= $class ) =~ s/^\Q$pack\E:://;
        $std_name =~ s/::/-/g;

        my @failed;
        for my $name ( $class, $std_name, @_ ) {
            if ( $alias2class{$name} and $alias2class{$name} ne $class ) {
                push @failed, $name;
                next;
            }
            __PACKAGE__->debug_warn( 5, "registered '$name' as an alias of '$class'" );
            $alias2class{$name}= $class;
            $class2alias{$class}{$name}= $class;
        }
        @failed
          and confess "Failed to register aliases for '$class' as they are already used\n",
          join( "\n", map { "\t'$_' is already assigned to '$alias2class{$_}'" } @failed ),
          "\n";
        return wantarray ? %{ $class2alias{$class} } : 0 + keys %{ $class2alias{$class} };
    }

    sub new {
        my ( $class, %opts )= @_;
        ref $class
          and confess "new() is a class method and cannot be called on an object\n";

        if ( $class eq __PACKAGE__ ) {
            my $type= $opts{type}
              or confess "'type' is a mandatory named parameter for $class->new()\n";
            eval "require $class\::$type";
            unless ( $class= $alias2class{$type} ) {
                confess "'type' parameter '$type' is either not installed or incorrect\n";
            }
        }
        my $object= bless {}, $class;
        $class->debug_warn( 5, "created new object '$object'" );
        return $object;
    }
}

=head2 $object->last_id()

Returns the identifier for the last item acquired.

Returns undef if acquire has never been called or if the last
attempt to acquire data failed because none was available.

=cut

sub last_id {
    my $self= shift;
    return $self->{last_id};
}

# Until i figure out to make gedit handle begin/end directives this has to
# stay commented out
#=begin dev
#
#=head2 $object->_mark_as($type,$id)
#
#** Must be overriden **
#
#Mark an item as a particular type if the object defines that type.
#
#This is wrapped by mark_as() for error checking, so you are guaranteed
#that $type will be one of
#
#    'unprocessed', 'working', 'processed', 'failed'
#
#and that $object->{$type} will be true value, and that $id will be from
#the currently acquired item.
#
#=end dev

=head2 $object->mark_as($type)

Mark an item as a particular type if the object defines that type.

Allowed types are 'unprocessed', 'working', 'processed', 'failed'

=cut

sub _mark_as { confess "must be overriden" }

BEGIN {
    my ( %valid, @valid );
    @valid= qw ( unprocessed working processed failed );
    @valid{@valid}= ( 1 .. @valid );

    sub mark_as {
        my $self= shift @_;
        my $key= shift @_;

        $valid{$key}
          or confess "Unknown type in mark_as(), valid options are ",
          join( ", ", map { "'$_'" } @valid ),
          "\n";

        my $id= @_ ? shift @_ : $self->last_id;
        defined $id
          or confess "Nothing acquired to be marked as '$key' in mark_as.\n";

        return unless $self->{$key};
        return $self->_mark_as( $key, $id );
    }
}

=head2 $object->process($callback)

Marks the current item as 'working' and processes it using the $callback.
If the $callback dies then the item is marked as 'failed', otherwise the
item is marked as 'processed' once the $callback returns. The return value
of the $callback is ignored.

$callback will be called with two arguments, the first being the id of the item
being processed, the second being the consumer object itself.

=cut

sub process {
    my $self= shift;
    my $callback= shift;
    my $id= $self->last_id;
    defined $id
      or $self->error("Undefined last_id. Nothing acquired yet?");
    $self->mark_as('working');
    local $Cmd;
    if ( my $error= $self->_do_callback($callback) ) {
        $self->mark_as('failed');
        $self->error($error);
    } else {
        $self->mark_as('processed');
    }
    return 1;
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

sub reset   { confess "abstract method must be overriden by subclass\n"; }
sub acquire { confess "abstract method must be overriden by subclass\n"; }
sub release { confess "abstract method must be overriden by subclass\n"; }

=head2 $object->error()

Calls the 'error' callback if the user has provided one, otherwise calls
confess(). Probably not all that useful for an end user.

=cut

sub error {
    my $self= shift;
    if ( $self->{error} ) {
        $self->{error}->(@_);
    } else {
        confess @_;
    }
}

=head2 $object->consume($callback)

Consumes a data resource until it is exhausted using
acquire(), process(), and release() as appropriate. Normally this is
the main method used by external processes.

Before each attempt to acquire a new resource, and once at the end of
each pass consume will call proceed() to determine if it can do so. The
user may hook into this by specifying a callback in the constructor. This
callback will be executed with no args when it is in the inner loop (per
item), and with the number of passes at the end of each pass (starting with 1).

=head2 $object->proceed($passes)

Returns true if the conditions specified at construction time are
satisfied and processing may proceed. Returns false otherwise.

If the user has specified a 'proceed' callback in the constructor then
this will be executed before any other rules are applied, with a reference
to the current $object, a reference to the runstats, and if being called at
the end of pass with the number of passes.

If this callback returns true then the other rules will be applied, and
only if all other conditions from the constructort are satisfied will proceed()
itself return true.

=head2 $object->sweep()

If the user has specified both a 'working' and a 'failed' state then
this routine will move all lockable 'working' items and change them to
the 'failed' state. This is to catch catastrophic failures where unprocessed
items are left in the working state. Presumably this is a rare case.

=head2 $object->runstats()

Returns a reference to a hash of statistics about the last (or currently running)
execution of consume.

=cut

sub runstats { $_[0]->{runstats} }

sub proceed {
    my $self= shift;
    my $runstats= $self->{runstats};
    $runstats->{end_time}= time;
    $runstats->{elapsed}= $runstats->{end_time} - $runstats->{start_time};

    if ( my $cb= $self->{proceed} ) {
        $cb->( $self, $self->{runstats}, @_ )    # pass on the $passes argument if its there
          or return;
    }
    for my $key (qw(elapsed passes processed failed)) {
        my $max= "max_$key";
        return if $self->{$max} && $runstats->{$key} > $self->{$max};
    }

    return 1;
}

sub consume {
    my $self= shift;
    my $callback= shift;

    my $passes= 0;

    my %runstats;
    $self->{runstats}= \%runstats;

    $runstats{start_time}= time;
    $runstats{$_}= 0 for qw(passes updated failed updated_this_pass failed_this_pass);

    $self->reset();
    do {
        ++$runstats{passes};
        $runstats{updated_this_pass}= $runstats{failed_this_pass}= 0;
        while ( $self->proceed && defined( my $item= $self->acquire ) ) {
            eval {
                $self->process($callback);
                $runstats{updated_this_pass}++;
                $runstats{updated}++;
                1;
              }
              or do {
                $runstats{failed_this_pass}++;
                $runstats{failed}++;

                # quotes force string copy
                $self->error("Failed during callback handling: $@");
              };
        }
        $self->_sweep() if $self->{sweep};
      } while $self->proceed( $runstats{passes} )
          && $runstats{updated_this_pass};

    # if we still hold a lock let it go.
    $self->release;
    return \%runstats;
}

sub _fixup_sweeper {
    ;    # no-op (semicolon prevents tidy from messing with this line)
}

sub _sweep {
    my $self= shift;
    return unless $self->{sweep};
    unless ( $self->{sweeper} ) {
        my $new= bless {%$self}, ref $self;

        @$new{ 'unprocessed', 'processed' }= @$new{ 'working', 'failed' };
        delete @$new{qw(proceed runstats working failed sweep sweeper)};
        delete @$new{ grep { /^max-/ } keys %$new };
        $new->{ max-passes }= 1;
        $self->_fixup_sweeper($new);

        $self->{sweeper}= $new;
    }
    $self->{sweeper}->consume( sub { $self->debug_warn( 5, "sweeping up $_[1]" ) } );
}

=head1 AUTHOR

Yves Orton, C<< <YVES at cpan.org> >>

=head1 BUGS

Please report any bugs or feature requests to
C<bug-data-consumer at rt.cpan.org>, or through the web interface at
L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Data-Consumer>.

I will be notified, and then you'll automatically be notified of progress on
your bug as I make changes.

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

Igor Sutton for ideas, testing and support.

=head1 COPYRIGHT & LICENSE

Copyright 2008 Yves Orton, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

=cut

1;    # End of Data::Consumer

