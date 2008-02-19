package Data::Consumer;

use warnings;
use strict;
use Carp;
use vars qw/$Debug $VERSION/;



=head1 NAME

Data::Consumer - Repeatedly consume a data resource in a robust way

=head1 VERSION

Version 0.03

=cut

$VERSION = '0.03';
#$Debug = 1;

=head1 SYNOPSIS

    use Data::Consumer;
    use Data::Consumer::MySQL;
    my $consumer = Data::Consumer->new(
        type => 'MySQL',
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


=head1 METHODS

=head2 CLASS->new(%opts)

Constructor. Normally this routine is not used and instead
the appropriate sub classes constructor is used, however
the base classes constructor may be used as a data driven
constructor by using the key 'type' and the name of the subclass.

Thus

    Data::Consumer->new(type=>'MySQL',%args);

is exactly equivalent to calling

    Data::Consumer::MySQL->new(%args);

The subclass name is case insensitive and unless otherwise documented 
is part of the class name after 'Data::Consumer' has been removed, 
with colons optionally replaced by dashes.

Thus 'mysql' and 'MYSQL' are valid type names for Data::Consumer::MySQL

=head2 CLASS->register(@alias)

Used by subclasses to register themselves as a Data::Consumer subclass
and register any additional aliases that the class may be identified as.

Will throw an exception is any of the aliases are already associated to a 
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
    my $self = shift;
    my $level = shift;
    if ($Debug and $Debug>=$level) {
        warn ref($self)||$self , "\t$$\t>>> $_\n"
        for @_;
    }
}

BEGIN {
    my %alias2class;
    my %class2alias;
    $Debug and $Debug>=5 and warn "\n";
    sub register {
        my $class = shift;

        ref $class
            and confess "register() is a class method and cannot be called on an object\n";
        my $pack=__PACKAGE__;

        if ($class eq $pack) {
            return wantarray ? %alias2class : 0 + keys %alias2class;
        }

        (my $std_name=$class)=~s/^\Q$pack\E:://;
        $std_name=~s/::/-/g;

        my @failed;
        for my $name (map { lc $_ } $class, $std_name, @_) {
            if ($alias2class{$name} and $alias2class{$name} ne $class) {
                push @failed,$name;
                next;
            }
            __PACKAGE__->debug_warn(5,"registered '$name' as an alias of '$class'");
            $alias2class{$name} = $class;
            $class2alias{$class}{$name}=$class;
        }
        @failed and 
            confess "Failed to register aliases for '$class' as they are already used\n",
                    join("\n",map { "\t'$_' is already assigned to '$alias2class{lc($_)}'"} @failed),
                    "\n";
        return wantarray ? %{$class2alias{$class}} : 0 + keys %{$class2alias{$class}};
    }

    sub new {
        my ($class, %opts)= @_;
        ref $class 
            and confess "new() is a class method and cannot be called on an object\n";

        if ($class eq __PACKAGE__) {
            my $type = $opts{type}
                or confess "'type' is a mandatory named parameter for $class->new()\n";
            $class = $alias2class{lc($type)}
                or confess "'type' parameter '$type' is not a known alias of any registered type currently loaded\n";
        }
        my $object = bless {}, $class;
        $class->debug_warn(5,"created new object '$object'");
        return $object
    }
}

=head2 $object->last_id

Returns the identifier for the last item acquired. 

Returns undef if acquire has never been called or if the last 
attempt to acquire data failed because none was available.

=cut

sub last_id {
    my $self = shift;
    return $self->{last_id};
}

=head2 $object->_mark_as($type,$id)

** Must be overriden **

Mark an item as a particular type if the object defines that type. 

This is wrapped by mark_as() for error checking, so you are guaranteed
that $type will be one of 

    'unprocessed', 'working', 'processed', 'failed'

and that $object->{$type} will be true value, and that $id will be from 
the currently acquired item.

=head2 $object->mark_as($type)

Mark an item as a particular type if the object defines that type.

Allowed types are 'unprocessed', 'working', 'processed', 'failed'

=cut

sub _mark_as { confess "must be overriden" }
BEGIN {
    my (%valid,@valid);
    @valid = qw ( unprocessed working processed failed );
    @valid{@valid} = (1..@valid);

    sub mark_as {
        my $self = shift @_;
        my $key = shift @_;
        
        $valid{$key} 
            or confess "Unknown type in mark_as(), valid options are ", 
                  join(", ", map { "'$_'" } @valid),
                  "\n";

        my $id = @_ ? shift @_ : $self->last_id;
        defined $id 
            or confess "Nothing acquired to be marked as '$key' in mark_as.\n";

        return unless $self->{$key};
        return $self->_mark_as($key,$id);
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
    my $self = shift;
    my $callback = shift;
    my $id = $self->last_id;
    defined $id 
        or $self->error("Undefined last_id. Nothing acquired yet?");
    $self->mark_as('working');
    if ( my $error = $self->_do_callback($callback) ) {
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


=head2 _check

Calls the 'check' callback if the user has provided one.

=head2 error

Calls the 'error' callback if the user has provided one, otherwise calls 
confess().

=cut


sub _check   { 
    my ($self,$passes,$updated,$failed,$updated_this_pass,$failed_this_pass)=@_;
    if ($self->{check}) {
        $self->{check}->($passes,$updated,$failed,$updated_this_pass,$failed_this_pass);
    }
}

sub error   { 
    my $self=shift;
    if ($self->{error}) {
        $self->{error}->(@_);
    } else {
        confess @_
    }
}


=head2 $object->consume($callback)

Consumes a data resource until it is exhausted using 
acquire(), process(), and release() as appropriate. Normally this is
the main method used by external processes.

Takes a subroutine reference as an argument. The subroutine
will be passed arguments of the id of the item currently being
processed, and the consumer object iteself. See process() for more 
details.

=cut

sub consume {
    my $self= shift;
    my $callback = shift;

    my $passes  = 0;

    my $updated = 0;
    my $failed  = 0;
    my ($updated_this_pass, $failed_this_pass);

    $self->reset();
    do  { UPDATED:{
        ++$passes;
        $updated_this_pass = 0;
        $failed_this_pass = 0;
        while ( defined( my $item = $self->acquire() ) ) {
            eval { 
                $self->process($callback);
                $updated_this_pass++;
                1; 
            } or do {
                $failed_this_pass++;
                $self->error("Failed during callback handling: $@"); # quotes force string copy
            };
            last if 'stop' eq lc(
                $self->_check($passes,$updated,$failed,$updated_this_pass,$failed_this_pass));
        }
        $updated += $updated_this_pass;
        $failed  += $failed_this_pass;
        last if 'stop' eq lc($self->_check($passes,$updated,$failed,$updated_this_pass,$failed_this_pass));
    } } while $updated_this_pass;
    $self->release(); # if we still hold a lock let it go.
    return wantarray ? ($updated,$failed) : $updated;
}

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
