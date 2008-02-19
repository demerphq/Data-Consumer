package Data::Consumer;

use warnings;
use strict;
use Carp;

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

Constructor. Must be overriden. See details of constructor in sub classes.

=cut

sub new {
    my $class = shift;
    $class eq __PACKAGE__
        and coness "$class is an abstract base class, you cannot instanciate it\n"

    ref $class 
        and confess "new() is a class method and cannot be called on an object\n";

    return bless {},$class;
}

=head2  reset 

Reset the state of the object.

=head2 acquire

Aquire an item to be processed. 

returns an identifier to be used to identify the item acquired.

=head2 release 

Release any locks on the currently held item.

Normally there is no need to call this directly. 

=cut

sub reset   { confess "abstract method must be overriden by subclass\n"; }
sub acquire { confess "abstract method must be overriden by subclass\n"; }
sub release { confess "abstract method must be overriden by subclass\n"; }


=begin developer

=head2 _check

Calls the 'check' callback if the user has provided one.

=head2 _error

Calls the 'error' callback if the user has provided one, otherwise calls 
confess().

=cut


sub _check   { 
    my ($self,$passes,$updated,$failed,$updated_this_pass,$failed_this_pass)=@_;
    if ($self->{check}) {
        $self->{check}->($passes,$updated,$failed,$updated_this_pass,$failed_this_pass);
    }
}

sub _error   { 
    my $self=shift;
    if ($self->{_error}) {
        $self->{_error}->(@_);
    } else {
        confess @_
    }
}

=end developer

=head2 consume

Takes a subroutine callback as an argument.

Will repeatedly aquire items, call the callback with the items identifier
as an argument, and then mark it as done.

If the callback dies during processing then the item will be marked as failed.

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
                $self->_error("Failed during callback handling: $@"); # quotes force string copy
            };
            
        }
        $updated += $updated_this_pass;
        $failed  += $failed_this_pass;
        last if 'stop' eq lc($self->_check($passes,$updated,$failed,$updated_this_pass,$failed_this_pass));
    } } while $updated_this_pass;
    $self->release(); # if we still hold a lock let it go.
    return wantarray ? ($updated,$failed) : $updated;
}

=head2 function2

=cut

sub function2 {
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
