# -*-perl-*-
# Creation date: 2005-03-04 21:15:40
# Authors: Don
# Change log:
# $Id: Delegator.pm,v 1.3 2005/03/06 00:05:23 don Exp $

use strict;

{   package DBIx::Wrapper::Delegator;

    use vars qw($VERSION $AUTOLOAD);
    $VERSION = do { my @r=(q$Revision: 1.3 $=~/\d+/g); sprintf "%d."."%02d"x$#r,@r };

    sub AUTOLOAD {
        my $self = shift;

        (my $func = $AUTOLOAD) =~ s/^.*::([^:]+)$/$1/;
        return undef if $func eq 'DESTROY';
        
        my $key = $func; # turn method call into hash access
        return $self->{$func};
    }
    
}

1;

__END__

=pod

=head1 NAME

 DBIx::Wrapper::Delegator - 

=head1 SYNOPSIS


=head1 DESCRIPTION


=head1 METHODS


=head1 EXAMPLES


=head1 BUGS


=head1 AUTHOR


=head1 VERSION

$Id: Delegator.pm,v 1.3 2005/03/06 00:05:23 don Exp $

=cut
