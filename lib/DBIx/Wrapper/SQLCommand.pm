# -*-perl-*-
# Creation date: 2003-03-30 16:26:50
# Authors: Don
# Change log:
# $Id: SQLCommand.pm,v 1.3 2004/07/01 06:37:11 don Exp $

use strict;

{   package DBIx::Wrapper::SQLCommand;

    use vars qw($VERSION);
    $VERSION = do { my @r=(q$Revision: 1.3 $=~/\d+/g); sprintf "%d."."%02d"x$#r,@r };

    sub new {
        my ($proto, $str) = @_;
        my $self = bless { _str => $str }, ref($proto) || $proto;
        return $self;
    }

    sub asString {
        my ($self) = @_;
        return $$self{_str};
    }
    *as_string = \&asString;

    
}

1;

__END__

=pod

=head1 NAME

DBIx::Wrapper::SQLCommand - Used by DBIx::Wrapper to pass SQL
as-is.  This is deprecated.  Use a scalar reference instead.

=head1 SYNOPSIS


=head1 DESCRIPTION


=head1 METHODS


=head1 EXAMPLES


=head1 BUGS


=head1 AUTHOR


=head1 VERSION

$Id: SQLCommand.pm,v 1.3 2004/07/01 06:37:11 don Exp $

=cut
