# -*-perl-*-
# Creation date: 2003-03-30 15:23:31
# Authors: Don
# Change log:
# $Id: Statement.pm,v 1.4 2004/10/30 00:09:31 don Exp $

use strict;

{   package DBIx::Wrapper::Statement;

    use vars qw($VERSION);
    $VERSION = do { my @r=(q$Revision: 1.4 $=~/\d+/g); sprintf "%d."."%02d"x$#r,@r };

    sub new {
        my ($proto) = @_;
        my $self = bless {}, ref($proto) || $proto;
        return $self;
    }


    ####################
    # getters/setters

    sub _getSth {
        my ($self) = @_;
        return $$self{_sth};
    }

    sub _setSth {
        my ($self, $sth) = @_;
        $$self{_sth} = $sth;
    }

    sub _getParent {
        my ($self) = @_;
        return $$self{_parent};
    }

    sub _setParent {
        my ($self, $parent) = @_;
        $$self{_parent} = $parent;
    }

    sub _getQuery {
        my $self = shift;
        return $self->{_query};
    }

    sub _setQuery {
        my $self = shift;
        my $query = shift;
        $self->{_query} = $query;
    }

    sub _getRequestObj {
        return shift()->{_request_obj};
    }
    
    sub _setRequestObj {
        my $self = shift;
        $self->{_request_obj} = shift;
    }
    
}

1;

__END__

=pod

=head1 NAME

DBIx::Wrapper::Statement - Statement wrapper used by DBIx::Wrapper

=head1 SYNOPSIS


=head1 DESCRIPTION


=head1 METHODS


=head1 EXAMPLES


=head1 BUGS


=head1 AUTHOR


=head1 VERSION

$Id: Statement.pm,v 1.4 2004/10/30 00:09:31 don Exp $

=cut
