# -*-perl-*-
# Creation date: 2003-03-30 15:23:31
# Authors: Don
# Change log:
# $Id: Statement.pm,v 1.5 2006/03/26 19:18:35 don Exp $

# Copyright (c) 2003-2006 Don Owens
#
# All rights reserved. This program is free software; you can
# redistribute it and/or modify it under the same terms as Perl
# itself.


use strict;

{   package DBIx::Wrapper::Statement;

    use vars qw($VERSION);
    $VERSION = do { my @r=(q$Revision: 1.5 $=~/\d+/g); sprintf "%d."."%02d"x$#r,@r };

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

