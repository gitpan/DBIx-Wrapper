# -*-perl-*-
# Creation date: 2003-03-30 15:25:00
# Authors: Don
# Change log:
# $Id: SelectLoop.pm,v 1.10 2005/03/06 00:02:18 don Exp $

use strict;

{   package DBIx::Wrapper::SelectLoop;

    use vars qw($VERSION);
    $VERSION = do { my @r=(q$Revision: 1.10 $=~/\d+/g); sprintf "%d."."%02d"x$#r,@r };

    use base 'DBIx::Wrapper::Statement';

    sub new {
        my ($proto, $parent, $query, $exec_args) = @_;
        my $self = { _query => $query, _exec_args => $exec_args,
                     _cur_row_count => 0
                   };

        bless $self, ref($proto) || $proto;
        $self->_setParent($parent);

        my ($sth, $rv, $r);
        if (scalar(@_) == 4) {
            ($sth, $rv, $r) = $parent->_getStatementHandleForQuery($query, $exec_args);
        } else {
            ($sth, $rv, $r) = $parent->_getStatementHandleForQuery($query);
        }
        
        return $sth unless $sth;
                        
        $self->_setSth($sth);
        $self->_setRequestObj($r);
        
        return $self;
    }

    sub next {
        my ($self) = @_;
        my $sth = $self->_getSth;
        $self->{_cur_row_count}++;

        my $r = $self->_getRequestObj;
        $self->_getParent()->_runPreFetchHook($r);
        $sth = $r->getStatementHandle;

        my $result = $sth->fetchrow_hashref($self->_getParent()->getNameArg);

        $r->setReturnVal($result);
        $self->_getParent()->_runPostFetchHook($r);
        $result = $r->getReturnVal;

        return $result;
    }

    sub nextWithArrayRef {
        my ($self) = @_;
        my $sth = $self->_getSth;
        $self->{_cur_row_count}++;

        my $r = $self->_getRequestObj;
        $self->_getParent()->_runPreFetchHook($r);
        $sth = $r->getStatementHandle;
        
        my $row = $sth->fetchrow_arrayref;

        $r->setReturnVal($row);
        $self->_getParent()->_runPostFetchHook($r);
        $row = $r->getReturnVal;
        
        return [ @$row ] if $row;
        
        return undef;
    }
    *nextArrayRef = \&nextWithArrayRef;

    sub rowCountCurrent {
        my ($self) = @_;
        return $$self{_cur_row_count};
    }

    sub rowCountTotal {
        my ($self) = @_;
        my $sth = $self->_getSth;
        return $sth->rows;
    }
    *count = \&rowCountTotal;

    sub DESTROY {
        my ($self) = @_;
        my $sth = $self->_getSth;
        $sth->finish if $sth;
    }


}

1;

__END__

=pod

=head1 NAME

DBIx::Wrapper::SelectLoop - Used by DBIx::Wrapper for SelectLoop methods

=head1 SYNOPSIS


=head1 DESCRIPTION


=head1 METHODS


=head1 EXAMPLES


=head1 BUGS


=head1 AUTHOR


=head1 VERSION

$Id: SelectLoop.pm,v 1.10 2005/03/06 00:02:18 don Exp $

=cut