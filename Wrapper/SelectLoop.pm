# -*-perl-*-
# Creation date: 2003-03-30 15:25:00
# Authors: Don
# Change log:
# $Id: SelectLoop.pm,v 1.2 2003/03/31 06:01:17 don Exp $

use strict;

{   package DBIx::Wrapper::SelectLoop;

    use vars qw($VERSION);
    $VERSION = do { my @r=(q$Revision: 1.2 $=~/\d+/g); sprintf "%d."."%02d"x$#r,@r };

    use base 'DBIx::Wrapper::Statement';

    sub new {
        my ($proto, $parent, $query, $exec_args) = @_;
        my $self = { _query => $query, _exec_args => $exec_args};
        my $dbh = $parent->_getDatabaseHandle;
        my $sth = $dbh->prepare($query)
            or return $parent->setErr(0, $DBI::errstr);
        unless (scalar(@_) == 4) {
            $exec_args = [];
        }
        $exec_args = [ $exec_args ] unless ref($exec_args);
        $sth->execute(@$exec_args)
            or return $parent->setErr(1, $DBI::errstr);
        
        bless $self, ref($proto) || $proto;
        
        $self->_setSth($sth);
        $self->_setParent($parent);
        
        return $self;
    }

    sub next {
        my ($self) = @_;
        my $sth = $self->_getSth;
        return $sth->fetchrow_hashref($self->_getParent()->getNameArg);
    }

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

DBIx::Wrapper::SelectLoop - 

=head1 SYNOPSIS


=head1 DESCRIPTION


=head1 METHODS


=head1 EXAMPLES


=head1 BUGS


=head1 AUTHOR


=head1 VERSION

$Id: SelectLoop.pm,v 1.2 2003/03/31 06:01:17 don Exp $

=cut
