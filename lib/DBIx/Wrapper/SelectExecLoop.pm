# -*-perl-*-
# Creation date: 2004-04-21 10:45:30
# Authors: Don
# Change log:
# $Id: SelectExecLoop.pm,v 1.1 2004/04/21 20:01:35 don Exp $

use strict;

{   package DBIx::Wrapper::SelectExecLoop;

    use vars qw($VERSION);
    $VERSION = do { my @r=(q$Revision: 1.1 $=~/\d+/g); sprintf "%d."."%02d"x$#r,@r };

    use base 'DBIx::Wrapper::Statement';
    
    sub new {
        my ($proto, $parent, $query, $multi) = @_;
        my $sth = $parent->_getDatabaseHandle()->prepare($query);
        unless ($sth) {
            $parent->_printDbiError("\nQuery was '$query'\n");
            return $parent->setErr(0, $DBI::errstr);
        }

        my $self =
            bless { _query => $query, _multi => $multi || '' }, ref($proto) || $proto;
        $self->_setSth($sth);
        $self->_setParent($parent);
        return $self;
    }

    sub next {
        my ($self, $exec_args) = @_;
        my $sth = $self->_getSth;
        if ($$self{_multi}) {
            if ($sth->execute(@$exec_args)) {
                my $rows = [];
                while (my $row = $sth->fetchrow_hashref) {
                    push @$rows, $row;
                }
                return $rows;
            }

        } else {
            if ($sth->execute(@$exec_args)) {
                return $sth->fetchrow_hashref;
            }
        }
        return undef;
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

 DBIx::Wrapper::SelectExecLoop - 

=head1 SYNOPSIS


=head1 DESCRIPTION


=head1 METHODS


=head1 EXAMPLES


=head1 BUGS


=head1 AUTHOR


=head1 VERSION

$Id: SelectExecLoop.pm,v 1.1 2004/04/21 20:01:35 don Exp $

=cut
