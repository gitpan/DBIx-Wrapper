# -*-perl-*-
# Creation date: 2003-03-30 15:24:44
# Authors: Don
# Change log:
# $Id: StatementLoop.pm,v 1.3 2004/04/21 20:00:53 don Exp $

use strict;

{   package DBIx::Wrapper::StatementLoop;

    use vars qw($VERSION);
    $VERSION = do { my @r=(q$Revision: 1.3 $=~/\d+/g); sprintf "%d."."%02d"x$#r,@r };

    use base 'DBIx::Wrapper::Statement';

    sub new {
        my ($proto, $parent, $query) = @_;
        my $sth = $parent->_getDatabaseHandle()->prepare($query);
        unless ($sth) {
            $parent->_printDbiError("\nQuery was '$query'\n");
            return $parent->setErr(0, $DBI::errstr);
        }
        my $self = bless {}, ref($proto) || $proto;
        $self->_setSth($sth);
        return $self;
    }

    sub next {
        my ($self, $exec_args) = @_;
        if (scalar(@_) == 3) {
            $exec_args = [ $exec_args ] unless ref($exec_args);
        }
        $exec_args = [] unless $exec_args;

        my $sth = $self->_getSth;
        $sth->execute(@$exec_args);
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

DBIx::Wrapper::StatementLoop - 

=head1 SYNOPSIS


=head1 DESCRIPTION


=head1 METHODS


=head1 EXAMPLES


=head1 BUGS


=head1 AUTHOR


=head1 VERSION

$Id: StatementLoop.pm,v 1.3 2004/04/21 20:00:53 don Exp $

=cut
