# -*-perl-*-
# Creation date: 2003-03-30 12:17:42
# Authors: Don
# Change log:
# $Id: Wrapper.pm,v 1.7 2003/04/02 06:29:36 don Exp $
#
# All rights reserved. This program is free software; you can
# redistribute it and/or modify it under the same terms as Perl
# itself.

=pod

=head1 NAME

DBIx::Wrapper - A wrapper around the DBI

=head1 SYNOPSIS

use DBIx::Wrapper;

my $db = DBIx::Wrapper->connect($dsn, $user, $auth, \%attr);

=head1 DESCRIPTION

DBIx::Wrapper provides a wrapper around the DBI that makes it a
bit easier on the programmer.  This module allows you to execute
a query with a single method call.

=head1 METHODS

=cut

use strict;

{   package DBIx::Wrapper;

    use vars qw($VERSION);

    BEGIN {
        $VERSION = 0.01; # update below in POD as well
    }

    use DBI;
    use DBIx::Wrapper::SQLCommand;
    use DBIx::Wrapper::Statement;
    use DBIx::Wrapper::SelectLoop;
    use DBIx::Wrapper::StatementLoop;

    sub _new {
        my ($proto) = @_;
        my $self = bless {}, ref($proto) || $proto;
        return $self;
    }

=pod

=head2 connect($data_source, $username, $auth, \%attr)

Connects to the given database.  These are the same parameters
you would pass to the connect call when using DBI directly.

=head2 new($data_source, $username, $auth, \%attr)

An alias for connect().

=cut
    sub connect {
        my ($proto, $data_source, $username, $auth, $attr) = @_;
        my $self = $proto->_new;

        my $dbh = DBI->connect($data_source, $username, $auth, $attr);
        return undef unless $dbh;

        $self->_setDatabaseHandle($dbh);
        $self->_setDataSource($data_source);
        $self->_setUsername($username);
        $self->_setAuth($auth);
        $self->_setAttr($attr);

        return $self;
    }

    *new = \&connect;

    sub _insert_replace {
        my ($self, $operation, $table, $data) = @_;

        my @values;
        my @fields;
        my @place_holders;

        while (my ($field, $value) = each %$data) {
            push @fields, $field;

            if (UNIVERSAL::isa($value, 'DBIx::Wrapper::SQLCommand')) {
                push @place_holders, $value->asString;
            } else {
                $value = '' unless defined($value);
                push @place_holders, '?';                
                push @values, $value;
            }
        }

        my $fields = join(",", @fields);
        my $place_holders = join(",", @place_holders);
        my $query = qq{$operation INTO $table ($fields) values ($place_holders)};

        my $sth = $self->_getDatabaseHandle()->prepare($query)
            or return $self->setErr(0, $DBI::errstr);
        my $rv = $sth->execute(@values)
            or return $self->setErr(1, $DBI::errstr);
        $sth->finish;
        
        return $rv;
    }

=pod

=head2 insert($table, \%data)

Insert the provided row into the database.  $table is the name of
the table you want to insert into.  %data is the data you want to
insert -- a hash with key/value pairs representing a row to be
insert into the database.

=cut
    sub insert {
        my ($self, $table, $data) = @_;
        return $self->_insert_replace('INSERT', $table, $data);
    }

=pod

=head2 replace($table, \%data)

Same as insert(), except does a REPLACE instead of an INSERT for
databases which support it.

=cut
    sub replace {
        my ($self, $table, $data) = @_;
        return $self->_insert_replace('REPLACE', $table, $data);
    }

=pod

=head2 update($table, \%keys, \%data)

Update the table using the key/value pairs in %keys to specify
the WHERE clause of the query.  %data contains the new values for
the row(s) in the database.

=cut
    sub update {
        my ($self, $table, $keys, $data) = @_;

        my @fields;
        my @values;
        my @set;

        while (my ($field, $value) = each %$data) {
            push @fields, $field;
            if (UNIVERSAL::isa($value, 'DBIx::Wrapper::SQLCommand')) {
                push @set, "$field=" . $value->asString;
            } else {
                $value = "" unless defined $value;
                push @set, "$field=?";
                push @values, $value;
            }
        }

        my @keys = keys %$keys;
        push @values, @$keys{@keys};

        my $set = join(",", @set);
        my $where = join(" AND ", map { "$_=?" } @keys);

        my $query = qq{UPDATE $table SET $set WHERE $where};
        my $sth = $self->_getDatabaseHandle()->prepare($query)
            or return $self->setErr(0, $DBI::errstr);
        my $rv = $sth->execute(@values)
            or return $self->setErr(1, $DBI::errstr);
        $sth->finish;
        
        return $rv;
    }

=pod

=head2 smartUpdate($table, \%keys, \%data)

Same as update(), except that a check is first made to see if
there are any rows matching the data in %keys.  If so, update()
is called, otherwise, insert() is called.

=cut
    sub smartUpdate {
        my ($self, $table, $keys, $data) = @_;
        my @keys = keys %$keys;
        my $where = join(" AND ", map { "$_=?" } @keys);

        my $query = qq{SELECT * FROM $table WHERE $where};
        my $sth = $self->_getDatabaseHandle()->prepare($query);
        $sth->execute(@$keys{@keys});
        my $info = $sth->fetchrow_hashref;
        if ($info and %$info) {
            return $self->update($table, $keys, $data);
        } else {
            my %new_data = %$data;
            while (my ($key, $value) = each %$keys) {
                $new_data{$key} = $value unless exists $new_data{$key};
            }
            return $self->insert($table, \%new_data);
        }
        
    }
    *smart_update = \&smartUpdate;

=pod

=head2 nativeSelect($query, \@exec_args)

Executes the query in $query and returns a single row result.  If
there are multiple rows in the result, the rest get silently
dropped.  @exec_args are the same arguments you would pass to an
execute() called on a DBI object.

=cut
    sub nativeSelect {
        my ($self, $query, $exec_args) = @_;
        
        if (scalar(@_) == 3) {
            $exec_args = [ $exec_args ] unless ref($exec_args);
        }
        $exec_args = [] unless $exec_args;

        my $sth = $self->_getDatabaseHandle()->prepare($query)
            or return $self->setErr(0, $DBI::errstr);
        my $rv = $sth->execute(@$exec_args)
            or return $self->setErr(1, $DBI::errstr);
        my $result = $sth->fetchrow_hashref($self->getNameArg);
        $sth->finish;

        return $result;
    }

    *read = \&nativeSelect;
    *native_select = \&nativeSelect;

=pod

=head2 nativeSelectMulti($query, @exec_args)

=cut
    sub nativeSelectMulti {
        my ($self, $query, $exec_args) = @_;

        if (scalar(@_) == 3) {
            $exec_args = [ $exec_args ] unless ref($exec_args);
        }
        $exec_args = [] unless $exec_args;

        my $sth = $self->_getDatabaseHandle()->prepare($query)
            or return $self->setErr(0, $DBI::errstr);
        my $rv = $sth->execute(@$exec_args)
            or return $self->setErr(1, $DBI::errstr);
        my $rows = [];
        while (my $row = $sth->fetchrow_hashref($self->getNameArg)) {
            push @$rows, $row;
        }
        $sth->finish;

        return $rows;
    }

    *readArray = \&nativeSelectMulti;
    *native_select_multi = \&nativeSelectMulti;

=pod

=head2 nativeSelectLoop($query, @exec_args)

=cut
    sub nativeSelectLoop {
        my ($self, $query, $exec_args) = @_;
        return DBIx::Wrapper::SelectLoop->new($self, $query, $exec_args);
    }

    *readLoop = \&nativeSelectLoop;
    *native_select_loop = \&nativeSelectLoop;

=pod

=head2 nativeQuery($query, @exec_args)

=cut
    sub nativeQuery {
        my ($self, $query, $exec_args) = @_;

        if (scalar(@_) == 3) {
            $exec_args = [ $exec_args ] unless ref($exec_args);
        }
        $exec_args = [] unless $exec_args;

        my $sth = $self->_getDatabaseHandle()->prepare($query)
            or return $self->setErr(0, $DBI::errstr);
        my $rv = $sth->execute(@$exec_args)
            or return $self->setErr(1, $DBI::errstr);
        $sth->finish;

        return $rv;
    }

    *doQuery = \&nativeQuery;
    *native_query = \&nativeQuery;

=pod

=head2 nativeQueryLoop($query)

=cut
    sub nativeQueryLoop {
        my ($self, $query) = @_;
        return DBIx::Wrapper::StatementLoop->new($self, $query);
    }
    *native_query_loop = \&nativeQueryLoop;

=pod

=head2 newCommand($cmd)

=cut
    sub newCommand {
        my ($self, $contents) = @_;
        return DBIx::Wrapper::SQLCommand->new($contents);
    }
    *new_command = \&newCommand;

    sub escapeString {
        my ($self, $value) = @_;
        
        $value = "" unless defined($value);
        $value =~ s|\\|\\\\|g;
        $value =~ s|\'|''|g;
        $value =~ s|\?|\\\?|g;
        $value =~ s|\000|\\0|g;
        $value =~ s|\"|""|g;
        $value =~ s|\n|\\n|g;
        $value =~ s|\r|\\r|g;
        $value =~ s|\t|\\t|g;

        return $value;
    }
    *escape_string = \&escapeString;

    sub DESTROY {
        my ($self) = @_;
        my $dbh = $self->_getDatabaseHandle;
        $dbh->disconnect if $dbh;
    }

    #################
    # getters/setters

    sub getNameArg {
        my ($self) = @_;
        my $arg = $$self{_name_arg};
        $arg = 'NAME_lc' if $arg eq '';

        return $arg;
    }

    sub setNameArg {
        my ($self, $arg) = @_;
        $$self{_name_arg} = $arg;
    }

    sub setErr {
        my ($self, $num, $str) = @_;
        $$self{_err_num} = $num;
        $$self{_err_str} = $str;
        return undef;
    }

    sub getErrorString {
        my ($self) = @_;
        return $$self{_err_str};
    }

    sub getErrorNum {
        my ($self) = @_;
        return $$self{_err_str};
    }
    
    sub _getAttr {
        my ($self) = @_;
        return $$self{_attr};
    }

    sub _setAttr {
        my ($self, $attr) = @_;
        $$self{_attr} = $attr;
    }

    sub _getAuth {
        my ($self) = @_;
        return $$self{_auth};
    }

    sub _setAuth {
        my ($self, $auth) = @_;
        return $$self{_auth};
    }

    sub _getUsername {
        my ($self) = @_;
        return $$self{_username};
    }

    sub _setUsername {
        my ($self, $username) = @_;
        $$self{_username} = $username;
    }

    sub _getDatabaseHandle {
        my ($self) = @_;
        return $$self{_dbh};
    }

    sub _setDatabaseHandle {
        my ($self, $dbh) = @_;
        $$self{_dbh} = $dbh;
    }

    sub _getDataSource {
        my ($self) = @_;
        return $$self{_data_source};
    }

    sub _setDataSource {
        my ($self, $data_source) = @_;
        $$self{_data_source} = $data_source;
    }


}

1;

__END__

=pod

=head2 There are also underscore_separated versions of these methods.

    E.g., nativeSelectLoop() becomes native_select_loop()


=head1 EXAMPLES

=head1 TODO

=over 4

=item Logging

=item Allow prepare() and execute()

=back

=head1 BUGS


=head1 AUTHOR

    Don Owens <don@owensnet.com>

=head1 COPYRIGHT

    Copyright (c) 2003 Don Owens

    All rights reserved. This program is free software; you can
    redistribute it and/or modify it under the same terms as Perl
    itself.

=head1 VERSION

    0.01

=cut
