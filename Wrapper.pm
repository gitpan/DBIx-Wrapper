# -*-perl-*-
# Creation date: 2003-03-30 12:17:42
# Authors: Don
# Change log:
# $Id: Wrapper.pm,v 1.17 2004/02/16 09:09:31 don Exp $
#
# Copyright (c) 2003-2004 Don Owens
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

    # use 5.006; # should have at least Perl 5.6.0
    
    use Carp ();
    
    use vars qw($VERSION);

    BEGIN {
        $VERSION = '0.07'; # update below in POD as well
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
        unless (ref($attr) eq 'HASH' and defined($$attr{PrintError}) and not $$attr{PrintError}) {
            $self->addDebugLevel(2); # print on error
        }
        unless ($dbh) {
            if ($self->_isDebugOn) {
                $self->_printDebug(Carp::longmess($DBI::errstr));
            } else {
                $self->_printDbiError;
            }
            return undef;
        }

        $self->_setDatabaseHandle($dbh);
        $self->_setDataSource($data_source);
        $self->_setUsername($username);
        $self->_setAuth($auth);
        $self->_setAttr($attr);
        $self->_setDisconnect(1);

        return $self;
    }

    *new = \&connect;

    sub addDebugLevel {
        my ($self, $level) = @_;
        $$self{_debug_level} |= $level;
    }

    sub getDebugLevel {
        return shift()->{_debug_level};
    }

=pod

=head2 newFromDBI($dbh)

Returns a new DBIx::Wrapper object from a DBI object that has
already been created.  Note that when created this way,
disconnect() will not be called automatically on the underlying
DBI object when the DBIx::Wrapper object goes out of scope.

=cut
    sub newFromDBI {
        my ($proto, $dbh) = @_;
        my $self = $proto->_new;
        $self->_setDatabaseHandle($dbh);
        return $self;
    }
    *new_from_dbi = \&newFromDBI;

=pod

=head2 getDBI()

Return the underlying DBI object used to query the database.

=cut
    sub getDBI {
        my ($self) = @_;
        return $self->_getDatabaseHandle;
    }
    *get_dbi = \&getDBI;

    sub _insert_replace {
        my ($self, $operation, $table, $data) = @_;

        my @values;
        my @fields;
        my @place_holders;

        while (my ($field, $value) = each %$data) {
            push @fields, $field;

            if (UNIVERSAL::isa($value, 'DBIx::Wrapper::SQLCommand')) {
                push @place_holders, $value->asString;
            } elsif (ref($value) eq 'SCALAR') {
                push @place_holders, $$value;
            } else {
                $value = '' unless defined($value);
                push @place_holders, '?';                
                push @values, $value;
            }
        }

        my $fields = join(",", @fields);
        my $place_holders = join(",", @place_holders);
        my $query = qq{$operation INTO $table ($fields) values ($place_holders)};

        $self->_printDebug($query);

        my $dbh = $self->_getDatabaseHandle;
        my $sth = $dbh->prepare($query);
        unless ($sth) {
            if ($self->_isDebugOn) {
                $self->_printDebug(Carp::longmess($dbh->errstr) . "\nQuery was '$query'\n");
            } else {
                $self->_printDbiError("\nQuery was '$query'\n");
            }
            return $self->setErr(0, $dbh->errstr);
        }
        my $rv = $sth->execute(@values);
        unless ($rv) {
            if ($self->_isDebugOn) {
                $self->_printDebug(Carp::longmess($dbh->errstr) . "\nQuery was '$query'\n");
            } else {
                $self->_printDbiError("\nQuery was '$query'\n");
            }
            return $self->setErr(1, $dbh->errstr);
        }
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
            } elsif (ref($value) eq 'SCALAR') {
                push @set, "$field=" . $$value;
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

        $self->_printDebug($query);

        my $dbh = $self->_getDatabaseHandle;
        my $sth = $dbh->prepare($query);
        unless ($sth) {
            if ($self->_isDebugOn) {
                $self->_printDebug(Carp::longmess($dbh->errstr) . "\nQuery was '$query'\n");
            } else {
                $self->_printDbiError("\nQuery was '$query'\n");
            }
            return $self->setErr(0, $dbh->errstr);
        }
        
        my $rv = $sth->execute(@values);
        unless ($rv) {
            if ($self->_isDebugOn) {
                $self->_printDebug(Carp::longmess($dbh->errstr) . "\nQuery was '$query'\n");
            } else {
                $self->_printDbiError("\nQuery was '$query'\n");
            }
            return $self->setErr(1, $dbh->errstr);
        }
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

        $self->_printDebug($query);

        my $dbh = $self->_getDatabaseHandle;
        my $sth = $dbh->prepare($query);
        unless ($sth) {
            if ($self->_isDebugOn) {
                $self->_printDebug(Carp::longmess($dbh->errstr) . "\nQuery was '$query'\n");
            } else {
                $self->_printDbiError("\nQuery was '$query'\n");
            }
            return $self->setErr(0, $dbh->errstr);
        }
        my $rv = $sth->execute(@$exec_args);
        unless ($rv) {
            if ($self->_isDebugOn) {
                $self->_printDebug(Carp::longmess($dbh->errstr) . "\nQuery was '$query'\n");
            } else {
                $self->_printDbiError("\nQuery was '$query'\n");
            }
            return $self->setErr(1, $dbh->errstr);
        }
        my $result = $sth->fetchrow_hashref($self->getNameArg);
        $sth->finish;

        return $result;
    }

    *read = \&nativeSelect;
    *native_select = \&nativeSelect;

=pod

=head2 nativeSelectMulti($query, @exec_args)

Executes the query in $query and returns an array of rows, where
each row is a hash representing a row of the result.

=cut
    sub nativeSelectMulti {
        my ($self, $query, $exec_args) = @_;

        if (scalar(@_) == 3) {
            $exec_args = [ $exec_args ] unless ref($exec_args);
        }
        $exec_args = [] unless $exec_args;

        $self->_printDebug($query);

        my $dbh = $self->_getDatabaseHandle;
        my $sth = $dbh->prepare($query);
        unless ($sth) {
            if ($self->_isDebugOn) {
                $self->_printDebug(Carp::longmess($dbh->errstr) . "\nQuery was '$query'\n");
            } else {
                $self->_printDbiError("\nQuery was '$query'\n");
            }
            return $self->setErr(0, $dbh->errstr);
        }
        
        my $rv = $sth->execute(@$exec_args);
        unless ($rv) {
            if ($self->_isDebugOn) {
                $self->_printDebug(Carp::longmess($dbh->errstr) . "\nQuery was '$query'\n");
            } else {
                $self->_printDbiError("\nQuery was '$query'\n");
            }
            return $self->setErr(1, $dbh->errstr);
        }
        
        my $rows = [];
        while (my $row = $sth->fetchrow_hashref($self->getNameArg)) {
            push @$rows, $row;
        }
        $sth->finish;

        return $rows;
    }

    *readArray = \&nativeSelectMulti;
    *native_select_multi = \&nativeSelectMulti;

    sub _getSqlObj {
        # return SQL::Abstract->new(case => 'textbook', cmp => '=', logic => 'and');
        require SQL::Abstract;
        return SQL::Abstract->new(case => 'textbook', cmp => '=');
    }

=pod

=head2 abstractSelect($table, \@fields, \%where, \@order)

Same as nativeSelect() except uses SQL::Abstract to generate the
SQL.  See the POD for SQL::Abstract for usage.  You must have
SQL::Abstract installed for this method to work.

=cut
    sub abstractSelect {
        my ($self, $table, $fields, $where, $order) = @_;
        my $sql_obj = $self->_getSqlObj;
        my ($query, @bind) = $sql_obj->select($table, $fields, $where, $order);

        if (@bind) {
            return $self->nativeSelect($query, \@bind);
        } else {
            return $self->nativeSelect($query);
        }
    }
    *abstract_select = \&abstractSelect;

=pod

=head2 abstractSelectMulti($table, \@fields, \%where, \@order)

Same as nativeSelectMulti() except uses SQL::Abstract to generate the
SQL.  See the POD for SQL::Abstract for usage.  You must have
SQL::Abstract installed for this method to work.

=cut
    sub abstractSelectMulti {
        my ($self, $table, $fields, $where, $order) = @_;
        my $sql_obj = $self->_getSqlObj;
        my ($query, @bind) = $sql_obj->select($table, $fields, $where, $order);

        if (@bind) {
            return $self->nativeSelectMulti($query, \@bind);
        } else {
            return $self->nativeSelectMulti($query);
        }
    }

    *abstract_select_multi = \&abstractSelectMulti;

=pod

=head2 nativeSelectLoop($query, @exec_args)

Executes the query in $query, then returns an object that allows
you to loop through one result at a time, e.g.,

    my $loop = $db->nativeSelectLoop("SELECT * FROM my_table");
    while (my $row = $loop->next) {
        my $id = $$row{id};
    }

=cut
    sub nativeSelectLoop {
        my ($self, $query, $exec_args) = @_;
        $self->_printDebug($query);

        if (scalar(@_) == 3) {
            return DBIx::Wrapper::SelectLoop->new($self, $query, $exec_args);
        } else {
            return DBIx::Wrapper::SelectLoop->new($self, $query);
        }
    }

    *readLoop = \&nativeSelectLoop;
    *native_select_loop = \&nativeSelectLoop;

=pod

=head2 nativeQuery($query, @exec_args)

Executes the query in $query and returns true if successful.
This is typically used for deletes and is a catchall for anything
the methods provided by this module don't take into account.

=cut
    sub nativeQuery {
        my ($self, $query, $exec_args) = @_;

        if (scalar(@_) == 3) {
            $exec_args = [ $exec_args ] unless ref($exec_args);
        }
        $exec_args = [] unless $exec_args;

        $self->_printDebug($query);

        my $dbh = $self->_getDatabaseHandle;
        my $sth = $dbh->prepare($query);
        unless ($sth) {
            if ($self->_isDebugOn) {
                $self->_printDebug(Carp::longmess($dbh->errstr) . "\nQuery was '$query'\n");
            }
            return $self->setErr(0, $dbh->errstr);
        }
        my $rv = $sth->execute(@$exec_args);
        unless ($rv) {
            if ($self->_isDebugOn) {
                $self->_printDebug(Carp::longmess($dbh->errstr) . "\nQuery was '$query'\n");
            } else {
                $self->_printDbiError("\nQuery was '$query'\n");
            }
            return $self->setErr(1, $dbh->errstr);
        }
        $sth->finish;

        return $rv;
    }

    *doQuery = \&nativeQuery;
    *native_query = \&nativeQuery;

=pod

=head2 nativeQueryLoop($query)

A loop on nativeQuery, where any placeholders you have put in
your query are bound each time you call next().  E.g.,

    my $loop = $db->nativeQueryLoop("UPDATE my_table SET value=? WHERE id=?");
    $loop->next([ 'one', 1]);
    $loop->next([ 'two', 2]);

=cut
    sub nativeQueryLoop {
        my ($self, $query) = @_;
        $self->_printDebug($query);

        return DBIx::Wrapper::StatementLoop->new($self, $query);
    }
    *native_query_loop = \&nativeQueryLoop;

=pod

=head2 newCommand($cmd)

This creates a literal SQL command for use in insert(), update(),
and related methods, since if you simply put something like
"CUR_DATE()" as a value in the %data parameter passed to insert,
the function will get quoted, and so will not work as expected.
Instead, do something like this:

    my $data = { file => 'my_document.txt',
                 the_date => $db->newCommand('CUR_DATE()')
               };
    $db->insert('my_doc_table', $data);

This can also be done by passing a reference to a string with the
SQL command, e.g.,

    my $data = { file => 'my_document.txt',
                 the_date => \'CUR_DATE()'
               };
    $db->insert('my_doc_table', $data);


=cut
    sub newCommand {
        my ($self, $contents) = @_;
        return DBIx::Wrapper::SQLCommand->new($contents);
    }
    *new_command = \&newCommand;

=pod

=head2 debugOn(\*FILE_HANDLE)

Turns on debugging output.  Debugging information will be printed
to the given filehandle.

=cut
    # expects a reference to a filehandle to print debug info to
    sub debugOn {
        my ($self, $fh) = @_;
        $$self{_debug} = 1;
        $$self{_debug_fh} = $fh;

        return 1;
    }
    *debug_on = \&debugOn;

=pod

=head2 debugOff()

Turns off debugging output.

=cut
    sub debugOff {
        my ($self) = @_;
        undef $$self{_debug};
        undef $$self{_debug_fh};

        return 1;
    }
    *debug_off = \&debugOff;

    sub _isDebugOn {
        my ($self) = @_;
        if (($$self{_debug} and $$self{_debug_fh})
            or $ENV{'DBIX_WRAPPER_DEBUG'}) {
            return 1;
        }
        return undef;
    }

    sub _printDbiError {
        my ($self, $extra) = @_;

        return undef unless ($self->getDebugLevel | 2);

        my $str = Carp::longmess($DBI::errstr);

        $str .= $extra if defined($extra);
        
        my $fh = $$self{_debug_fh};
        $fh = \*STDERR unless $fh;
        
        my $time = $self->_getCurDateTime;

        my ($package, $filename, $line, $subroutine, $hasargs,
            $wantarray, $evaltext, $is_require, $hints, $bitmask);

        my $frame = 1;
        my $this_pkg = __PACKAGE__;

        ($package, $filename, $line, $subroutine, $hasargs,
         $wantarray, $evaltext, $is_require, $hints, $bitmask) = caller($frame);
        while ($package eq $this_pkg) {
            $frame++;
            ($package, $filename, $line, $subroutine, $hasargs,
             $wantarray, $evaltext, $is_require, $hints, $bitmask) = caller($frame);

            # if we get more than 10 something must be wrong
            last if $frame >= 10;
        }

        my @one_more = caller($frame + 1);
        $subroutine = $one_more[3];
        $subroutine = '' unless defined($subroutine);
        $subroutine .= '()' if $subroutine ne '';
        
        print $fh '*' x 60, "\n", "$time:$filename:$line:$subroutine\n", $str, "\n";

    }
    
    sub _printDebug {
        my ($self, $str) = @_;
        unless ($self->_isDebugOn) {
            return undef;
        }

        my $fh = $$self{_debug_fh};
        $fh = \*STDERR unless $fh;
        
        my $time = $self->_getCurDateTime;

        my ($package, $filename, $line, $subroutine, $hasargs,
            $wantarray, $evaltext, $is_require, $hints, $bitmask);

        my $frame = 1;
        my $this_pkg = __PACKAGE__;

        ($package, $filename, $line, $subroutine, $hasargs,
         $wantarray, $evaltext, $is_require, $hints, $bitmask) = caller($frame);
        while ($package eq $this_pkg) {
            $frame++;
            ($package, $filename, $line, $subroutine, $hasargs,
             $wantarray, $evaltext, $is_require, $hints, $bitmask) = caller($frame);

            # if we get more than 10 something must be wrong
            last if $frame >= 10;
        }

        my @one_more = caller($frame + 1);
        $subroutine = $one_more[3];
        $subroutine = '' unless defined($subroutine);
        $subroutine .= '()' if $subroutine ne '';
        
        print $fh '*' x 60, "\n", "$time:$filename:$line:$subroutine\n", $str, "\n";
    }

    sub _getCurDateTime {
        my ($self) = @_;
        
        my $time = time();
        my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($time);
        $mon += 1;
        $year += 1900;
        my $date = sprintf "%04d-%02d-%02d %02d:%02d:%02d", $year, $mon, $mday,
            $hour, $min, $sec;
        
        return $date;
    }

    
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

    sub _moduleHasSub {
        my ($self, $module, $sub_name) = @_;
    }
    
    sub DESTROY {
        my ($self) = @_;
        return undef unless $self->_getDisconnect;
        my $dbh = $self->_getDatabaseHandle;
        $dbh->disconnect if $dbh;
    }

    #################
    # getters/setters

    sub getNameArg {
        my ($self) = @_;
        my $arg = $$self{_name_arg};
        $arg = 'NAME_lc' unless defined($arg) and $arg ne '';

        return $arg;
    }

=pod

=head2 setNameArg($arg)

This is the argument to pass to the fetchrow_hashref() call on
the underlying DBI object.  By default, this is 'NAME_lc', so
that all field names returned are all lowercase to provide for
portable code.  If you want to make all the field names return be
uppercase, call $db->setNameArg('NAME_uc') after the connect()
call.  And if you really want the case of the field names to be
what the underlying database driveer returns them as, call
$db->setNameArg('NAME').

=cut
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

    sub _getDisconnect {
        my ($self) = @_;
        return $$self{_should_disconnect};
    }

    # whether or not to disconnect when the Wrapper object is
    # DESTROYed
    sub _setDisconnect {
        my ($self, $val) = @_;
        $$self{_should_disconnect} = 1;
    }

=pod

=head2 commit()

Calls commit() on the underlying DBI object to commit your
transactions.

=cut
    sub commit {
        my ($self) = @_;
        my $dbh = $self->_getDatabaseHandle;
        if ($dbh) {
            return $dbh->commit;
        }
        return undef;
    }


    ###############################################################################
    # stuff that I don't know if I want to make public yet

    # MySQL specific.  May be able to generalize to other dbs
    # with using serial types if given table name and column name
    sub getLastInsertId {
        my ($self, $table_name, $col_name) = @_;
        my $query = qq{SELECT LAST_INSERT_ID() AS id};
        my $row = $self->nativeSelect($query);
        if ($row and %$row) {
            return $$row{id};
        }
        return undef;
    }

}

1;

__END__

=pod

=head2 There are also underscore_separated versions of these methods.

    E.g., nativeSelectLoop() becomes native_select_loop()


=head1 TODO

=over 4

=item Allow creation from existing DBI handle.

=item Logging

=item Allow prepare() and execute()

=back

=head1 ACKNOWLEDGEMENTS

    People who have contributed ideas and/or code for this module:

    Mark Stosberg
    Kevin Wilson

=head1 AUTHOR

    Don Owens <don@owensnet.com>

=head1 COPYRIGHT

    Copyright (c) 2003-2004 Don Owens

    All rights reserved. This program is free software; you can
    redistribute it and/or modify it under the same terms as Perl
    itself.

=head1 VERSION

    0.07

=cut
