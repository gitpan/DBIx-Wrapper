# -*-perl-*-
# Creation date: 2003-03-30 12:17:42
# Authors: Don
# Change log:
# $Id: Wrapper.pm,v 1.33 2004/07/01 06:37:11 don Exp $
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

 my $dbi_obj = DBI->connect(...)
 my $db = DBIx::Wrapper->newFromDBI($dbi_obj);

 my $dbi_obj = $db->getDBI;

 my $rv = $db->insert($table, { id => 5, val => "myval",
                                the_date => \"NOW()"
                              });
 my $rv = $db->replace($table, \%data);
 my $rv = $db->delete($table, \%keys);
 my $rv = $db->update($table, \%keys, \%data);
 my $rv = $db->smartUpdate($table, \%keys, \%data);

 my $row = $db->selectFromHash($table, \%keys);
 my $row = $db->nativeSelect($query, \@exec_args);

 my $loop = $db->nativeSelectExecLoop($query);
 foreach my $val (@vals) {
     my $row = $loop->next([ $val ]);
 }

 my $row = $db->nativeSelectWithArrayRef($query, \@exec_args);

 my $rows = $db->nativeSelectMulti($query, \@exec_args);

 my $loop = $db->nativeSelectMultiExecLoop($query)
 foreach my $val (@vals) {
     my $rows = $loop->next([ $val ]);
 }

 my $rows = $db->nativeSelectMultiWithArrayRef($query, \@exec_args);

 my $hash = $db->nativeSelectMapping($query, \@exec_args);
 my $hash = $db->nativeSelectDynaMapping($query, \@cols, \@exec_args);

 my $hash = $db->nativeSelectRecordMapping($query, \@exec_args);
 my $hash = $db->nativeSelectRecordDynaMapping($query, $col, \@exec_args);

 my $val = $db->nativeSelectValue($query, \@exec_args);

 my $row = $db->abstractSelect($table, \@fields, \%where, \@order);
 my $rows = $db->abstractSelectMulti($table, \@fields, \%where, \@order);

 my $loop = $db->nativeSelectLoop($query, @exec_args);

 my $rv = $db->nativeQuery($query, @exec_args);

 my $loop = $db->nativeQueryLoop("UPDATE my_table SET value=? WHERE id=?");
 $loop->next([ 'one', 1]);
 $loop->next([ 'two', 2]);

 my $id = $db->getLastInsertId;

 $db->debugOn(\*FILE_HANDLE);

 $db->setNameArg($arg)

 $db->commit();
 $db->ping();
 $db->err();

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
        $VERSION = '0.11'; # update below in POD as well
    }

    use DBI;
    use DBIx::Wrapper::SQLCommand;
    use DBIx::Wrapper::Statement;
    use DBIx::Wrapper::SelectLoop;
    use DBIx::Wrapper::SelectExecLoop;
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
            # FIXME: make a way to set debug level here
            # $self->addDebugLevel(2); # print on error
        }
        unless ($dbh) {
            if ($self->_isDebugOn) {
                $self->_printDebug(Carp::longmess($DBI::errstr));
            } else {
                $self->_printDbiError
                    if not defined($$attr{PrintError}) or $$attr{PrintError};
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
    *getDbi = \&getDBI;

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

=head2 smartReplace($table, \%data)

 This method is MySQL specific.  If $table has an auto_increment
 column, the return value will be the value of the auto_increment
 column.  So if that column was specified in \%data, that value
 will be returned, otherwise, an insert will be performed and the
 value of LAST_INSERT_ID() will be returned.  If there is no
 auto_increment column, but primary keys are provided, the row
 containing the primary keys will be returned.  Otherwise, a true
 value will be returned upon success.

=cut
    sub smartReplace {
        my ($self, $table, $data, $keys) = @_;

        if (0 and $keys) {
            # ignore $keys for now
            
        } else {
            my $dbh = $self->_getDatabaseHandle;
            my $query = qq{DESCRIBE $table};
            my $sth = $self->_getStatementHandleForQuery($query);
            return $sth unless $sth;
            my $auto_incr = undef;
            my $key_list = [];
            my $info_list = [];
            while (my $info = $sth->fetchrow_hashref('NAME_lc')) {
                push @$info_list, $info;
                push @$key_list, $$info{field} if lc($$info{key}) eq 'pri';
                if ($$info{extra} =~ /auto_increment/i) {
                    $auto_incr = $$info{field};
                }
            }

            my $orig_auto_incr = $auto_incr;
            $auto_incr = lc($auto_incr);
            my $keys_provided = [];
            my $key_hash = { map { (lc($_) => 1) } @$key_list };
            my $auto_incr_provided = 0;
            foreach my $key (keys %$data) {
                push @$keys_provided, $key if exists($$key_hash{lc($key)});
                if (lc($key) eq $auto_incr) {
                    $auto_incr_provided = 1;
                    last;
                }
            }

            if (@$keys_provided) {
                # do replace and return the value of this field
                my $rv = $self->replace($table, $data);
                return $rv unless $rv;
                if ($orig_auto_incr eq '') {
                    my %hash = map { ($_ => $$data{$_}) } @$keys_provided;
                    my $row = $self->selectFromHash($table, \%hash);
                    return $row if $row and %$row;
                    return undef;
                } else {
                    return $$data{$orig_auto_incr};
                }
            } else {
                # do insert and return last insert id
                my $rv = $self->insert($table, $data);
                return $rv unless $rv;
                if ($orig_auto_incr eq '') {
                    # FIXME: what do we do here?
                    return 1;
                } else {
                    my $id = $self->getLastInsertId(undef, undef, $table, $orig_auto_incr);
                    return $id;
                }
            }
        }
    }

=pod

=head2 delete($table, \%keys), delete($table, \@keys)

 Delete rows from table $table using the key/value pairs in %keys
 to specify the WHERE clause of the query.  Multiple key/value
 pairs are joined with 'AND' in the WHERE clause.  The cols
 parameter can optionally be an array ref instead of a hashref.
 E.g.

     $db->delete($table, [ key1 => $val1, key2 => $val2 ])

 This is so that the order of the parameters in the WHERE clause
 are kept in the same order.  This is required to use the correct
 multi field indexes in some databases.

=cut
    sub delete {
        my ($self, $table, $keys) = @_;

        unless ($keys and (UNIVERSAL::isa($keys, 'HASH') or UNIVERSAL::isa($keys, 'ARRAY'))) {
            return $self->setErr(-1, 'DBIx::Wrapper: No keys passed to update()');
        }

        my @keys;
        my @values;
        if (ref($keys) eq 'ARRAY') {
            # allow this to maintain order in the WHERE clause in
            # order to use the right indexes
            my @copy = @$keys;
            while (my $key = shift @copy) {
                push @keys, $key;
                my $val = shift @copy; # shift off the value
            }
            $keys = { @$keys };
        } else {
            @keys = keys %$keys;
        }
        push @values, @$keys{@keys};

        my $where = join(" AND ", map { "$_=?" } @keys);
        my $query = qq{DELETE FROM $table WHERE $where};

        my $sth = $self->_getStatementHandleForQuery($query, \@values);
        return $sth unless $sth;
        $sth->finish;
        
        return 1;
    }

=pod

=head2 update($table, \%keys, \%data), update($table, \@keys, \%data)

 Update the table using the key/value pairs in %keys to specify
 the WHERE clause of the query.  %data contains the new values
 for the row(s) in the database.  The keys parameter can
 optionally be an array ref instead of a hashref.  E.g.,

     $db->update($table, [ key1 => $val1, key2 => $val2 ], \%data);

 This is so that the order of the parameters in the WHERE clause
 are kept in the same order.  This is required to use the correct
 multi field indexes in some databases.

=cut
    sub update {
        my ($self, $table, $keys, $data) = @_;

        unless ($keys and (UNIVERSAL::isa($keys, 'HASH') or UNIVERSAL::isa($keys, 'ARRAY'))) {
            return $self->setErr(-1, 'DBIx::Wrapper: No keys passed to update()');
        }

        unless ($data and UNIVERSAL::isa($data, 'HASH')) {
            return $self->setErr(-1, 'DBIx::Wrapper: No values passed to update()');
        }
        
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

        my @keys;
        if (ref($keys) eq 'ARRAY') {
            # allow this to maintain order in the WHERE clause in
            # order to use the right indexes
            my @copy = @$keys;
            while (my $key = shift @copy) {
                push @keys, $key;
                my $val = shift @copy; # shift off the value
            }
            $keys = { @$keys };
        } else {
            @keys = keys %$keys;
        }
        push @values, @$keys{@keys};

        my $set = join(",", @set);
        my $where = join(" AND ", map { "$_=?" } @keys);

        my $query = qq{UPDATE $table SET $set WHERE $where};
        my ($sth, $rv) = $self->_getStatementHandleForQuery($query, \@values);
        return $sth unless $sth;
        $sth->finish;
        
        return $rv;
    }

=pod

=head2 selectFromHash($table, \%keys);

 Select from table $table using the key/value pairs in %keys to
 specify the WHERE clause of the query.  Multiple key/value pairs
 are joined with 'AND' in the WHERE clause.  Returns a single row
 as a hashref.

=cut
    sub selectFromHash {
        my ($self, $table, $keys, $extra) = @_;
        my @keys = keys %$keys;
        my $where = join(" AND ", map { "$_=?" } @keys);

        my $query = qq{SELECT * FROM $table WHERE $where};
        my $sth = $self->_getStatementHandleForQuery($query, [ @$keys{@keys} ]);
        my $info = $sth->fetchrow_hashref;
        my $rv;
        if ($info and %$info) {
            $rv = $info; 
        } else {
            $rv = undef;
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

    sub _getStatementHandleForQuery {
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
            return wantarray ? ($self->setErr(0, $dbh->errstr), undef)
                : $self->setErr(0, $dbh->errstr);
        }

        my $rv = $sth->execute(@$exec_args);
        unless ($rv) {
            if ($self->_isDebugOn) {
                $self->_printDebug(Carp::longmess($dbh->errstr) . "\nQuery was '$query'\n");
            } else {
                $self->_printDbiError("\nQuery was '$query'\n");
            }
            return wantarray ? ($self->setErr(1, $dbh->errstr), undef)
                : $self->setErr(1, $dbh->errstr);
        }

        return wantarray ? ($sth, $rv) : $sth;
    }

=pod

=head2 nativeSelect($query, \@exec_args)

Executes the query in $query and returns a single row result (as
a hash ref).  If there are multiple rows in the result, the rest
get silently dropped.  @exec_args are the same arguments you
would pass to an execute() called on a DBI object.  Returns undef
on error.

=cut
    sub nativeSelect {
        my ($self, $query, $exec_args) = @_;

        my $sth;
        if (scalar(@_) == 3) {
            $sth = $self->_getStatementHandleForQuery($query, $exec_args);
        } else {
            $sth = $self->_getStatementHandleForQuery($query);
        }
        
        return $sth unless $sth;
        
        my $result = $sth->fetchrow_hashref($self->getNameArg);
        $sth->finish;

        return $result; 
   }

    *read = \&nativeSelect;
    *native_select = \&nativeSelect;

=pod

=head2 nativeSelectExecLoop($query)

 Like nativeSelect(), but returns a loop object that can be used
 to execute the same query over and over with different bind
 parameters.  This does a single DBI prepare() instead of a new
 prepare() for select.

 E.g.,

     my $loop = $db->nativeSelectExecLoop("SELECT * FROM mytable WHERE id=?");
     foreach my $id (@ids) {
         my $row = $loop->next([ $id ]);
     }

=cut    
    # added for v 0.08
    sub nativeSelectExecLoop {
        my ($self, $query) = @_;
        return DBIx::Wrapper::SelectExecLoop->new($self, $query);
    }
    *native_select_exec_loop = \&nativeSelectExecLoop;

=pod

=head2 nativeSelectWithArrayRef($query, \@exec_args)

 Like nativeSelect(), but return a reference to an array instead
 of a hash.  Returns undef on error.  If there are no results
 from the query, a reference to an empty array is returned.

=cut
    sub nativeSelectWithArrayRef {
        my ($self, $query, $exec_args) = @_;

        my $sth;
        if (scalar(@_) == 3) {
            $sth = $self->_getStatementHandleForQuery($query, $exec_args);
        } else {
            $sth = $self->_getStatementHandleForQuery($query);
        }
        
        return $sth unless $sth;
        
        my $result = $sth->fetchrow_arrayref;
        $sth->finish;

        return [] unless $result and ref($result) =~ /ARRAY/;
        
        # have to make copy because recent version of DBI now
        # return the same array reference each time
        return [ @$result ];
    }
    *native_select_with_array_ref = \&nativeSelectArrayWithArrayRef;

=pod

=head2 nativeSelectMulti($query, \@exec_args)

 Executes the query in $query and returns an array of rows, where
 each row is a hash representing a row of the result.  Returns
 undef on error.  If there are no results for the query, an empty
 array ref is returned.

=cut
    sub nativeSelectMulti {
        my ($self, $query, $exec_args) = @_;

        my $sth;
        if (scalar(@_) == 3) {
            $sth = $self->_getStatementHandleForQuery($query, $exec_args);
        } else {
            $sth = $self->_getStatementHandleForQuery($query);
        }
        return $sth unless $sth;
        
        my $rows = [];
        while (my $row = $sth->fetchrow_hashref($self->getNameArg)) {
            push @$rows, $row;
        }
        my $col_names = $sth->{$self->getNameArg};
        $$self{_last_col_names} = $col_names;
        $sth->finish;

        return $rows;
    }

    *readArray = \&nativeSelectMulti;
    *native_select_multi = \&nativeSelectMulti;

=pod

=head2 nativeSelectMultiExecLoop($query)

 Like nativeSelectExecLoop(), but returns an array of rows, where
 each row is a hash representing a row of the result.

=cut
    sub nativeSelectMultiExecLoop {
        my ($self, $query) = @_;
        return DBIx::Wrapper::SelectExecLoop->new($self, $query, 1);
    }
    *native_select_multi_exec_loop = \&nativeSelectMultiExecLoop;

=pod

=head2 nativeSelectMultiWithArrayRef($query, \@exec_args)

 Like nativeSelectMulti(), but return a reference to an array of
 arrays instead of to an array of hashes.  Returns undef on error.

=cut
    
    sub nativeSelectMultiWithArrayRef {
        my ($self, $query, $exec_args) = @_;

        my $sth;
        if (scalar(@_) == 3) {
            $sth = $self->_getStatementHandleForQuery($query, $exec_args);
        } else {
            $sth = $self->_getStatementHandleForQuery($query);
        }
        
        return $sth unless $sth;
        
        my $list = [];
       
        while (my $result = $sth->fetchrow_arrayref()) {
            # have to make copy because recent version of DBI now
            # return the same array reference each time
            push @$list, [ @$result ];
        }
        $sth->finish;

        return $list;
    }
    *native_select_multi_with_array_ref = \&nativeSelectMultiWithArrayRef;

=pod

=head2 nativeSelectMapping($query, \@exec_args)

 Executes the given query and returns a reference to a hash
 containing the first and second columns of the results as
 key/value pairs.

=cut
    sub nativeSelectMapping {
        my ($self, $query, $exec_args) = @_;
        if (scalar(@_) == 3) {
            $self->nativeSelectDynaMapping($query, [ 0, 1 ], $exec_args);
        } else {
            $self->nativeSelectDynaMapping($query, [ 0, 1 ]);
        }
    }
    *native_select_mapping = \&nativeSelectMapping;

=pod

=head2 nativeSelectDynaMapping($query, \@cols, \@exec_args)

 Similar to nativeSelectMapping() except you specify which
 columns to use for the key/value pairs in the return hash.  If
 the first element of @cols starts with a digit, then @cols is
 assumed to contain indexes for the two columns you wish to use.
 Otherwise, @cols is assumed to contain the field names for the
 two columns you wish to use.

 For example,

     nativeSelectMapping($query, \@exec_args) is

  equivalent (and in fact calls) to

     nativeSelectDynaMapping($query, [ 0, 1 ], $exec_args).

=cut
    # FIXME: return undef on error
    sub nativeSelectDynaMapping {
        my ($self, $query, $cols, $exec_args) = @_;

        my ($first, $second) = @$cols;
        my $map = {};
        if ($first =~ /^\d/) {
            my $rows;
            if ($exec_args and @$exec_args) {
                $rows = $self->nativeSelectMultiWithArrayRef($query, $exec_args);
            } else {
                $rows = $self->nativeSelectMultiWithArrayRef($query);
            }
            foreach my $row (@$rows) {
                $$map{$$row[$first]} = $$row[$second];
            }

        } else {
            my $rows;
            if ($exec_args and @$exec_args) {
                $rows = $self->nativeSelectMulti($query, $exec_args);
            } else {
                $rows = $self->nativeSelectMulti($query);
            }
            foreach my $row (@$rows) {
                $$map{$$row{$first}} = $$row{$second};
            }
        }
        
        return $map;
    }
    *native_select_dyna_mapping = \&nativeSelectDynaMapping;

=pod

=head2 nativeSelectRecordMapping($query, \@exec_args)

 Similar to nativeSelectMapping(), except the values in the hash
 are references to the corresponding record (as a hash).

=cut
    sub nativeSelectRecordMapping {
        my ($self, $query, $exec_args) = @_;

        if ($exec_args and @$exec_args) {
            return $self->nativeSelectRecordDynaMapping($query, 0, $exec_args);
        } else {
            return $self->nativeSelectRecordDynaMapping($query, 0);
        }
    }
    *native_select_record_mapping = \&nativeSelectRecordMapping;

=pod

=head2 nativeSelectRecordDynaMapping($query, $col, \@exec_args)

 Similar to nativeSelectRecordMapping(), except you specify
 which column is the key in each key/value pair in the hash.  If
 $col starts with a digit, then it is assumed to contain the
 index for the column you wish to use.  Otherwise, $col is
 assumed to contain the field name for the two columns you wish
 to use.

=cut
    # FIXME: return undef on error
    sub nativeSelectRecordDynaMapping {
        my ($self, $query, $col, $exec_args) = @_;

        my $map = {};
        if ($col =~ /^\d/) {
            my $rows;
            if ($exec_args and @$exec_args) {
                $rows = $self->nativeSelectMulti($query, $exec_args);
            } else {
                $rows = $self->nativeSelectMulti($query);
            }
            my $names = $$self{_last_col_names};
            my $col_name = $$names[$col];
            foreach my $row (@$rows) {
                $$map{$$row{$col_name}} = $row;
            }

        } else {
            my $rows;
            if ($exec_args and @$exec_args) {
                $rows = $self->nativeSelectMulti($query, $exec_args);
            } else {
                $rows = $self->nativeSelectMulti($query);
            }
            foreach my $row (@$rows) {
                $$map{$$row{$col}} = $row;
            }
        }

        return $map;
    }
    *native_select_record_dyna_mapping = \&nativeSelectRecordDynaMapping;
    
    sub _getSqlObj {
        # return SQL::Abstract->new(case => 'textbook', cmp => '=', logic => 'and');
        require SQL::Abstract;
        return SQL::Abstract->new(case => 'textbook', cmp => '=');
    }

=pod

=head2 nativeSelectValue($query, \@exec_args)

 Returns a single value, the first column from the first row of
 the result.  Returns undef on error or if there are no rows in
 the result.  Note this may be the same value returned for a NULL
 value in the result.

=cut        
    sub nativeSelectValue {
        my ($self, $query, $exec_args) = @_;
        my $row;
        if ($exec_args and UNIVERSAL::isa($exec_args, 'ARRAY')) {
            $row = $self->nativeSelectWithArrayRef($query, $exec_args);
        } else {
            $row = $self->nativeSelectWithArrayRef($query);
        }
        if ($row and @$row) {
            return $row->[0];
        }

        return undef;
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

    To get the number of rows selected, you can call the
    rowCountCurrent() method on the loop object, e.g.,

    my $loop = $db->nativeSelectLoop("SELECT * FROM my_table");
    my $rows_in_result = $loop->rowCountCurrent;

    The count() method is an alias for rowCountCurrent().


    To get the number of rows returned by next() so far, use the
    rowCountTotal() method.


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

        my $sth;
        my $rv;
        if (scalar(@_) == 3) {
            ($sth, $rv) = $self->_getStatementHandleForQuery($query, $exec_args);
        } else {
            ($sth, $rv) = $self->_getStatementHandleForQuery($query);
        }
        return $sth unless $sth;
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
        return $$self{_err_num};
    }

=pod

=head2 err()

Calls err() on the underlying DBI object, which returns the
native database engine error code from the last driver method
called.

=cut
    sub err {
        my ($self) = @_;
        my $dbh = $self->_getDatabaseHandle;
        return $dbh->err if $dbh;
        return 0;
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

=pod

=head2 ping()

Calls ping() on the underlying DBI object to see if the database
connection is still up.

=cut
    sub ping {
        my ($self) =@_;
        my $dbh = $self->_getDatabaseHandle;
        return undef unless $dbh;

        return $dbh->ping;
    }

# =pod

# =head2 getLastInsertId($catalog, $schema, $table, $field, \%attr)

# Returns a value identifying the row just inserted, if possible.
# If using DBI version 1.38 or later, this method calls
# last_insert_id() on the underlying DBI object.  Otherwise, does a
# "SELECT LAST_INSERT_ID()", which is MySQL specific.  The
# parameters passed to this method are driver-specific.  See the
# documentation on DBI for details.

# get_last_insert_id() and last_insert_id() are aliases for this
# method.

# =cut

    # bah, DBI's last_insert_id is not working for me, so for
    # now this will be MySQL only

=pod

=head2 getLastInsertId(), get_last_insert_id(), last_insert_id()

 Returns the last_insert_id.  This is MySQL specific for now.  It
 just runs the query "SELECT LAST_INSERT_ID()".

=cut
    sub getLastInsertId {
        my ($self, $catalog, $schema, $table, $field, $attr) = @_;
        if (0 and DBI->VERSION >= 1.38) {
            my $dbh = $self->_getDatabaseHandle;
            return $dbh->last_insert_id($catalog, $schema, $table, $field, $attr);
        } else {
            my $query = qq{SELECT LAST_INSERT_ID()};
            my $row = $self->nativeSelectWithArrayRef($query);
            if ($row and @$row) {
                return $$row[0];
            }
            return undef;
        }
    }
    *get_last_insert_id = \&getLastInsertId;
    *last_insert_id = \&getLastInsertId;

}

1;

__END__

=pod

=head2 There are also underscore_separated versions of these methods.

    E.g., nativeSelectLoop() becomes native_select_loop()


=head1 TODO

=over 4

=item More logging/debugging options

=item Allow prepare() and execute() for easier integration into existing code.

=back

=head1 ACKNOWLEDGEMENTS

    People who have contributed ideas and/or code for this module:

    Kevin Wilson
    Mark Stosberg

=head1 AUTHOR

    Don Owens <don@owensnet.com>

=head1 COPYRIGHT

    Copyright (c) 2003-2004 Don Owens

    All rights reserved. This program is free software; you can
    redistribute it and/or modify it under the same terms as Perl
    itself.

=head1 VERSION

    0.11

=cut
