# -*-perl-*-
# Creation date: 2003-03-30 12:17:42
# Authors: Don
# Change log:
# $Id: Wrapper.pm,v 1.53 2005/04/21 22:07:19 don Exp $
#
# Copyright (c) 2003-2005 Don Owens
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

 my $db = DBIx::Wrapper->connect($dsn, $user, $auth, \%attr,
          { error_handler => sub { print $DBI::errstr },
            debug_handler => sub { print $DBI::errstr },
          });

 my $dbi_obj = DBI->connect(...)
 my $db = DBIx::Wrapper->newFromDBI($dbi_obj);

 my $dbi_obj = $db->getDBI;

 my $rv = $db->insert($table, { id => 5, val => "myval",
                                the_date => \"NOW()",
                              });
 my $rv = $db->insert($table, { id => 5, val => "myval",
                                the_date => $db->command("NOW()"),
                              });

 my $rv = $db->replace($table, \%data);
 my $rv = $db->smartReplace($table, \%data)
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
 while (my $row = $loop->next) {
     my $id = $$row{id};
 }

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
a query with a single method call as well as make inserts easier,
etc.  It also supports running hooks at various stages of
processing a query (see the section on Hooks).

=head1 METHODS

=cut

use strict;

{   package DBIx::Wrapper;

    # use 5.006; # should have at least Perl 5.6.0
    
    use Carp ();
    
    use vars qw($VERSION $AUTOLOAD $Heavy);
    $Heavy = 0;

    BEGIN {
        $VERSION = '0.16'; # update below in POD as well
    }

    use DBI;
    use DBIx::Wrapper::Request;
    use DBIx::Wrapper::SQLCommand;
    use DBIx::Wrapper::Statement;
    use DBIx::Wrapper::SelectLoop;
    use DBIx::Wrapper::SelectExecLoop;
    use DBIx::Wrapper::StatementLoop;
    use DBIx::Wrapper::Delegator;

    sub _new {
        my ($proto) = @_;
        my $self = bless {}, ref($proto) || $proto;
        return $self;
    }

    sub import {
        my $class = shift;

        foreach my $e (@_) {
            if ($e eq ':heavy') {
                $Heavy = 1;
            }
        }
    }

=pod

=head2 connect($data_source, $username, $auth, \%attr, \%params)

Connects to the given database.  The first four parameters are
the same parameters you would pass to the connect call when using
DBI directly.

The %params hash is optional and contains extra parameters to
control the behaviour of DBIx::Wrapper itself.  Following are the
valid parameters.

=over 4

=item error_handler and debug_handler

These values should either be a reference to a subroutine, or a
reference to an array whose first element is an object and whose
second element is a method name to call on that object.  The
parameters passed to the error_handler callback are the current
DBIx::Wrapper object and an error string, usually the query if
appropriate.  The parameters passed to the debug_handler callback
are the current DBIx::Wrapper object, an error string, and the
filehandle passed to the debugOn() method (defaults to STDERR).
E.g.,

  sub do_error {
      my ($db, $str) = @_;
      print $DBI::errstr;
  }
  sub do_debug {
      my ($db, $str, $fh) = @_;
      print $fh "query was: $str\n";
  }

  my $db = DBIx::Wrapper->connect($ds, $un, $auth, \%attr,
                                  { error_handler => \&do_error,
                                    debug_handler => \&do_debug,
                                  });


=item db_style

Used to control some database specific logic.  The default value
is 'mysql'.  Currently, this is only used for the
getLastInsertId() method.  MSSQL is supported with a value of
mssql for this parameter.

=item heavy

If set to a true value, any hashes returned will actually be
objects on which you can call methods to get the values back.  E.g.,

  my $row = $db->nativeSelect($query);
  my $id = $row->id;
  or
  my $id = $row->{id};

=back


=head2 new($data_source, $username, $auth, \%attr, \%params)

An alias for connect().

=cut
    sub connect {
        my ($proto, $data_source, $username, $auth, $attr, $params) = @_;
        my $self = $proto->_new;

        my $dsn = $data_source;
        $dsn = $self->_getDsnFromHash($data_source) if ref($data_source) eq 'HASH';

        my $dbh = DBI->connect($dsn, $username, $auth, $attr);
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

        $params = {} unless UNIVERSAL::isa($params, 'HASH');
        
        $self->_setDatabaseHandle($dbh);
        $self->_setDataSource($data_source);
        $self->_setDataSourceStr($dsn);
        $self->_setUsername($username);
        $self->_setAuth($auth);
        $self->_setAttr($attr);
        $self->_setDisconnect(1);

        $self->_setErrorHandler($params->{error_handler}) if $params->{error_handler};
        $self->_setDebugHandler($params->{debug_handler}) if $params->{debug_handler};
        $self->_setDbStyle($params->{db_style}) if exists($params->{db_style});
        $self->_setHeavy(1) if $params->{heavy};
        
        my ($junk, $dbd_driver, @rest) = split /:/, $dsn;
        $self->_setDbdDriver(lc($dbd_driver));

        return $self;
    }

    *new = \&connect;

=pod

=head2 reconnect()

Reconnect to the database using the same parameters that were
given to the connect() method.  It does not try to disconnect
before attempting to connect again.

=cut
    sub reconnect {
        my $self = shift;

        my $dsn = $self->_getDataSourceStr;

        my $dbh = DBI->connect($dsn, $self->_getUsername, $self->_getAuth,
                               $self->_getAttr);
        if ($dbh) {
            $self->_setDatabaseHandle($dbh);
            return $self;
        } else {
            return undef;
        }
    }

=pod

=head2 disconnect()

Disconnect from the database.  This disconnects and frees up the
underlying DBI object.

=cut
    sub disconnect {
        my $self = shift;
        my $dbi_obj = $self->{_dbh};
        $dbi_obj->disconnect if $dbi_obj;
        delete $self->{_dbh};

        return 1;
    }
    
    sub _getDsnFromHash {
        my $self = shift;
        my $data_source = shift;
        my @dsn;
        
        push @dsn, "database=$$data_source{database}" if $data_source->{database};
        push @dsn, "host=$$data_source{host}" if $data_source->{host};
        push @dsn, "port=$$data_source{port}" if $data_source->{port};

        my $driver = $data_source->{driver} || $data_source->{type};
        return "dbi:$driver:" . join(';', @dsn);
    }

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
        my ($sth, $rv) = $self->_getStatementHandleForQuery($query, \@values);
        return $sth unless $sth;
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
        my $style = lc($self->_getDbStyle);
        if ($style eq 'mssql') {
            # mssql doesn't support replace, so do an insert instead
            return $self->_insert_replace('INSERT', $table, $data);
        } else {
            return $self->_insert_replace('REPLACE', $table, $data);
        }
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

    *smart_replace = \&smartReplace;

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

        unless (%$data) {
            return "0E";
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
        unless (ref($data) eq 'HASH' and %$data) {
            return "0E";
        }
        
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

    sub _runHandler {
        my ($self, $handler_info, @args) = @_;
        return undef unless ref($handler_info);

        my ($handler, $custom_args) = @$handler_info;
        $custom_args = [] unless $custom_args;
        
        unshift @args, $self;
        if (ref($handler) eq 'ARRAY') {
            my $method = $handler->[1];
            $handler->[0]->$method(@args, @$custom_args);
        } else {
            $handler->(@args, @$custom_args);
        }

        return 1;
    }

    sub _runHandlers {
        my ($self, $handlers, $r) = @_;
        return undef unless $handlers;

        my $rv = $r->OK;
        foreach my $handler_info (reverse @$handlers) {
            my ($handler, $custom_args) = @$handler_info;
            $custom_args = [] unless $custom_args;
            
            if (ref($handler) eq 'ARRAY') {
                my $method = $handler->[1];
                $rv = $handler->[0]->$method($r);
            } else {
                $rv = $handler->($r);
            }
            last unless $rv == $r->DECLINED;
        }

        return $rv;
    }



    sub _defaultPrePrepareHandler {
        my $r = shift;
        return $r->OK;
    }

    sub _defaultPostPrepareHandler {
        my $r = shift;
        return $r->OK;
    }

    sub _defaultPreExecHandler {
        my $r = shift;
        return $r->OK;
    }

    sub _defaultPostExecHandler {
        my $r = shift;
        return $r->OK;
    }

    sub _defaultPreFetchHandler {
        my $r = shift;
        return $r->OK;
    }
    
    sub _defaultPostFetchHandler {
        my $r = shift;
        return $r->OK;
    }

    sub _runGenericHook {
        my ($self, $r, $default_handler, $custom_handler_field) = @_;
        my $handlers = [ $default_handler ];
        
        if ($self->shouldBeHeavy) {
            if ($custom_handler_field eq '_post_fetch_hooks') {
                push @$handlers, [ \&_heavyPostFetchHook ];
            }
        }
        
        my $custom_handlers = $self->{$custom_handler_field};
        push @$handlers, @$custom_handlers if $custom_handlers;

        return $self->_runHandlers($handlers, $r);
    }

    sub _runPrePrepareHook {
        my $self = shift;
        my $r = shift;
        my $handlers = [ [ \&_defaultPrePrepareHandler ] ];
        my $custom_handlers = $self->{_pre_prepare_hooks};
        push @$handlers, @$custom_handlers if $custom_handlers;
                
        return $self->_runHandlers($handlers, $r);
    }

    sub _runPostPrepareHook {
        my $self = shift;
        my $r = shift;
        my $handlers = [ [ \&_defaultPostPrepareHandler ] ];
        my $custom_handlers = $self->{_post_prepare_hooks};
        push @$handlers, @$custom_handlers if $custom_handlers;
                
        return $self->_runHandlers($handlers, $r);
    }

    sub _runPreExecHook {
        my $self = shift;
        my $r = shift;
        my $handlers = [ [ \&_defaultPreExecHandler ] ];
        my $custom_handlers = $self->{_pre_exec_hooks};
        push @$handlers, @$custom_handlers if $custom_handlers;
                
        return $self->_runHandlers($handlers, $r);
    }

    sub _runPostExecHook {
        my $self = shift;
        my $r = shift;
        return $self->_runGenericHook($r, [ \&_defaultPostExecHandler ], '_post_exec_hooks');
    }

    sub _runPreFetchHook {
        my $self = shift;
        my $r = shift;
        return $self->_runGenericHook($r, [ \&_defaultPreFetchHandler ], '_pre_fetch_hooks');
    }

    sub _runPostFetchHook {
        my $self = shift;
        my $r = shift;
        return $self->_runGenericHook($r, [ \&_defaultPostFetchHandler ],
                                         '_post_fetch_hooks');
    }

    sub _heavyPostFetchHook {
        my $r = shift;
        my $row = $r->getReturnVal;

        if (ref($row) eq 'HASH') {
            $r->setReturnVal(bless($row, 'DBIx::Wrapper::Delegator'));
        } elsif (ref($row) eq 'ARRAY') {
            # do nothing for now
        }
    }

    sub _getStatementHandleForQuery {
        my ($self, $query, $exec_args) = @_;
        
        if (scalar(@_) == 3) {
            $exec_args = [ $exec_args ] unless ref($exec_args);
        }
        $exec_args = [] unless $exec_args;

        $self->_printDebug($query);

        my $r = DBIx::Wrapper::Request->new($self);
        $r->setQuery($query);
        $r->setExecArgs($exec_args);
        
        $self->_runPrePrepareHook($r);
        $query = $r->getQuery;
        $exec_args = $r->getExecArgs;
        
        my $dbh = $self->_getDatabaseHandle;
        my $sth = $dbh->prepare($query);

        $r->setStatementHandle($sth);
        $r->setErrorStr($sth ? $dbh->errstr : '');
        $self->_runPostPrepareHook($r);
        
        unless ($sth) {
            if ($self->_isDebugOn) {
                $self->_printDebug(Carp::longmess($dbh->errstr) . "\nQuery was '$query'\n");
            } else {
                $self->_printDbiError("\nQuery was '$query'\n");
            }
            return wantarray ? ($self->setErr(0, $dbh->errstr), undef)
                : $self->setErr(0, $dbh->errstr);
        }

        $r->setQuery($query);
        $r->setExecArgs($exec_args);

        $self->_runPreExecHook($r);

        $exec_args = $r->getExecArgs;
        
        my $rv = $sth->execute(@$exec_args);
        
        $r->setExecReturnValue($rv);
        $r->setErrorStr($rv ? '' : $dbh->errstr);
        $self->_runPostExecHook($r);
        $rv = $r->getExecReturnValue;
        $sth = $r->getStatementHandle;
        
        unless ($rv) {
            if ($self->_isDebugOn) {
                $self->_printDebug(Carp::longmess($dbh->errstr) . "\nQuery was '$query'\n");
            } else {
                $self->_printDbiError("\nQuery was '$query'\n");
            }
            return wantarray ? ($self->setErr(1, $dbh->errstr), undef)
                : $self->setErr(1, $dbh->errstr);
        }

        return wantarray ? ($sth, $rv, $r) : $sth;
    }

    sub prepare_no_hooks {
        my $self = shift;
        my $query = shift;

        my $dbi_obj = $self->getDBI;
        my $sth = $dbi_obj->prepare($query);

        return $sth;
    }
    *prepare_no_handlers = \&prepare_no_hooks;


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

        my ($sth, $rv, $r);
        if (scalar(@_) == 3) {
            ($sth, $rv, $r) = $self->_getStatementHandleForQuery($query, $exec_args);
        } else {
            ($sth, $rv, $r) = $self->_getStatementHandleForQuery($query);
        }
        
        return $sth unless $sth;

        $self->_runPreFetchHook($r);
        $sth = $r->getStatementHandle;
        
        my $result = $sth->fetchrow_hashref($self->getNameArg);
        
        $r->setReturnVal($result);
        $self->_runPostFetchHook($r);
        $result = $r->getReturnVal;
        
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

        my ($sth, $rv, $r);
        if (scalar(@_) == 3) {
            ($sth, $rv, $r) = $self->_getStatementHandleForQuery($query, $exec_args);
        } else {
            ($sth, $rv, $r) = $self->_getStatementHandleForQuery($query);
        }
        
        return $sth unless $sth;

        $self->_runPreFetchHook($r);
        $sth = $r->getStatementHandle;

        my $result = $sth->fetchrow_arrayref;

        $r->setReturnVal($result);
        $self->_runPostFetchHook($r);

        $result = $r->getReturnVal;

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

        my ($sth, $rv, $r);
        if (scalar(@_) == 3) {
            ($sth, $rv, $r) = $self->_getStatementHandleForQuery($query, $exec_args);
        } else {
            ($sth, $rv, $r) = $self->_getStatementHandleForQuery($query);
        }
        return $sth unless $sth;

        $self->_runPreFetchHook($r);
        $sth = $r->getStatementHandle;

        my $rows = [];
        my $row = $sth->fetchrow_hashref($self->getNameArg);
        while ($row) {
            $r->setReturnVal($row);
            $self->_runPostFetchHook($r);

            $row = $r->getReturnVal;
            push @$rows, $row;
            
            $self->_runPreFetchHook($r);
            $sth = $r->getStatementHandle;

            $row = $sth->fetchrow_hashref($self->getNameArg)
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

        my ($sth, $rv, $r);
        if (scalar(@_) == 3) {
            ($sth, $rv, $r) = $self->_getStatementHandleForQuery($query, $exec_args);
        } else {
            ($sth, $rv, $r) = $self->_getStatementHandleForQuery($query);
        }
        
        return $sth unless $sth;

        $self->_runPreFetchHook($r);
        $sth = $r->getStatementHandle;

        my $list = [];

        my $result = $sth->fetchrow_arrayref;
        while ($result) {
            $r->setReturnVal($result);
            $self->_runPostFetchHook($r);
            $result = $r->getReturnVal;
            
            # have to make copy because recent version of DBI now
            # return the same array reference each time
            push @$list, [ @$result ];
            $result = $sth->fetchrow_arrayref;
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
    *native_select_value = \&nativeSelectValue;

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

Same as nativeSelectMulti() except uses SQL::Abstract to generate
the SQL.  See the POD for SQL::Abstract for usage.  You must have
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

        my ($sth, $rv, $r);
        if (scalar(@_) == 3) {
            ($sth, $rv, $r) = $self->_getStatementHandleForQuery($query, $exec_args);
        } else {
            ($sth, $rv, $r) = $self->_getStatementHandleForQuery($query);
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

This method is deprecated.  Use $db->command($cmd_str) instead.

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

=head2 command($cmd_string)

This creates a literal SQL command for use in insert(), update(),
and related methods, since if you simply put something like
"CUR_DATE()" as a value in the %data parameter passed to insert,
the function will get quoted, and so will not work as expected.
Instead, do something like this:

    my $data = { file => 'my_document.txt',
                 the_date => $db->command('CUR_DATE()')
               };
    $db->insert('my_doc_table', $data);

This can also be done by passing a reference to a string with the
SQL command, e.g.,

    my $data = { file => 'my_document.txt',
                 the_date => \'CUR_DATE()'
               };
    $db->insert('my_doc_table', $data);

This is currently how command() is implemented.

=cut
    sub command {
        my ($self, $str) = @_;
        return \$str;
    }

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

        my $handler = $self->_getErrorHandler;
        $handler = [ $self, \&_default_error_handler ] unless $handler;
        if ($handler) {
            if (UNIVERSAL::isa($handler, 'ARRAY')) {
                my ($obj, $meth) = @$handler;
                return $obj->$meth($self, $extra);
            } else {
                return $handler->($self, $extra);
            }
        }

        return undef;
    }

    sub _default_error_handler {
        my ($self, $db, $extra) = @_;

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

    sub _default_debug_handler {
        my ($self, $db, $str, $fh) = @_;

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

        my $handler = $self->_getDebugHandler;
        $handler = [ $self, \&_default_debug_handler ] unless $handler;
        if ($handler) {
            if (UNIVERSAL::isa($handler, 'ARRAY')) {
                my ($obj, $meth) = @$handler;
                return $obj->$meth($self, $str, $fh);
            } else {
                return $handler->($self, $str, $fh);
            }
        }

        return undef;
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

=pod

=head2 errstr()

Calls errstr() on the underlying DBI object, which returns the
native database engine error message from the last driver method
called.

=cut
    sub errstr {
        my $self = shift;
        my $dbh = $self->_getDatabaseHandle;
        return $dbh ? $dbh->errstr : undef;
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
        my $self = shift;
        $self->{_auth} = shift;
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

    sub getDataSourceAsString {
        return shift()->_getDataSourceStr;
    }

    sub _getDataSourceStr {
        my $self = shift;
        return $self->{_data_source_str};
    }

    sub _setDataSourceStr {
        my $self = shift;
        $self->{_data_source_str} = shift;
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

    sub _setErrorHandler {
        my ($self, $handler) = @_;
        $$self{_error_handler} = $handler;
    }

    sub _getErrorHandler {
        return shift()->{_error_handler};
    }

    sub _setDebugHandler {
        my ($self, $handler) = @_;
        $$self{_debug_handler} = $handler;
    }
    
    sub _getDebugHandler {
        return shift()->{_debug_handler};
    }

    sub _setDbStyle {
        my ($self, $style) = @_;
        $$self{_db_style} = $style;
    }

    sub _getDbStyle {
        return shift()->{_db_style};
    }

    sub _setDbdDriver {
        my $self = shift;
        $self->{_dbd_driver} = shift;
    }

    sub _getDbdDriver {
        return shift()->{_dbd_driver};
    }

    # whether or not to disconnect when the Wrapper object is
    # DESTROYed
    sub _setDisconnect {
        my ($self, $val) = @_;
        $$self{_should_disconnect} = 1;
    }

    sub _setHeavy {
        my $self = shift;
        $self->{_heavy} = shift;
    }

    sub _getHeavy {
        my $self = shift;
        return $self->{_heavy};
    }

    sub shouldBeHeavy {
        my $self = shift;
        return 1 if $Heavy or $self->_getHeavy;
        return undef;
    }

    sub get_info {
        my ($self, $name) = @_;
        require DBI::Const::GetInfoType;
        my $dbh = $self->_getDatabaseHandle;
        return $dbh->get_info($DBI::Const::GetInfoType::GetInfoType{$name});
    }

=pod

=head2 DBI methods

 The following method calls are just passed through to the
 underlying DBI object for convenience.  See the documentation
 for DBI for details.

=over 4

=item prepare

 This method may call hooks in the future.  Use
 prepare_no_hooks() if you want to ensure that it will be a
 simple DBI call.

=back

=cut
    sub prepare {
        my $self = shift;
        my $query = shift;

        my $dbi_obj = $self->getDBI;
        my $sth = $dbi_obj->prepare($query);

        return $sth;
    }

=pod

=over 4

=item selectrow_arrayref

=back

=cut
    sub selectrow_arrayref {
        my $self = shift;
        my $dbh = $self->_getDatabaseHandle;
        return $dbh->selectrow_arrayref(@_);
    }

=pod

=over 4

=item selectrow_hashref

=back

=cut
    sub selectrow_hashref {
        my $self = shift;
        my $dbh = $self->_getDatabaseHandle;
        return $dbh->selectrow_hashref(@_);
    }

=pod

=over 4

=item selectall_arrayref

=back

=cut
    sub selectall_arrayref {
        my ($self, @args) = @_;
        my $dbh = $self->_getDatabaseHandle;
        return $dbh->selectall_arrayref(@args);
    }

=pod

=over 4

=item selectall_hashref

=back

=cut
    sub selectall_hashref {
        my ($self, @args) = @_;
        my $dbh = $self->_getDatabaseHandle;
        return $dbh->selectall_hashref(@args);
    }

=pod

=over 4

=item selectcol_arrayref

=back

=cut
    sub selectcol_arrayref {
        my ($self, @args) = @_;
        my $dbh = $self->_getDatabaseHandle;
        return $dbh->selectcol_arrayref(@args);
    }

=pod

=over 4

=item do

=back

=cut
    sub do {
        my ($self, @args) = @_;
        my $dbh = $self->_getDatabaseHandle;
        return $dbh->do(@args);
    }

=pod

=over 4

=item quote

=back

=cut
    sub quote {
        my ($self, @args) = @_;
        my $dbh = $self->_getDatabaseHandle;
        return $dbh->quote(@args);
    }

=pod

=over 4

=item commit

=back

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

=over 4

=item begin_work

=back

=cut
    sub begin_work {
        my $self = shift;
        my $dbh = $self->_getDatabaseHandle;
        if ($dbh) {
            return $dbh->begin_work;
        }
        return undef;        
    }

=pod

=over 4

=item rollback

=back

=cut
    sub rollback {
        my $self = shift;
        my $dbh = $self->_getDatabaseHandle;
        if ($dbh) {
            return $dbh->rollback;
        }
        return undef;        
    }

=pod

=over 4

=item ping

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

 Returns the last_insert_id.  The default is to be MySQL
 specific.  It just runs the query "SELECT LAST_INSERT_ID()".
 However, it will also work with MSSQL with the right parameters
 (see the db_style parameter in the section explaining the
 connect() method).

=cut
    sub getLastInsertId {
        my ($self, $catalog, $schema, $table, $field, $attr) = @_;
        if (0 and DBI->VERSION >= 1.38) {
            my $dbh = $self->_getDatabaseHandle;
            return $dbh->last_insert_id($catalog, $schema, $table, $field, $attr);
        } else {
            my $query;
            my $db_style = $self->_getDbStyle;
            my $dbd_driver = $self->_getDbdDriver;
            if (defined($db_style) and $db_style ne '') {
                $db_style = lc($db_style);
                if ($db_style eq 'mssql' or $db_style eq 'sybase' or $db_style eq 'asa'
                   or $db_style eq 'asany') {
                    $query = q{select @@IDENTITY};
                } elsif ($db_style eq 'mysql') {
                    $query = qq{SELECT LAST_INSERT_ID()};
                } else {
                    $query = qq{SELECT LAST_INSERT_ID()};
                }
            } elsif (defined($dbd_driver) and $dbd_driver ne '') {
                if ($dbd_driver eq 'sybase' or $dbd_driver eq 'asany') {
                    $query = q{SELECT @@IDENTITY};
                } else {
                    $query = qq{SELECT LAST_INSERT_ID()};
                }
            } else {
                $query = qq{SELECT LAST_INSERT_ID()};
            }
            
            my $row = $self->nativeSelectWithArrayRef($query);
            if ($row and @$row) {
                return $$row[0];
            }

            return undef;
        }
    }
    *get_last_insert_id = \&getLastInsertId;
    *last_insert_id = \&getLastInsertId;

=pod

=head2 Hooks

DBIx::Wrapper supports hooks that get called just before and just
after various query operations.  The add*Hook methods take a
single argument that is either a code reference (e.g., anonymous
subroutine reference), or an array whose first element is an
object and whose second element is the name of a method to call
on that object.

The hooks will be called with a request object as the first
argument.  See DBIx::Wrapper::Request.

The two expected return values are $request->OK and
$request->DECLINED.  The first tells DBIx::Wrapper that the
current hook has done everything that needs to be done and
doesn't call any other hooks in the stack for the current
request.  DECLINED tells DBIx::Wrapper to continue down the
hook stack as if the current handler was never invoked.

See DBIx::Wrapper::Request for example hooks.

=cut

=pod

=head3 addPrePrepareHook($hook)

Specifies a hook to be called just before any SQL statement is
prepare()'d.

=cut
    sub addPrePrepareHook {
        my $self = shift;
        my $handler = shift;
        push @{$self->{_pre_prepare_hooks}}, [ $handler ];
    }
    *add_pre_prepare_handler = \&addPrePrepareHook;
    *addPrePrepareHandler = \&addPrePrepareHook;
    *add_pre_prepare_hook = \&addPrePrepareHook;

=pod

=head3 addPostPrepareHook($hook)

Specifies a hook to be called just after any SQL statement is
prepare()'d.

=cut
    sub addPostPrepareHook {
        my $self = shift;
        my $handler = shift;
        push @{$self->{_post_prepare_hooks}}, [ $handler ];
    }
    *add_post_prepare_hook = \&addPostPrepareHook;

=pod

=head3 addPreExecHook($hook)

Specifies a hook to be called just before any SQL statement is
execute()'d.

=cut
    sub addPreExecHook {
        my $self = shift;
        my $handler = shift;
        push @{$self->{_pre_exec_hooks}}, [ $handler ];
    }
    *add_pre_exec_hook = \&addPreExecHook;

=pod

=head3 addPostExecHook($hook)

Adds a hook to be called just after a statement is execute()'d.

=cut
    sub addPostExecHook {
        my $self = shift;
        my $handler = shift;
        push @{$self->{_post_exec_hooks}}, [ $handler ];
    }
    *add_post_exec_handler = \&addPostExecHook;
    *addPostExecHandler = \&addPostExecHook;
    *add_post_exec_hook = \&addPostExecHook;

=pod

=head3 addPreFetchHook($hook)

Adds a hook to be called just before data is fetch()'d from the server.

=cut
    sub addPreFetchHook {
        my $self = shift;
        my $handler = shift;
        push @{$self->{_pre_fetch_hooks}}, [ $handler ];
    }
    *add_pre_fetch_hook = \&addPreFetchHook;
    *addPreFetchHandler = \&addPreFetchHook;

=pod

=head3 addPostFetchHook($hook)

Adds a hook to be called just after data is fetch()'d from the server.

=cut
    sub addPostFetchHook {
        my $self = shift;
        my $handler = shift;
        push @{$self->{_post_fetch_hooks}}, [ $handler ];
    }
    *addPostFetchHandler = \&addPostFetchHook;
    
    sub AUTOLOAD {
        my $self = shift;

        (my $func = $AUTOLOAD) =~ s/^.*::([^:]+)$/$1/;
        
        no strict 'refs';

        if (ref($self)) {
            my $dbh = $self->_getDatabaseHandle;
            return $dbh->$func(@_);
        } else {
            return DBI->$func(@_);
        }
    }
}

1;

__END__

=pod

=head2 There are also underscore_separated versions of these methods.

    E.g., nativeSelectLoop() becomes native_select_loop()


=head1 ACKNOWLEDGEMENTS

    Others who have contributed ideas and/or code for this module:

    Kevin Wilson
    Mark Stosberg

=head1 AUTHOR

    Don Owens <don@owensnet.com>

=head1 COPYRIGHT

    Copyright (c) 2003-2005 Don Owens

    All rights reserved. This program is free software; you can
    redistribute it and/or modify it under the same terms as Perl
    itself.

=head1 VERSION

    0.16

=cut
