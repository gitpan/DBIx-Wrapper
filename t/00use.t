#!/usr/bin/env perl -w

# Creation date: 2003-03-05 07:42:25
# Authors: Don
# Change log:
# $Id: 00use.t,v 1.2 2004/07/01 06:37:12 don Exp $

use strict;

# main
{
    use strict;
    use Test;

    use vars qw($Skip);
    BEGIN { eval 'use DBIx::Wrapper';
            if ($@) {
                plan tests => 1;
                print STDERR "\n\n  " . '=' x 10 . '> ';
                print STDERR "Skipping tests because DBI is not installed.\n";
                print STDERR "  " . '=' x 10 . '> ';
                print STDERR "You must install DBI before this module will work.\n\n";
                $Skip = 1;
            } else {
                plan tests => 1;
                $Skip = 0;
            }
        }
        
    eval 'use DBIx::Wrapper'; skip($Skip, ($Skip ? 1 : not $@));

}

exit 0;

###############################################################################
# Subroutines

