#!/usr/bin/env perl -w

# Creation date: 2003-03-05 07:42:25
# Authors: Don
# Change log:
# $Id: 00use.t,v 1.1 2003/03/31 01:56:00 don Exp $

use strict;

# main
{
    use strict;
    use Test;
    BEGIN { plan tests => 1 }
    
    use DBIx::Wrapper; ok(1);

}

exit 0;

###############################################################################
# Subroutines

