#!/usr/bin/env perl

# Creation date: 2003-03-30 17:52:14
# Authors: Don
# Change log:
# $Id: Makefile.PL,v 1.3 2003/10/21 05:11:15 don Exp $

use strict;
use Carp;

# main
{
    local($SIG{__DIE__}) = sub { &Carp::cluck(); exit 0 };

    use ExtUtils::MakeMaker;

    WriteMakefile(
                  NAME => 'DBIx::Wrapper',
                  DISTNAME => 'DBIx-Wrapper',
                  VERSION_FROM => 'Wrapper.pm',
                  ABSTRACT => 'Wrapper Module for the DBI',
                  AUTHOR => 'DON OWENS <don@owensnet.com>',
                  PM => { 'Wrapper.pm' => '$(INST_LIBDIR)/Wrapper.pm',
                          'Wrapper/SelectLoop.pm' => '$(INST_LIBDIR)/Wrapper/SelectLoop.pm',
                          'Wrapper/SQLCommand.pm' => '$(INST_LIBDIR)/Wrapper/SQLCommand.pm',
                          'Wrapper/StatementLoop.pm' => '$(INST_LIBDIR)/Wrapper/StatementLoop.pm',
                          'Wrapper/Statement.pm' => '$(INST_LIBDIR)/Wrapper/Statement.pm',
                        },
                  dist => { COMPRESS => 'gzip -9f', SUFFIX => 'gz' },
                  DIR => [],
                  EXE_FILES => [],
                  PREREQ_PM => { Carp => 0 },
                  );

}

exit 0;

###############################################################################
# Subroutines

