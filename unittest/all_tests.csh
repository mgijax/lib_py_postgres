#!/bin/csh -f

#
# Run all unit tests
#

echo "Running sybase translator_tests"
/usr/local/bin/python translator_tests.py
if ( $status ) then
    exit 1
endif