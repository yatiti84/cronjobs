#!/bin/sh

set -x

EXECFOLDER=$1
EXECSCRIPT=$2
ENVFOLDER=.venv

if cd $EXECFOLDER \
    && . ./$ENVFOLDER/bin/activate \
    && shift 2 \
    && python3 $EXECSCRIPT $@ \
    && echo "$2 is finished"; then exit 0; else echo "$2 failed"； echo "$2 failed" > /dev/termination-log; exit 1; fi
