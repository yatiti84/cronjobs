#!/bin/sh

set -x

EXECFOLDER=$1
EXECSCRIPT=$2
ENVFOLDER=.venv

if cd $EXECFOLDER \
    && ls \
    && pwd \
    && ls ./$ENVFOLDER/bin \
    && . ./$ENVFOLDER/bin/activate \
    && shift 2 \
    && python3 $EXECSCRIPT $@ \
    && echo "$EXECSCRIPT is finished"; then exit 0; else echo "$EXECSCRIPT failed"； echo "$EXECSCRIPT failed" > /dev/termination-log; exit 1; fi
