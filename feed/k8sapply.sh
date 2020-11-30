#!/bin/sh

set -x

FEED_DIR=$PWD

for dir in */
do
    cd $FEED_DIR/$dir
    kubectl apply -f cronjob.yaml
done