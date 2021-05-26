#!/bin/bash
#this script will execute the command n time
#function run
run() {
    number=$1
    shift
    for i in `seq $number`; do
      . /$@
    done
}
 
