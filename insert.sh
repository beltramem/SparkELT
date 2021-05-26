#!/bin/sh

d=$1
while [ "$d" != $2 ]; do

  sbt  -J-Xmx4g "run $d"
  d=$(date -I -d "$d + 1 day")

  # mac option for d decl (the +1d is equivalent to + 1 day)
  # d=$(date -j -v +1d -f "%Y-%m-%d" "2020-12-12" +%Y-%m-%d)
done