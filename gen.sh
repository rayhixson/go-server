#!/bin/zsh

for count in {1..10000000}; printf "%09d\n" $[RANDOM]$[RANDOM] >> bestnums5.txt
