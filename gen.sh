#!/bin/zsh

count=10000000
while ( [[ $count -le 100000000 ]] ); do
    echo $count
    # echo $count | nc 127.0.0.1 1111
    let count++
done
							 
