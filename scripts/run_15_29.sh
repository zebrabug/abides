#!/bin/bash
for i in 20200915 20200929
do
   python -u abides.py -c marketreplay_USD -t USD -d $i -s 1234 -l USD_09_replay
done

