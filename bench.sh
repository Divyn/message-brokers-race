set -e
ulimit -n 65535
cd /root/exp
# Waits for GO, written from the Mac once the broker box is checked; GO holds the broker names to run.
echo WAITING_FOR_GO > STATUS
while [ ! -f GO ]; do sleep 5; done
echo RUNNING > STATUS
/root/v/bin/python mb_race.py "$(cat BROKER_IP)" $(cat GO) > results.csv
