#:/bin/bash
echo ******************************************
echo Starting ReplicaSet
echo ******************************************

sleep 20 | echo Sleeping 20 seconds to allow MongoDB to start
mongo mongodb://mongo-rs1:27017 replicaSet.js
