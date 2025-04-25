#!/bin/bash

echo "Initializing replica set..."

mongosh --host mongo1 --eval '
rs.initiate({
  _id: "rs0",
  members: [
    { _id: 0, host: "mongo1:27017" },
    { _id: 1, host: "mongo2:27017" },
    { _id: 2, host: "arbiter:27017", arbiterOnly: true }
  ]
});
'

echo "Replica set initialized ✅"
echo "Waiting for MongoDB to be ready..."
until mongosh --host mongo1 --eval 'db.adminCommand("ping")' &> /dev/null; do
  sleep 1
done
echo "MongoDB is ready! ✅"
