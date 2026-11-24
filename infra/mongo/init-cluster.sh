#!/bin/bash
# Initialises the MongoDB sharded cluster (idempotent: safe to run more than once).
#   1. config server replica set  (cfgRS)
#   2. two shard replica sets     (shard1RS, shard2RS) with 3 members each
#   3. registers both shards with mongos and shards quicketa.orders on hashed order_id
set -e

init_rs () {   # $1 = host to run on, $2 = replica-set config document
  echo ">> initiating replica set on $1"
  mongosh --host "$1" --quiet --eval "
    try { rs.status(); print('   already initiated'); }
    catch (e) { printjson(rs.initiate($2)); }"
}

wait_primary () {   # $1 = host
  echo ">> waiting for a PRIMARY on $1's replica set"
  until mongosh --host "$1" --quiet --eval "db.hello().primary ? 1 : 0" | grep -q 1; do sleep 2; done
  echo "   primary = $(mongosh --host "$1" --quiet --eval 'db.hello().primary')"
}

init_rs cfg1 '{_id:"cfgRS", configsvr:true, members:[{_id:0, host:"cfg1:27017"}]}'
init_rs shard1a '{_id:"shard1RS", members:[
  {_id:0, host:"shard1a:27017", priority:2},
  {_id:1, host:"shard1b:27017", priority:1},
  {_id:2, host:"shard1c:27017", priority:1}]}'
init_rs shard2a '{_id:"shard2RS", members:[
  {_id:0, host:"shard2a:27017", priority:2},
  {_id:1, host:"shard2b:27017", priority:1},
  {_id:2, host:"shard2c:27017", priority:1}]}'

wait_primary cfg1
wait_primary shard1a
wait_primary shard2a

echo ">> waiting for mongos"
until mongosh --host mongos --quiet --eval "db.adminCommand('ping').ok" 2>/dev/null | grep -q 1; do sleep 3; done

echo ">> registering shards and sharding quicketa.orders"
mongosh --host mongos --quiet --eval '
  sh.addShard("shard1RS/shard1a:27017,shard1b:27017,shard1c:27017");
  sh.addShard("shard2RS/shard2a:27017,shard2b:27017,shard2c:27017");
  sh.enableSharding("quicketa");
  try { sh.shardCollection("quicketa.orders", { order_id: "hashed" }); }
  catch (e) { print("   shardCollection: " + e.message); }
  print("   shards: " + db.adminCommand({listShards:1}).shards.map(s => s._id).join(", "));
'
echo ">> MongoDB sharded cluster READY"
