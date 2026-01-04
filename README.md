## What is this?

A toy implementation of Raft protocol along with an in-memory distributed KV store on top of it.

## How to run it?
This will start a 5 node Raft cluster with a distributed KV store.
```shell
./run.sh
```

You can take use the [req.http](./req.http) file to test the KV store.
KV currently support the following operations:
- `GET <key>`: Retrieve the value associated with the given key.
- `SET <key> <value>`: Set the value for the given key.

**Note**: All the state of the KV store including Raft is only persisted in memory.

## Automated Tests
The test can be run using the below command.
```shell
uv run end-to-end.py
```

The test performs the following operations which should cover the basic functionality of the KV store.
* Starts a 5-node Raft cluster with KV store.
* Inserts 10 values into the KV store.
* Kills the leader node
* Starts a clean state node
* Inserts one more value
* Waits for the node to catch up
* Checks if the node has the inserted value.

## Load testing
A basic load testing script is also provided, which simulates 10 concurrent clients performing GET and SET operations on the KV store.
```shell
k6 run loadtest.js
```
Make sure you start the cluster before running the load test.