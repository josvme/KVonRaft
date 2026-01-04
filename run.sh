#!/run/current-system/sw/bin/bash

NODE_ID=1 NEIGHBOR_NODE_IDS=2,3,4,5 go run raft.go &
NODE_ID=2 NEIGHBOR_NODE_IDS=1,3,4,5 go run raft.go &
NODE_ID=3 NEIGHBOR_NODE_IDS=1,2,4,5 go run raft.go &
NODE_ID=4 NEIGHBOR_NODE_IDS=1,2,3,5 go run raft.go &
NODE_ID=5 NEIGHBOR_NODE_IDS=1,2,3,4 go run raft.go &

wait