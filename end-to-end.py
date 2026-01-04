#!/usr/bin/env python3
# /// script
# dependencies = [
#   "requests",
# ]
# ///

import subprocess
import time
import requests
import os

def leader_node_id(num_nodes):
    leader_id = None
    for i in range(1, num_nodes + 1):
        response = requests.get(f"http://localhost:{8000 + i}/leader")
        response.raise_for_status()
        data = response.json()
        is_leader= data.get("Leader")
        if is_leader == "true":
            leader_id = data.get("NodeId")
            print(f"Leader found: Node {leader_id}")
            break

    return leader_id

def start_node(node_id, num_nodes):
    neighbor_ids = [str(j) for j in range(1, num_nodes + 1) if j != node_id]
    env = os.environ.copy()
    env["NODE_ID"] = str(node_id)
    env["NEIGHBOR_NODE_IDS"] = ",".join(neighbor_ids)

    # Start raft.go as a background process
    process = subprocess.Popen(["./raft"], env=env)
    return process

def main():
    nodes = []
    num_nodes = 5

    print(f"Starting {num_nodes} Raft nodes...")
    for i in range(1, num_nodes + 1):
        process = start_node(i, num_nodes)
        nodes.append(process)

    try:
        # Wait for election to take place
        print("Waiting 10 seconds for election...")
        time.sleep(10)

        # Find the leader
        print("Finding the leader...")
        leader_id = leader_node_id(num_nodes)
        if not leader_id:
            print("Leader not found after checking all nodes")
            return

        if leader_id:
            for i in range(10):
                response = requests.post(f"http://localhost:{8000 + leader_id}/insert", json={"key": "key"+str(i), "value": "value"+str(i)})

        response = requests.get(f"http://localhost:{8000 + int(leader_id)}/full")
        response.raise_for_status()
        data = response.json()
        print(data)

        response = requests.get(f"http://localhost:{8000 + (int(leader_id)+1)%5}/full")
        response.raise_for_status()
        data = response.json()
        print(data)

        if leader_id:
            # Send kill request to leader
            port = int(leader_id) + 8000
            print(f"Sending kill request to leader (Node {leader_id}) on port {port}...")
            try:
                kill_response = requests.get(f"http://localhost:{port}/die")
                print(f"Kill response: {kill_response.text}")
            except Exception as e:
                print(f"Error killing leader: {e}")

        # Start the node again.
        new_node = start_node(leader_id, num_nodes)
        nodes.append(new_node)

        # wait for new election
        time.sleep(10)
        # Since we dont implement no-op on election success, client needs to sent it
        new_leader_id = leader_node_id(num_nodes)
        response = requests.post(f"http://localhost:{8000 + new_leader_id}/insert", json={"key": "key"+str(10), "value": "value"+str(10)})

        print("Waiting 20 seconds for data to be replicated to new node ...")
        time.sleep(20)
        # Find the leader
        print("Check if leader has joined as a follower and has replicated data")
        try:
            # Atleast last 8 keys should be applied
            response = requests.get(f"http://localhost:{8000 + int(leader_id)}/get?key=key10")
            response.raise_for_status()
            data = response.json()
            if data.get("value") == "value10":
                print("End to end test passed !!!!")
            else:
                print("End to end test failed !!!!")
        except Exception as e:
            print(f"Error getting key from node {leader_id}: {e}")

        # Atleast last 8 keys should be applied
        response = requests.get(f"http://localhost:{8000 + int(leader_id)}/full")
        response.raise_for_status()
        data = response.json()
        print(data)
        # Wait for processes to finish (one of them was killed, others might still run)
        print("Killing for background processes to finish...")
        for node in nodes:
            node.terminate()

    except KeyboardInterrupt:
        print("\nTerminating nodes...")
        for node in nodes:
            node.terminate()
    except subprocess.CalledProcessError as e:
        print(f"k6 failed: {e}")
        for node in nodes:
            node.terminate()

if __name__ == "__main__":
    main()
