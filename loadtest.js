import http from "k6/http";
import { check, sleep } from "k6";
import { scenario } from 'k6/execution';

// Test configuration
export const options = {
    thresholds: {
        // Assert that 99% of requests finish within 3000ms.
        http_req_duration: ["p(99) < 3000"],
    },
    // Ramp the number of virtual users up and down
    stages: [
        { duration: "30s", target: 10 },
    ],
};

function payload(k, v) {
    return JSON.stringify({
        key: k,
        value: v,
    });
}

// request headers


// Simulated user behavior
export default function () {
    let leader_id = null;
    let num_nodes = 5;
    for (let i = 1; i <= num_nodes; i++) {
        try {
            let res = http.get(`http://localhost:${8000 + i}/leader`);
            if (res.status === 200) {
                let data = res.json();
                if (data.Leader === "true") {
                    leader_id = data.NodeId;
                    break;
                }
            }
        } catch (e) {
            // Ignore connection errors for individual nodes
        }
    }

    if (!leader_id) {
        console.log("Leader not found after checking all nodes");
        return;
    }

    let host = "http://localhost:" + (parseInt(leader_id) + 8000);
    const iteration = scenario.iterationInTest + 2000;
    const pl = payload("hello" + iteration, "world" + iteration)
    let res = http.post(host + "/insert", pl);
    sleep(1);
    let rep = http.get(host + "/get?key=hello" + iteration);
    check(rep, { "value matches": (r) => r.json('value') === "world" + iteration });
}