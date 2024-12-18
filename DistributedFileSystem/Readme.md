# **Custom Distributed File System**

The custom file system used to store the flight data.

---

## **How to use:**

1. **Master Node**
    - In line 193 of master-node.py, change the host IP(host) and port number(port) to your Master Node's IP and port.
    - Run `python3 master-node.py` with the correct permissions.
    - Master Node should be up and running.

2. **Data Node**
    - From line 101 in worker_node.py, change node_id to the name of the node, master_host to the IP of the master node, master_port to the port of the master node, hostip to the IP address of the data node.
    - In line 107 set host to the IP address of the data node and port to the port number of the worker node.
    - Run `python3 data-node.py` with the correct permissions.
    - Data Node should be running.
    - You can set up multiple data nodes in this fashion.

3. **Client**
    - Connect to the master node at athe correct IP and send json requests.
        - **For Writing:**
            -Split file into blocks (block1, block2, etc) and block_data should have the data in each block. See client.py for an example.
            -Send write request with the action as write, file name, blocks and block data. See client.py for an example
        - **For Reading**
            -Send a read request with the action as read and file name. See client.py for an example.




