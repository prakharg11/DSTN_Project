import socket
import threading
import json
import time
import os
import random

class MasterNode:
    def __init__(self, host, port, metadata_file):
        self.host = host
        self.port = port
        self.metadata = {}  # File metadata (file -> blocks -> node locations)
        self.heartbeat_data = {}  # Node -> last heartbeat time
        self.replication_factor = 2
        self.lock = threading.Lock()
        self.metadata_file = metadata_file
        self.load_metadata()  # Load metadata from disk if available

        # Metrics
        self.latency_data = {"read": [], "write": []}  # Stores latencies of requests
        self.request_count = 0  # Tracks the total number of requests
        self.start_time = time.time()  # Start time for throughput measurement

    def load_metadata(self):
        """Load metadata from disk if the file exists."""
        if os.path.exists(self.metadata_file):
            with open(self.metadata_file, 'r') as f:
                self.metadata = json.load(f)
            print("Metadata loaded from disk.")

    def save_metadata(self):
        """Save metadata to disk."""
        with open(self.metadata_file, 'w') as f:
            json.dump(self.metadata, f)
        print("Metadata saved to disk.")

    def handle_client(self, client_socket):
        request = client_socket.recv(1024).decode()
        data = json.loads(request)
        start_time = time.time()  # Start time for latency measurement

        if data['action'] == 'write':
            file_name = data['file_name']
            blocks = data['blocks']  # Blocks and their data
            block_data = data['block_data']
            block_locations = self.allocate_blocks(blocks)
            
            # Distribute blocks to WorkerNodes
            for block, replicas in block_locations.items():
                for replica in replicas:
                    node_host, node_port = replica.split(":")
                    self.send_block_to_worker(node_host, int(node_port), block, block_data[block], file_name)
            
            # Update metadata and save
            with self.lock:
                if file_name in self.metadata:
                # Append new block locations to existing metadata
                    self.metadata[file_name].update(block_locations)
                else:
                # Add new file with its block locations
                    self.metadata[file_name] = block_locations
                    self.save_metadata()  # Save metadata to disk
            client_socket.send(json.dumps(block_locations).encode())

            # Update latency metrics
            self.record_latency("write", start_time)
        
        elif data['action'] == 'read':
            file_name = data['file_name']
            if file_name in self.metadata:
                block_locations = self.metadata[file_name]
                file_data = self.read_blocks_from_workers(block_locations, file_name)
                client_socket.send(json.dumps(file_data).encode())
            else:
                client_socket.send(b"File not found")

            # Update latency metrics
            self.record_latency("read", start_time)
        
        elif data['action'] == 'heartbeat':
            node_id = data['node_id']
            with self.lock:
                self.heartbeat_data[node_id] = time.time()
            client_socket.send(b"Heartbeat acknowledged")
            print("Heartbeat from", node_id)
        
        client_socket.close()

        # Update throughput metrics
        self.update_throughput()

    def allocate_blocks(self, blocks):
        """Simulate block allocation across worker nodes."""
        nodes = list(self.heartbeat_data.keys())
        random.shuffle(nodes)
        block_locations = {}
        for block in blocks:
            replicas = nodes[:self.replication_factor]  # Simple round-robin for now
            block_locations[block] = replicas
        return block_locations

    def send_block_to_worker(self, worker_host, worker_port, block_id, block_data, file_name):
        """Send block data to a WorkerNode."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as worker_socket:
                worker_socket.connect((worker_host, worker_port))
                write_request = json.dumps({
                    "action": "write_block",
                    "file_name": file_name,
                    "block_id": block_id,
                    "data": block_data
                })
                worker_socket.send(write_request.encode())
                response = worker_socket.recv(1024).decode()
                print(f"WorkerNode {worker_host}:{worker_port} response: {response}")
        except ConnectionRefusedError:
            print(f"Failed to connect to WorkerNode {worker_host}:{worker_port}")

    def read_blocks_from_workers(self, block_locations, file_name):
        """Fetch block data from WorkerNodes."""
        file_data = {}
        for block, replicas in block_locations.items():
            for replica in replicas:
                node_host, node_port = replica.split(":")
                block_data = self.read_block_from_worker(node_host, int(node_port), block, file_name)
                if block_data:
                    file_data[block] = block_data
                    break  # Only need one successful read
        return file_data

    def read_block_from_worker(self, worker_host, worker_port, block_id, file_name):
        """Request block data from a WorkerNode."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as worker_socket:
                worker_socket.connect((worker_host, worker_port))
                read_request = json.dumps({
                    "action": "read_block",
                    "file_name": file_name,
                    "block_id": block_id
                })
                worker_socket.send(read_request.encode())
                response = worker_socket.recv(1024).decode()
                if response != "Block not found":
                    print(f"Read block {block_id} from WorkerNode {worker_host}:{worker_port}")
                    return response
        except ConnectionRefusedError:
            print(f"Failed to connect to WorkerNode {worker_host}:{worker_port}")
        return None

    def record_latency(self, action, start_time):
        """Record the latency of a request."""
        latency = time.time() - start_time
        self.latency_data[action].append(latency)

    def update_throughput(self):
        """Calculate and display throughput every 1000 requests."""
        self.request_count += 1
        if self.request_count >= 1000:
            elapsed_time = time.time() - self.start_time
            throughput = self.request_count / elapsed_time
            print(f"Throughput: {throughput:.2f} requests/second")
            self.request_count = 0  # Reset the counter
            self.start_time = time.time()  # Reset the start time

    def monitor_nodes(self):
        """Monitor worker nodes for heartbeats."""
        while True:
            time.sleep(10)
            current_time = time.time()
            with self.lock:
                for node, last_heartbeat in list(self.heartbeat_data.items()):
                    if current_time - last_heartbeat > 30:
                        print(f"Node {node} failed!")
                        del self.heartbeat_data[node]

            # Print latency metrics
            print("Latency (seconds):", {action: sum(latencies) / len(latencies) if latencies else 0 
                                         for action, latencies in self.latency_data.items()})

    def start(self):
        """Start the master node server."""
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((self.host, self.port))
        server.listen(5)
        print("Master node listening...")
        threading.Thread(target=self.monitor_nodes).start()
        while True:
            client_socket, addr = server.accept()
            threading.Thread(target=self.handle_client, args=(client_socket,)).start()

# Run master node
if __name__ == "__main__":
    master = MasterNode(host="10.30.55.88", port=5000, metadata_file="metadata.json")
    master.start()
