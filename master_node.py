import socket
import threading
import json
import time
import os

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
        
        if data['action'] == 'write':
            file_name = data['file_name']
            blocks = data['blocks']
            block_locations = self.allocate_blocks(blocks)
            with self.lock:
                self.metadata[file_name] = block_locations
                self.save_metadata()  # Save metadata to disk
            client_socket.send(json.dumps(block_locations).encode())
        
        elif data['action'] == 'read':
            file_name = data['file_name']
            if file_name in self.metadata:
                client_socket.send(json.dumps(self.metadata[file_name]).encode())
            else:
                client_socket.send(b"File not found")
        
        elif data['action'] == 'heartbeat':
            node_id = data['node_id']
            with self.lock:
                self.heartbeat_data[node_id] = time.time()
            client_socket.send(b"Heartbeat acknowledged")
            print("Heartbeat")
        
        client_socket.close()

    def allocate_blocks(self, blocks):
        """Simulate block allocation across worker nodes."""
        nodes = list(self.heartbeat_data.keys())
        block_locations = {}
        for i, block in enumerate(blocks):
            replicas = nodes[i:i + self.replication_factor]
            block_locations[block] = replicas
        return block_locations

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
    master = MasterNode(host="10.8.1.98", port=5000, metadata_file="metadata.json")
    master.start()
