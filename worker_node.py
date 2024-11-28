import socket
import threading
import json
import os
import time
import gzip

class WorkerNodeWithDisk:
    def __init__(self, node_id, master_host, master_port, storage_dir):
        self.node_id = node_id
        self.master_host = master_host
        self.master_port = master_port
        self.storage_dir = storage_dir
        os.makedirs(self.storage_dir, exist_ok=True)  # Ensure storage directory exists

    def heartbeat(self):
        """Send periodic heartbeats to the master node."""
        while True:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((self.master_host, self.master_port))
                    heartbeat_message = json.dumps({"action": "heartbeat", "node_id": self.node_id})
                    s.send(heartbeat_message.encode())
            except ConnectionRefusedError:
                print("Master node unreachable")
            time.sleep(15)

    def save_block(self, file_name, block_id, data):
        """Save a block to disk."""
        file_dir = os.path.join(self.storage_dir, file_name)
        os.makedirs(file_dir, exist_ok=True)  # Create directory for the file
        block_path = os.path.join(file_dir, f"block_{block_id}.gz")
        with gzip.open(block_path, "wt") as f:
            f.write(data)  # Save block data (compressed)
        print(f"Block {block_id} of file {file_name} saved at {block_path}")

    def read_block(self, file_name, block_id):
        """Read a block from disk."""
        block_path = os.path.join(self.storage_dir, file_name, f"block_{block_id}.gz")
        if os.path.exists(block_path):
            with gzip.open(block_path, "rt") as f:
                data = f.read()  # Load block data
            print(f"Block {block_id} of file {file_name} read successfully")
            return data
        else:
            print(f"Block {block_id} of file {file_name} not found")
            return None

    def handle_client(self, client_socket):
        """Handle client requests for reading and writing blocks."""
        request = client_socket.recv(1024).decode()
        data = json.loads(request)
        
        if data['action'] == 'write_block':
            file_name = data['file_name']
            block_id = data['block_id']
            block_data = data['data']
            self.save_block(file_name, block_id, block_data)
            client_socket.send(b"Block written")

        elif data['action'] == 'read_block':
            file_name = data['file_name']
            block_id = data['block_id']
            block_data = self.read_block(file_name, block_id)
            if block_data:
                client_socket.send(block_data.encode())
            else:
                client_socket.send(b"Block not found")
        
        client_socket.close()

    def start(self, host, port):
        """Start the worker node server."""
        threading.Thread(target=self.heartbeat).start()
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((host, port))
        server.listen(5)
        print(f"Worker node {self.node_id} listening...")
        while True:
            client_socket, addr = server.accept()
            threading.Thread(target=self.handle_client, args=(client_socket,)).start()

# Run worker node
if __name__ == "__main__":
    worker = WorkerNodeWithDisk(node_id="Node1", master_host="10.8.1.98", master_port=5000, storage_dir="./data_store")
    worker.start(host="localhost", port=5001)
