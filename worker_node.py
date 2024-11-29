import socket
import threading
import json
import time
import os

class WorkerNode:
    def __init__(self, node_id, master_host, master_port, hostip, storage_dir):
        self.node_id = node_id
        self.master_host = master_host
        self.master_port = master_port
        self.storage_dir = storage_dir  # Directory to store file and block data
        self.hostip = hostip

        # Ensure storage directory exists
        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)

    def heartbeat(self):
        while True:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((self.master_host, self.master_port))
                    heartbeat_message = json.dumps({"action": "heartbeat", "node_id": f"{self.hostip}:{self.port}"})
                    s.send(heartbeat_message.encode())
            except ConnectionRefusedError:
                print("Master node unreachable")
            time.sleep(15)

    def handle_client(self, client_socket):
        try:
            request = client_socket.recv(1024).decode()
            data = json.loads(request)

            if data['action'] == 'write_block':
                file_name = data['file_name']
                block_id = data['block_id']
                block_data = data['data']
                self.write_block_to_disk(file_name, block_id, block_data)
                print(f"Stored block {block_id} of file {file_name} with data: {block_data}")
                client_socket.send(b"Block written")

            elif data['action'] == 'read_block':
                file_name = data['file_name']
                block_id = data['block_id']
                block_data = self.read_block_from_disk(file_name, block_id)
                if block_data:
                    client_socket.send(block_data.encode())
                else:
                    client_socket.send(b"Block not found")
        
        finally:
            client_socket.close()

    def write_block_to_disk(self, file_name, block_id, block_data):
        """Write block data to disk, organized by file name."""
        file_dir = os.path.join(self.storage_dir, file_name)
        if not os.path.exists(file_dir):
            os.makedirs(file_dir)  # Create a directory for the file if it doesn't exist
        block_file_path = os.path.join(file_dir, f"{block_id}.txt")
        with open(block_file_path, 'w') as block_file:
            block_file.write(block_data)

    def read_block_from_disk(self, file_name, block_id):
        """Read block data from disk, given file name and block ID."""
        block_file_path = os.path.join(self.storage_dir, file_name, f"{block_id}.txt")
        if os.path.exists(block_file_path):
            with open(block_file_path, 'r') as block_file:
                return block_file.read()
        return None

    def start(self, host, port):
        self.port = port
        threading.Thread(target=self.heartbeat).start()
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((host, port))
        server.listen(5)
        print(f"Worker node {self.node_id} listening on {host}:{port}...")
        while True:
            client_socket, addr = server.accept()
            threading.Thread(target=self.handle_client, args=(client_socket,)).start()


if __name__ == "__main__":
    # Directory for storing file and block data
    storage_directory = "./data_node_storage"

    worker = WorkerNode(
        node_id="node2",
        master_host="localhost",
        master_port=5000,
        hostip="localhost",
        storage_dir=storage_directory
    )
    worker.start(host="localhost", port=5003)
