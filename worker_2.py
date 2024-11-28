import socket
import threading
import json
import time

class WorkerNode:
    def __init__(self, node_id, master_host, master_port):
        self.node_id = node_id
        self.master_host = master_host
        self.master_port = master_port
        self.data_store = {}  # block_id -> data

    def heartbeat(self):
        while True:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((self.master_host, self.master_port))
                    heartbeat_message = json.dumps({"action": "heartbeat", "node_id": self.node_id})
                    s.send(heartbeat_message.encode())
            except ConnectionRefusedError:
                print("Master node unreachable")
            time.sleep(15)

    def handle_client(self, client_socket):
        try:
            request = client_socket.recv(1024).decode()
            data = json.loads(request)

            if data['action'] == 'write_block':
                block_id = data['block_id']
                self.data_store[block_id] = data['data']
                client_socket.send(b"Block written")

            elif data['action'] == 'read_block':
                block_id = data['block_id']
                if block_id in self.data_store:
                    client_socket.send(self.data_store[block_id].encode())
                else:
                    client_socket.send(b"Block not found")
        
        finally:
            client_socket.close()

    def start(self, host, port):
        threading.Thread(target=self.heartbeat).start()
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((host, port))
        server.listen(5)
        print(f"Worker node {self.node_id} listening...")
        while True:
            client_socket, addr = server.accept()
            threading.Thread(target=self.handle_client, args=(client_socket,)).start()

if __name__ == "__main__":
    worker = WorkerNode(node_id="node1", master_host="10.8.1.98", master_port=5000)
    worker.start(host="localhost", port=5001)
