import socket
import json

# Master Node details
master_host = '10.30.55.88'
master_port = 5000

# File name and block data
file_name = "file.txt"
blocks = ["block1", "block2", "block3"]
block_data = {
    "block1": "This is the content of block 1.",
    "block2": "HThis is the content of block 2.",
    "block3": "HThis is the content of block 3."
}

# Connect to the Master Node and send the write request
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((master_host, master_port))
    write_request = json.dumps({
        # "action": "write",
        # "file_name": file_name,
        # "blocks": blocks,
        # "block_data": block_data  # Include the data for each block
        "action": "read",
        "file_name": file_name
    })
    s.send(write_request.encode())
    response = s.recv(1024).decode()
    print(response)
