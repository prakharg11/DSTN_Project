# **IoT Streaming Pipeline with Custom Distributed File System**

This project implements an end-to-end architecture for processing and storing **flights data streams**, completed as part of the **Data Storage Technologies and Networks** (CS F446) coursework at BITS Pilani, K K Birla Goa Campus, under the guidance of **Dr. Arnab K. Paul**.

The system is built to handle real-time IoT flight data using **Apache Kafka**, **Apache Flink**, and a **custom-built distributed file system**. The implementation includes data compression and fault tolerance.

---

## **Features**

1. **Data Stream Simulation:**
   - **Flights data** is simulated using real-world datasets.
   - Data is published to a Kafka topic for processing.

2. **Data Processing:**
   - **Apache Flink** is used for real-time data processing with three key features:
     - Transformation of raw flights data into structured formats.
     - Filtering and aggregation based on specific criteria (e.g., delay filtering).
     - Advanced processing and export to the custom file system.
   - Flink also reads and processes data from the distributed file system when required.

3. **Custom Distributed File System:**
   - Implements master-node and data-node architecture.
   - **Replication factor:** 2, ensuring fault tolerance.
   - Provides compression and decompression capabilities to optimize storage.
   - No limit on number of data-nodes that can be added to the file system.
     
4. **Data Optimizations:**
   - Compression is applied to the backend storage.
   - Fault tolerance strategies ensure data consistency and availability.

---

## **Project Structure**

The project is divided into three main modules:

### **1. Producer Code**
   - Simulates IoT sensors and produces **flights data** to a Kafka topic.
   - Reads input datasets and publishes them to the Kafka broker.
   - **Folder:** `Producer`

### **2. Processor Code**
   - Processes the streaming **flights data** using Apache Flink.
   - Performs three types of processing.
   - Reads data from the distributed file system for additional processing when needed.
   - **Folder:** `DataProcessing`

### **3. Distributed File System**
   - Implements a custom master-node and data-node architecture.
   - **Features:**
     - Fault tolerance with a replication factor of 2.
     - Data compression.
   - **Folder:** `DistributedFileSystem`
   - **Contains:**
     - Master node and data node implementation.

---


### **Steps to Run**
1. **Set Up Kafka Broker:**
   - Configure and start Zookeeper and Kafka on your local machine.

2. **Simulate IoT Data:**
   - Run the producer code to simulate IoT streams and publish to Kafka topics.

3. **Start Distributed File System:**
   - Run the master node and start the required number of data nodes.
   - Ensure the replication factor and compression settings are configured as needed.

4. **Run Data Processing:**
   - Execute the Flink pipeline to process data streams and write outputs to the distributed file system.
    

## **Key Learnings**

- Integration of **Apache Kafka** and **Apache Flink** for real-time event streaming and processing.
- Designing a custom distributed file system with fault tolerance and compression.
- Handling batch and stream processing in a cohesive pipeline.
- Achieving high performance and reliability in a multi-node system.

---

## **References**
This project was inspired by concepts and techniques discussed in the **CS F446: Data Storage Technologies and Networks** course. Special thanks to **Dr. Arnab K. Paul** for providing guidance and support.

---

Feel free to explore the project and provide feedback. Contributions are welcome! For detailed instructions on each module, refer to the individual README files in their respective folders.
