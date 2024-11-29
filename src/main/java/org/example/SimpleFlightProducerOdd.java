package org.example;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

public class SimpleFlightProducerOdd {

    private final boolean inDocker = new File("/.dockerenv").exists(); // DONT CHANGE
    private final Producer<String, String> producer; // DONT CHANGE

    public SimpleFlightProducerOdd() { // DONT CHANGE
        this.producer = createKafkaProducer();
    }

    public SimpleFlightProducerOdd(Producer<String, String> producer) { // DONT CHANGE
        this.producer = producer;
    }

    public Producer<String, String> createKafkaProducer() { // DONT CHANGE
        try (var stream = Producer.class.getClassLoader().getResourceAsStream("producer.properties")) {
            Properties props = new Properties();
            props.load(stream);
            props.setProperty("client.id", "producer-" + UUID.randomUUID());
            if (inDocker) {
                props.setProperty("bootstrap.servers", props.getProperty("bootstrap.servers.docker"));
            }
            System.out.println("Producer initialized:");
            return new KafkaProducer<>(props);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void produceFlight(Flight flight, String topic) {
        Gson gson = new Gson();
        String flightJson = gson.toJson(flight);
        producer.send(new ProducerRecord<>(topic, Integer.toString(flight.id), flightJson));
    }

    public void readAndProduceFlightsFromCSV(String filePath, String topic) throws IOException {
        int cnt = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            br.readLine(); // Skip header line
            while ((line = br.readLine()) != null) {
                if ( (cnt % 2) == 1) {
                    String[] fields = line.split(",");
                    System.out.println(line);
                    Flight flight = new Flight(fields);
                    produceFlight(flight, topic);
                }
                cnt++;
            }
        }
    }

    public void flush() {
        producer.flush();
    }

    public void close() {
        producer.close();
    }

    public static void main(String[] args) {
        SimpleFlightProducerOdd SimpleFlightProducerOdd = new SimpleFlightProducerOdd();
        String csvFilePath = "D:\\coding\\projects\\DSTN\\project\\data\\data.csv"; // Change this to the actual path
        try {
            SimpleFlightProducerOdd.readAndProduceFlightsFromCSV(csvFilePath, "flights-topic");
            SimpleFlightProducerOdd.flush();
            System.out.println("Produce Finished");
        } catch (IOException e) {
            System.err.println("Error reading CSV file: " + e.getMessage());
        } finally {
            SimpleFlightProducerOdd.close();
        }
    }
}
