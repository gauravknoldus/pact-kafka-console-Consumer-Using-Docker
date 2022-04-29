package testcase;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.stream.Stream;


public class ProducerTest {
    public void ProduceEvent(String filename) throws IOException {
        Stream<String> object = Files.lines(Paths.get(filename));

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:29092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String,String> sampleProducer= new KafkaProducer<String,String>(properties);
        object.forEach(f->{
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("quickstart-events",f);
            sampleProducer.send(record);
        });
        sampleProducer.close();

    }
    @Test
    public void Test () throws IOException {
        ProduceEvent("Event.json");
    }


}
