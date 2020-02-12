package myapps;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.*;

import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        Properties properties = new Properties();

        // IP e Porta do broker bootstrap
        properties.setProperty("bootstrap.servers", "localhost:9092");

        // A chave será serializada para Long
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());

        // O valor será serializado para String
        properties.setProperty("value.deserializer", LongDeserializer.class.getName());

        // Id do grupo do tópico que esta se conectando no broker
        properties.setProperty("group.id", "group.1");

        // Id do producer que esta se conectando no broker
        properties.setProperty("client.id", "cliente.1");

        // Define se o commit das mensagens lidas vai ser automático
        properties.setProperty("enable.auto.commit", "true");

        // Tempo maximo de espera do commit até ser considerado falha
        properties.setProperty("auto.commit.interval.ms", "5000");

        // Pega o offset mais recente ao se conectar com o kafka
        properties.setProperty("auto.offset.reset", "earliest");

        // Máximo de mensagem que irá ser pega por vez (lote)
        properties.setProperty("max.poll.records", "500");

        // Tempo do heartbeat que o componente irá enviar ao kafka
        properties.setProperty("heartbeat.interval.ms", "1000");

        // Espera do heartbeat do componente até ser considerado falha
        properties.setProperty("session.timeout.ms", "10000");

        KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(properties);
        String topico = "streams-wordcount-output2";
        consumer.subscribe(Arrays.asList(topico));

        try {
            while (true) {
                final ConsumerRecords<String, Long> consumerRecords = consumer.poll(1000);

                consumerRecords.forEach(record -> {
                    System.out.println(record.key() + " - " + record.value().toString());
                });
            }
        } finally {
            consumer.close();
        }
    }
}
