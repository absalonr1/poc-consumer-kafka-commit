package kafkaretry;

import java.util.Properties;
import java.time.Duration;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;


/**
 * mvn install
 * export MY_POD_IP=localhost
 * java -jar target/simple-consumer-bx-1.0.0-jar-with-dependencies.jar
 * 
 * 
 * docker build -t simple-consumer-bx .
 * docker tag simple-consumer-bx absalon1000rr/simple-consumer-bx:v2
 * docker push absalon1000rr/simple-consumer-bx:v6
 */
public class SimpleConsumer {

   private static final Logger log = Logger.getLogger(SimpleConsumer.class); 
   public static void main(String[] args) throws Exception{

      Logger rootLogger = Logger.getRootLogger();
      rootLogger.setLevel(Level.INFO);
      
      //Define log pattern layout
      PatternLayout layout = new PatternLayout("%d{ISO8601} [%t] %-5p %c %x - %m%n");
      
      //Add console appender to root logger
      rootLogger.addAppender(new ConsoleAppender(layout));

      String topicName =  "test-topic-aopazo";
      
      Properties props = new Properties();
      // Datos de conexion
      props.put("bootstrap.servers","pkc-pgq85.us-west-2.aws.confluent.cloud:9092");
      props.put("security.protocol", "SASL_SSL");
      props.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username='YLWHDZM2XQ5KUTNF' password='CbVQkZ5/NsbS+KmvZwiZE/G9JJUvvHypm0qLAmmTUhKi/oTvDMCBLB2U2DeUkUcc';");
      props.put("sasl.mechanism", "PLAIN");
      // Define el "consumer group" ID
      props.put("group.id", "grp1-tes");
      // deshabilitare el auto-commit de kafka
      props.put("enable.auto.commit", "false");
      // Si kafka no recive un heratbeat durante mas de
      // 50000 milisecs, entonces este consumer se dara por
      // "muerto" produciendose un rebalaning
      props.put("session.timeout.ms", "50000");
      // define cada cuantos milisegundos el consumer
      // se reporta como "vivo" hacia kafka
      props.put("heartbeat.interval.ms", "1000");
      // Si poll() no es llamado en un maximo de 50000 milisecs. Kafka
      // considera como "muerto" a este consumer
      props.put("max.poll.interval.ms", "50000");
      // cantidad maxima de mensajes a recuperar en cada llamada a poll()
      props.put("max.poll.records", "10");
      props.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
      props.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");

      log.info("Configuracion: "+props);
      log.info("topicName: "+topicName);
      KafkaConsumer<String, String> consumer = null;
      Health health = null;
      try{
         consumer = new KafkaConsumer<String, String>(props);
         consumer.subscribe(Arrays.asList(topicName));
         log.info("Subscribed to topic " + topicName);
         int i = 0;
         health = new Health("/health");
         health.start();
         log.info("Entering while...");
         while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
            log.info("Poll number "+i);
            for (ConsumerRecord<String, String> record : records){
               log.info(String.format("\toffset = %d, key = %s, value = %s, partition = %s\n", record.offset(), record.key(), record.value(),record.partition() ));
               if(record.value().equals("ERROR"))
                  log.error(String.format("\toffset = %d, key = %s, value = %s, partition = %s\n", record.offset(), record.key(), record.value(),record.partition() ));
               if(record.value().equals("WARN"))
                  log.warn(String.format("\toffset = %d, key = %s, value = %s, partition = %s\n", record.offset(), record.key(), record.value(),record.partition() ));
            }
            ++i;
         }
      }finally{
         if(health!=null)
               health.stop();
         if(consumer!=null){
            
            consumer.close();
         }
      }
   }
}