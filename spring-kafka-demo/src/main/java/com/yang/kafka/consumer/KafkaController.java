package com.yang.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by yz on 2018/10/28.
 */
@RestController
public class KafkaController {
    /**
     * 使用日志打印消息
     */
    private static Logger logger = LoggerFactory.getLogger(KafkaController.class);

    /**
     * 注入kafkaTemplate
     */
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    /**
     * 发送消息到kafka
     * @param key key
     * @param data data
     */
    private void send(String key, String data) {
        kafkaTemplate.send("kafka-demo", key, data);
    }

    @RequestMapping("/kafka")
    public String testKafka() {
        int iMax = 10;
        for (int i=1; i < iMax; i++) {
            send("key" + i, "data" + i);
        }
        return "success";
    }

    @KafkaListener(topics = "kafka-demo")
    public void receive(ConsumerRecord<?, ?> consumer) {
        logger.info("{} - {}:{}", consumer.topic(), consumer.key(), consumer.value());
    }
}
