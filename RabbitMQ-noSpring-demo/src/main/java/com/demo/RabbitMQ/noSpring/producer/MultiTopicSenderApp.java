package com.demo.RabbitMQ.noSpring.producer;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;


public class MultiTopicSenderApp {
    private static final String EXCHANGE_NAME = "MultiTopicSender";

    public static void main(String[] args) throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");

        Thread t1 = new Thread(() -> {
            byte[] data = new byte[20];
            while (!new String(data).equals("q")) {
                try (Connection connection = connectionFactory.newConnection();
                     Channel channel = connection.createChannel()) {
                    System.in.read(data);
                    String topicAndMessage = new String(data);
                    String topic = topicAndMessage.split(" ")[0];
                    String routingKey = "topic." + topic;
                    String messageForSending = topicAndMessage.substring(topic.length() + 1);
                    channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
                    channel.basicPublish(EXCHANGE_NAME, routingKey, null, messageForSending.getBytes(StandardCharsets.UTF_8));
                    System.out.println("-- sent: [" + routingKey + "] (" + messageForSending + ")");
                } catch (IOException | TimeoutException e) {
                    e.printStackTrace();
                }
            }
        });
        t1.start();
    }
}
