package com.demo.RabbitMQ.noSpring.consumer;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class MultiTopicReceiverApp {
    private static final String EXCHANGE_NAME = "MultiTopicSender";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

        String queueName = channel.queueDeclare().getQueue();
        System.out.println("QUEUE NAME: " + queueName);

        System.out.println("pls inter: set_topic + php or java or js");
        Thread t1 = new Thread(() -> {
            byte[] data = new byte[20];
            while (!new String(data).equals("q")) {
                try {
                    System.in.read(data);
                    String incomeMessage = new String(data);
                    if (incomeMessage.split(" ")[0].equalsIgnoreCase("set_topic")) {
                        String topic = incomeMessage.substring(10).trim();
                        String routingKey = "topic." + topic;
                        channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
                        System.out.println("-- receiving messages with routing key (" + routingKey + "):");
                    } else if (incomeMessage.split(" ")[0].equalsIgnoreCase("del_topic")) {
                        String oldTopic = "topic." + incomeMessage.split(" ")[1].trim();
                        channel.queueUnbind(queueName, EXCHANGE_NAME, oldTopic);
                        System.out.println("-- receiving messages with routing key (" + oldTopic + ") canceled");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        t1.start();

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("-- received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });
    }
}
