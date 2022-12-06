package com.learnkafkastreams.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.learnkafkastreams.domain.OrderLineItemRecord;
import com.learnkafkastreams.domain.OrderRecord;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.topology.OrdersTopology;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

import static com.learnkafkastreams.producer.ProducerUtil.publishMessageSync;

@Slf4j
public class OrdersMockDataProducer {

    public static void main(String[] args) throws InterruptedException {
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        var orderItems = List.of(
                new OrderLineItemRecord("Bananas", 2, new BigDecimal("2.00")),
                new OrderLineItemRecord("Iphone Charger", 1, new BigDecimal("25.00"))
        );

        var orderItemsRestaurant = List.of(
                new OrderLineItemRecord("Pizza", 2, new BigDecimal("12.00")),
                new OrderLineItemRecord("Coffee", 1, new BigDecimal("3.00"))
        );

        var order1 = new OrderRecord(12345, "store_1234",
                new BigDecimal("27.00"),
                OrderType.GENERAL,
                orderItems,
                LocalDateTime.parse("2022-12-05T08:55:27")
        );

        var order2 = new OrderRecord(54321, "store_1234",
                new BigDecimal("123.00"),
                OrderType.RESTAURANT,
                orderItemsRestaurant,
                LocalDateTime.parse("2022-12-05T08:55:27")
        );

        var orders = List.of(
                order1,
                order2
        );
        orders
                .forEach(order -> {
                    try {
                        var greetingJSON = objectMapper.writeValueAsString(order);
                        var recordMetaData = publishMessageSync(OrdersTopology.ORDERS, order.orderId()+"", greetingJSON);
                        log.info("Published the order message : {} ", recordMetaData);
                    } catch (JsonProcessingException e) {
                        log.error("JsonProcessingException : {} ", e.getMessage(), e);
                        throw new RuntimeException(e);
                    }
                    catch (Exception e) {
                        log.error("Exception : {} ", e.getMessage(), e);
                        throw new RuntimeException(e);
                    }
                });

    }

}