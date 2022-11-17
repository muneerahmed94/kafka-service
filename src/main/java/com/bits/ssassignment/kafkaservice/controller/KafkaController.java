package com.bits.ssassignment.kafkaservice.controller;

import com.bits.ssassignment.kafkaservice.model.KafkaMessage;
import com.bits.ssassignment.kafkaservice.model.Order;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.net.URISyntaxException;

@RestController
public class KafkaController {
    @RequestMapping(value="/kafka", method = RequestMethod.POST)
    @ResponseBody
    public KafkaMessage publish(@RequestBody KafkaMessage message) throws URISyntaxException {
        if (message.getTopic().equals("NewOrders")) {
            publishToRestaurantService(message);
        } else if (message.getTopic().equals("OrderUpdates")) {
            publishToOrderService(message);
        }

        message.setStatus("published");
        return message;
    }

    private void publishToOrderService(KafkaMessage message) {
        ObjectMapper mapper = new ObjectMapper();
        Order order;

        try {
            order = mapper.readValue(message.getContent(), Order.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        String url = "http://localhost:8081/order/" + order.getId();
        publish(url, order);
    }

    public String publishToRestaurantService(KafkaMessage message) throws URISyntaxException {
        String url = "http://localhost:8080/listenOrder";
        return publish(url, message);
    }

    public String publish(String url, KafkaMessage message) {
        RestTemplate restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<KafkaMessage> request = new HttpEntity<>(message, headers);

        ResponseEntity<KafkaMessage> response = restTemplate.postForEntity( url, request , KafkaMessage.class );
        return response.toString();
    }

    public String publish(String url, Order order) {
        RestTemplate restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<Order> request = new HttpEntity<>(order, headers);

        ResponseEntity<Order> response = restTemplate.postForEntity( url, request , Order.class );
        return response.toString();
    }
}
