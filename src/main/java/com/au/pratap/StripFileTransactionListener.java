package com.au.pratap;

import com.au.pratap.model.StripFileTransaction;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.jms.support.converter.MappingJackson2MessageConverter;
import org.springframework.jms.support.converter.MessageType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import javax.jms.JMSException;
import javax.jms.Session;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Pratap
 */
//@Component
public class StripFileTransactionListener implements SessionAwareBatchMessageListener<ActiveMQTextMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(StripFileTransactionListener.class);

    @Override
    public void onMessages(Session session, List<ActiveMQTextMessage> messages) throws JMSException {
        //Reading messages from MQ using the custom batch listener
        System.out.println("Message Size:" + messages.size());
        MappingJackson2MessageConverter converter = new MappingJackson2MessageConverter();
        converter.setTargetType(MessageType.TEXT);
        converter.setTypeIdPropertyName("_type");

        final List<StripFileTransaction> tTransactions = new ArrayList<>();

        for (ActiveMQTextMessage message : messages) {
            StripFileTransaction tStripFileTransactions = (StripFileTransaction) converter.fromMessage(message);
            System.out.println(tStripFileTransactions.toString());
            tTransactions.add(tStripFileTransactions);
        }

        //Call the account identifier service
        /*final HttpHeaders headers = new HttpHeaders();
        final HttpEntity<List<StripFileTransaction>> requestEntity = new HttpEntity<>(tTransactions, headers);

        final RestTemplate restTemplate = new RestTemplate();
        final StringBuilder stringBuilder = new StringBuilder("http://localhost:9000/accounts");

        //Call the account identifier MS
        ResponseEntity<List<StripFileTransaction>> tResponse = restTemplate.exchange(stringBuilder.toString(),
                HttpMethod.POST, requestEntity, new ParameterizedTypeReference<List<StripFileTransaction>>() {
        });

        //Iterate over the transactions and print the account number
        final List<StripFileTransaction> tFinalTransactions = tResponse.getBody();
        for(final StripFileTransaction tTransaction : tFinalTransactions){
            System.out.println("Account ID returned from the account identifier MS"+ tTransaction);
        }*/
    }
}
