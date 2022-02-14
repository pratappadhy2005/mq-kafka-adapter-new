package com.au.pratap;


import com.au.pratap.model.StripFileTransaction;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.support.converter.MappingJackson2MessageConverter;
import org.springframework.jms.support.converter.MessageType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import javax.jms.JMSException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Pratap
 */
@Component
public class MessageConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageConsumer.class);

    //We can;t achive using this annotation with batch message
    @JmsListener(destination = "person-queue", containerFactory = "batchJmsListenerContainerFactory")
    public void messageListener(List<ActiveMQTextMessage> messages) throws JMSException {
        LOGGER.info("Testing");
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
        //Submit Kafka
    }
}
