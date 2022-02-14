package com.au.pratap.executors;


import com.au.pratap.StripFileTransactionListener;
import com.au.pratap.jms.config.BatchMessageListenerContainer;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;

import javax.jms.*;

/**
 * This tasklet is not needed. can be a simple spring bean which can start execution.
 */
@Component
public class MessageTasklet implements Tasklet {


    @Autowired
    private JmsTemplate jmsTemplate;

    //@Autowired
    private StripFileTransactionListener batchPersonListener;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws JMSException {
        /*BatchMessageListenerContainer container = new BatchMessageListenerContainer();


        Connection conn = jmsTemplate.getConnectionFactory().createConnection();
        conn.start();
        Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);


        // Set a batch size of 200 messages
        container.setBatchSize(60);
        container.setCacheLevel(BatchMessageListenerContainer.CACHE_CONSUMER);
        container.setConnectionFactory(jmsTemplate.getConnectionFactory());
        container.setDestination(session.createQueue(jmsTemplate.getDefaultDestinationName()));
        container.setMessageListener(batchPersonListener);
        container.setConcurrentConsumers(1);
        container.setMaxConcurrentConsumers(1);
        container.initialize();
        container.start();*/
        return RepeatStatus.FINISHED;
    }
}
