package com.au.pratap.config;

import com.au.pratap.executors.MessageTasklet;
import com.au.pratap.jms.config.BatchJmsListenerContainerFactory;
import com.au.pratap.model.StripFileTransaction;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.jms.JmsItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.config.JmsListenerContainerFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.converter.MappingJackson2MessageConverter;
import org.springframework.jms.support.converter.MessageConverter;
import org.springframework.jms.support.converter.MessageType;

import javax.jms.ConnectionFactory;
import java.util.stream.IntStream;

@EnableJms
@Configuration
@EnableBatchProcessing
public class SpringBatchJmsConfig {

    public static final Logger logger = LoggerFactory.getLogger(SpringBatchJmsConfig.class.getName());

    @Autowired
    private MessageTasklet messageTasklet;

    @Autowired
    private JmsTemplate jmsTemplate;

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Bean
    public JmsListenerContainerFactory<?> queueListenerFactory() {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        factory.setMessageConverter(messageConverter());
        return factory;
    }

    @Bean
    public ActiveMQConnectionFactory receiverActiveMQConnectionFactory() {
        ActiveMQConnectionFactory activeMQConnectionFactory =
                new ActiveMQConnectionFactory();
        activeMQConnectionFactory.setBrokerURL("tcp://localhost:61616");
        return activeMQConnectionFactory;
    }

    @Bean
    public BatchJmsListenerContainerFactory batchJmsListenerContainerFactory() {

        BatchJmsListenerContainerFactory factory = new BatchJmsListenerContainerFactory();
        factory.setConnectionFactory(receiverActiveMQConnectionFactory());
        return factory;
    }

    @Bean
    public MessageConverter messageConverter() {
        MappingJackson2MessageConverter converter = new MappingJackson2MessageConverter();
        converter.setTargetType(MessageType.TEXT);
        converter.setTypeIdPropertyName("_type");
        return converter;
    }


    @Bean
    public JmsItemReader<StripFileTransaction> personJmsItemReader(MessageConverter messageConverter) {
        JmsItemReader<StripFileTransaction> personJmsItemReader = new JmsItemReader<>();
        personJmsItemReader.setJmsTemplate(jmsTemplate);
        personJmsItemReader.setItemType(StripFileTransaction.class);
        return personJmsItemReader;
    }

    @Bean
    public FlatFileItemWriter<StripFileTransaction> personFlatFileItemWriter() {
        FlatFileItemWriter<StripFileTransaction> personFlatFileItemWriter = new FlatFileItemWriter<>();
        personFlatFileItemWriter.setLineAggregator(person -> person.toString());
        personFlatFileItemWriter.setLineSeparator(System.lineSeparator());
        personFlatFileItemWriter.setResource(new FileSystemResource("person.txt"));
        return personFlatFileItemWriter;
    }

    @Bean
    public Job importUserJob() {
        return jobBuilderFactory.get("importUserJob")
                .incrementer(new RunIdIncrementer())
                .listener(jobExecutionListener())
                .flow(step2())
                .end()
                .build();
    }

    private Step step1() {
        return stepBuilderFactory.get("step1")
                .<StripFileTransaction, StripFileTransaction>chunk(10)
                .reader(personJmsItemReader(messageConverter()))
                .writer(personFlatFileItemWriter())
                .build();
    }

    private Step step2() {
        return stepBuilderFactory.get("step2")
                .tasklet(messageTasklet)
                .build();
    }

    @Bean
    public JobExecutionListener jobExecutionListener() {
        return new JobExecutionListener() {
            @Override
            public void beforeJob(JobExecution jobExecution) {
                IntStream.rangeClosed(1, 100).forEach(token -> {
                    /*StripFileTransaction[] people = {new StripFileTransaction("Jack", "Ryan"), new StripFileTransaction("Raymond", "Red"), new StripFileTransaction("Olivia", "Dunham"),
                            new StripFileTransaction("Walter", "Bishop"), new StripFileTransaction("Harry", "Bosch")};
                    for (StripFileTransaction person : people) {
                        //jmsTemplate.convertAndSend(person);
                    }*/
                });
                StripFileTransaction person = new StripFileTransaction();
                //jmsTemplate.convertAndSend(person);
            }

            @Override
            public void afterJob(JobExecution jobExecution) {

            }
        };
    }

}

