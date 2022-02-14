package com.au.pratap.jms.config;

//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//


import java.util.concurrent.Executor;

import org.springframework.jms.config.AbstractJmsListenerContainerFactory;
import org.springframework.lang.Nullable;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.backoff.BackOff;

public class BatchJmsListenerContainerFactory extends AbstractJmsListenerContainerFactory<BatchMessageListenerContainer> {
    @Nullable
    private Executor taskExecutor;
    @Nullable
    private PlatformTransactionManager transactionManager;
    @Nullable
    private Integer cacheLevel;
    @Nullable
    private String cacheLevelName;
    @Nullable
    private String concurrency;
    @Nullable
    private Integer maxMessagesPerTask;
    @Nullable
    private Long receiveTimeout;
    @Nullable
    private Long recoveryInterval;
    @Nullable
    private BackOff backOff;

    public BatchJmsListenerContainerFactory() {
    }

    public void setTaskExecutor(Executor taskExecutor) {
        this.taskExecutor = taskExecutor;
    }

    public void setTransactionManager(PlatformTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public void setCacheLevel(Integer cacheLevel) {
        this.cacheLevel = cacheLevel;
    }

    public void setCacheLevelName(String cacheLevelName) {
        this.cacheLevelName = cacheLevelName;
    }

    public void setConcurrency(String concurrency) {
        this.concurrency = concurrency;
    }

    public void setMaxMessagesPerTask(Integer maxMessagesPerTask) {
        this.maxMessagesPerTask = maxMessagesPerTask;
    }

    public void setReceiveTimeout(Long receiveTimeout) {
        this.receiveTimeout = receiveTimeout;
    }

    public void setRecoveryInterval(Long recoveryInterval) {
        this.recoveryInterval = recoveryInterval;
    }

    public void setBackOff(BackOff backOff) {
        this.backOff = backOff;
    }

    protected BatchMessageListenerContainer createContainerInstance() {
        BatchMessageListenerContainer container =new BatchMessageListenerContainer();
        container.setBatchSize(60);
        container.setCacheLevel(BatchMessageListenerContainer.CACHE_CONSUMER);
        container.setConcurrentConsumers(1);
        container.setMaxConcurrentConsumers(1);
        return container;
    }

    protected void initializeContainer(BatchMessageListenerContainer container) {
        if (this.taskExecutor != null) {
            container.setTaskExecutor(this.taskExecutor);
        }

        if (this.transactionManager != null) {
            container.setTransactionManager(this.transactionManager);
        }

        if (this.cacheLevel != null) {
            container.setCacheLevel(this.cacheLevel);
        } else if (this.cacheLevelName != null) {
            container.setCacheLevelName(this.cacheLevelName);
        }

        if (this.concurrency != null) {
            container.setConcurrency(this.concurrency);
        }

        if (this.maxMessagesPerTask != null) {
            container.setMaxMessagesPerTask(this.maxMessagesPerTask);
        }

        if (this.receiveTimeout != null) {
            container.setReceiveTimeout(this.receiveTimeout);
        }

        if (this.backOff != null) {
            container.setBackOff(this.backOff);
            if (this.recoveryInterval != null) {
                this.logger.info("Ignoring recovery interval in DefaultJmsListenerContainerFactory in favor of BackOff");
            }
        } else if (this.recoveryInterval != null) {
            container.setRecoveryInterval(this.recoveryInterval);
        }

    }
}

