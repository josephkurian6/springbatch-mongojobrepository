package com.springbatch.mongojobrepository.config;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.springbatch.mongojobrepository.dao.MongoExecutionContextDao;
import com.springbatch.mongojobrepository.dao.MongoJobExecutionDao;
import com.springbatch.mongojobrepository.dao.MongoJobInstanceDao;
import com.springbatch.mongojobrepository.dao.MongoStepExecutionDao;
import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.SimpleJobExplorer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.SimpleJobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;

@Configuration
public class MongoConfiguration implements BatchConfigurer {

    @Autowired
    private MongoExecutionContextDao mongoExecutionContextDao;

    @Autowired
    private MongoJobExecutionDao mongoJobExecutionDao;

    @Autowired
    private MongoJobInstanceDao mongoJobInstanceDao;

    @Autowired
    private MongoStepExecutionDao mongoStepExecutionDao;

    @Autowired
    private MongoDatabaseFactory dbFactory;

    @Override
    public JobRepository getJobRepository() {
        return new SimpleJobRepository(
                mongoJobInstanceDao,
                mongoJobExecutionDao,
                mongoStepExecutionDao,
                mongoExecutionContextDao
        );
    }

    @Override
    public MongoTransactionManager getTransactionManager() {
        TransactionOptions transactionOptions = TransactionOptions.builder().readConcern(ReadConcern.LOCAL)
                .writeConcern(WriteConcern.W1).build();
        return new MongoTransactionManager(dbFactory, transactionOptions);
    }

    @Override
    public SimpleJobLauncher getJobLauncher() throws Exception {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(getJobRepository());
        jobLauncher.afterPropertiesSet();
        return jobLauncher;
    }

    @Override
    public JobExplorer getJobExplorer() {
        return new SimpleJobExplorer(
                mongoJobInstanceDao,
                mongoJobExecutionDao,
                mongoStepExecutionDao,
                mongoExecutionContextDao
        );
    }
}
