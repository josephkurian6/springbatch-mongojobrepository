package com.springbatch.mongojobrepository.dao;

import com.springbatch.mongojobrepository.docrepo.MongoStepExecutionRepo;
import com.springbatch.mongojobrepository.docrepo.MongoJobExecutionRepo;
import com.springbatch.mongojobrepository.documents.MongoJobExecution;
import com.springbatch.mongojobrepository.documents.MongoStepExecution;
import com.springbatch.mongojobrepository.utils.ConverterUtil;
import com.springbatch.mongojobrepository.utils.LockUtil;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.util.*;

@Service
public class MongoExecutionContextDao implements ExecutionContextDao {

    @Autowired
    private MongoJobExecutionRepo mongoJobExecutionRepo;

    @Autowired
    private MongoStepExecutionRepo mongoStepExecutionRepo;

    @Autowired
    private MongoOperations mongoOperations;

    private static final String EXECUTION_ID_ERROR_MESSAGE = "ExecutionId must not be null.";
    private static final String EXECUTION_CONTEXT_ERROR_MESSAGE = "The ExecutionContext must not be null.";

    @Override
    public ExecutionContext getExecutionContext(JobExecution jobExecution) {
        Assert.notNull(jobExecution.getId(), EXECUTION_ID_ERROR_MESSAGE);
        ExecutionContext executionContext = new ExecutionContext();
        MongoJobExecution mongoJobExecution = mongoJobExecutionRepo.findContextById(jobExecution.getId());
        if(mongoJobExecution != null) {
            Map<String, Object> context = mongoJobExecution.getShortContext();
            if(context != null && !context.isEmpty()) {
                executionContext = new ExecutionContext(context);
            }
        }
        return executionContext;
    }

    @Override
    public ExecutionContext getExecutionContext(StepExecution stepExecution) {
        Assert.notNull(stepExecution.getId(), EXECUTION_ID_ERROR_MESSAGE);
        ExecutionContext executionContext = new ExecutionContext();
        MongoStepExecution mongoStepExecution = mongoStepExecutionRepo.findContextById(stepExecution.getId());
        if(mongoStepExecution != null) {
            Map<String, Object> context = mongoStepExecution.getShortContext();
            if(context != null && !context.isEmpty()) {
                executionContext = new ExecutionContext(context);
            }
        }
        return executionContext;
    }

    @Override
    public void saveExecutionContext(JobExecution jobExecution) {
        Assert.notNull(jobExecution.getId(), EXECUTION_ID_ERROR_MESSAGE);
        Assert.notNull(jobExecution.getExecutionContext(), EXECUTION_CONTEXT_ERROR_MESSAGE);
        Map<String, Object> contextEntry = ConverterUtil.convertEntryToMap(jobExecution.getExecutionContext().entrySet());
        mongoOperations.updateFirst(Query.query(Criteria.where("id").is(jobExecution.getId())),
                Update.update("shortContext", contextEntry), MongoJobExecution.class);
    }

    @Override
    public void saveExecutionContext(StepExecution stepExecution) {
        Assert.notNull(stepExecution.getId(), EXECUTION_ID_ERROR_MESSAGE);
        Assert.notNull(stepExecution.getExecutionContext(), EXECUTION_CONTEXT_ERROR_MESSAGE);
        Map<String, Object> contextEntry = ConverterUtil.convertEntryToMap(stepExecution.getExecutionContext().entrySet());
        mongoOperations.updateFirst(Query.query(Criteria.where("id").is(stepExecution.getId())),
                Update.update("shortContext", contextEntry), MongoStepExecution.class);
    }

    @Override
    public void saveExecutionContexts(Collection<StepExecution> stepExecutions) {
        Assert.notNull(stepExecutions, "Attempt to save an null collection of step executions");
        for (StepExecution stepExecution: stepExecutions) {
            saveExecutionContext(stepExecution);
        }
    }

    @Override
    public void updateExecutionContext(JobExecution jobExecution) {
        saveExecutionContext(jobExecution);
    }

    @Override
    public void updateExecutionContext(StepExecution stepExecution) {
        synchronized(LockUtil.acquireStepLock(stepExecution.getId())) {
            saveExecutionContext(stepExecution);
            LockUtil.releaseStepLock(stepExecution.getId());
        }
    }
}
