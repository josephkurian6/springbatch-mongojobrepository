package com.springbatch.mongojobrepository.dao;

import com.mongodb.client.result.UpdateResult;
import com.springbatch.mongojobrepository.docrepo.MongoStepExecutionRepo;
import com.springbatch.mongojobrepository.docrepo.MongoJobExecutionRepo;
import com.springbatch.mongojobrepository.documents.MongoJobExecution;
import com.springbatch.mongojobrepository.documents.MongoStepExecution;
import com.springbatch.mongojobrepository.services.SequenceGeneratorService;
import com.springbatch.mongojobrepository.utils.LockUtil;
import org.springframework.batch.core.*;
import org.springframework.batch.core.repository.dao.NoSuchObjectException;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.util.Collection;
import java.util.List;

@Service
public class MongoStepExecutionDao implements StepExecutionDao {

    @Autowired
    private MongoStepExecutionRepo mongoStepExecutionRepo;

    @Autowired
    private MongoJobExecutionRepo mongoJobExecutionRepo;

    @Autowired
    private MongoOperations mongoOperations;

    @Override
    public void saveStepExecution(StepExecution stepExecution) {
        Assert.isNull(stepExecution.getId(),
                "to-be-saved (not updated) StepExecution can't already have an id assigned");
        Assert.isNull(stepExecution.getVersion(),
                "to-be-saved (not updated) StepExecution can't already have a version assigned");
        validateStepExecution(stepExecution);
        stepExecution.setId(SequenceGeneratorService.generateSequence(MongoStepExecution.SEQUENCE_NAME));
        stepExecution.incrementVersion();
        MongoStepExecution mongoStepExecution = new MongoStepExecution(stepExecution.getId());
        setMongoStepExecution(stepExecution, mongoStepExecution);
        mongoStepExecutionRepo.insert(mongoStepExecution);
    }

    @Override
    public void saveStepExecutions(Collection<StepExecution> stepExecutions) {
        Assert.notNull(stepExecutions, "Attempt to save a null collection of step executions");
        if (!stepExecutions.isEmpty()) {
            for (StepExecution stepExecution: stepExecutions) {
                saveStepExecution(stepExecution);
            }
        }
    }

    @Override
    public void updateStepExecution(StepExecution stepExecution) {
        validateStepExecution(stepExecution);
        Assert.notNull(stepExecution.getId(), "StepExecution Id cannot be null. StepExecution must saved"
                + " before it can be updated.");
        synchronized (LockUtil.acquireStepLock(stepExecution.getId())) {
            if(mongoStepExecutionRepo.existById(stepExecution.getId()) != null) {
                Update update = setStepExecutionUpdate(stepExecution);
                update.set("version", stepExecution.getVersion() + 1);
                UpdateResult updateResult = mongoOperations.updateFirst(Query.query(Criteria.where("id").is(stepExecution.getId())
                        .and("version").is(stepExecution.getVersion())), update, MongoStepExecution.class);
                if(updateResult.getModifiedCount() == 0) {
                    throw new OptimisticLockingFailureException("Attempt to update step execution id="
                            + stepExecution.getId() + " with wrong version (" + stepExecution.getVersion() + ")");
                }
                stepExecution.incrementVersion();
            } else {
                throw new NoSuchObjectException("Invalid StepExecution, ID " + stepExecution.getId() + " not found.");
            }
            LockUtil.releaseStepLock(stepExecution.getId());
        }
    }

    @Override
    public StepExecution getStepExecution(JobExecution jobExecution, Long stepExecutionId) {
        MongoStepExecution mongoStepExecution = mongoStepExecutionRepo
                .findByStepExecutionAndJobExecution(stepExecutionId, jobExecution.getId());
        StepExecution stepExecution = null;
        if (mongoStepExecution != null) {
            stepExecution = setStepExecution(mongoStepExecution, jobExecution);
        }
        return stepExecution;
    }

    @Override
    public StepExecution getLastStepExecution(JobInstance jobInstance, String stepName) {
        List<MongoStepExecution> mongoStepExecutions = mongoStepExecutionRepo.findLastStepExecutionByJobInstanceIDAndName(jobInstance.getId(),
                stepName, PageRequest.of(0, 1, Sort.by(Sort.Order.desc("id"))));
        StepExecution stepExecution = null;
        if(mongoStepExecutions != null && !mongoStepExecutions.isEmpty()) {
            stepExecution = setStepExecution(mongoStepExecutions.get(0), setJobExecution(mongoStepExecutions.get(0).getMongoJobExecution()));
        }
        return stepExecution;
    }

    @Override
    public void addStepExecutions(JobExecution jobExecution) {
        List<MongoStepExecution> mongoStepExecutions = mongoStepExecutionRepo.findStepExecutionsByJobID(jobExecution.getId());
        for (MongoStepExecution mongoStepExecution: mongoStepExecutions) {
            setStepExecution(mongoStepExecution, jobExecution);
        }
    }

    @Override
    public int countStepExecutions(JobInstance jobInstance, String stepName) {
        return mongoStepExecutionRepo.countStepExecutions(jobInstance.getId(), stepName);
    }

    private void validateStepExecution(StepExecution stepExecution) {
        Assert.notNull(stepExecution, "stepExecution is required");
        Assert.notNull(stepExecution.getStepName(), "StepExecution step name cannot be null.");
        Assert.notNull(stepExecution.getStartTime(), "StepExecution start time cannot be null.");
        Assert.notNull(stepExecution.getStatus(), "StepExecution status cannot be null.");
    }

    private MongoStepExecution setMongoStepExecution(StepExecution stepExecution,
                                                     MongoStepExecution mongoStepExecution) {
        mongoStepExecution.setVersion(stepExecution.getVersion());
        mongoStepExecution.setStepName(stepExecution.getStepName());
        mongoStepExecution.setMongoJobExecution(getJobExecution(stepExecution.getJobExecution().getId()));
        mongoStepExecution.setStartTime(stepExecution.getStartTime());
        mongoStepExecution.setEndTime(stepExecution.getEndTime());
        mongoStepExecution.setStatus(stepExecution.getStatus().name());
        mongoStepExecution.setCommitCount(stepExecution.getCommitCount());
        mongoStepExecution.setReadCount(stepExecution.getReadCount());
        mongoStepExecution.setFilterCount(stepExecution.getFilterCount());
        mongoStepExecution.setWriteCount(stepExecution.getWriteCount());
        mongoStepExecution.setExitCode(stepExecution.getExitStatus().getExitCode());
        mongoStepExecution.setExitMessage(stepExecution.getExitStatus().getExitDescription());
        mongoStepExecution.setReadSkipCount(stepExecution.getReadSkipCount());
        mongoStepExecution.setWriteSkipCount(stepExecution.getWriteSkipCount());
        mongoStepExecution.setProcessSkipCount(stepExecution.getProcessSkipCount());
        mongoStepExecution.setRollbackCount(stepExecution.getRollbackCount());
        mongoStepExecution.setLastUpdated(stepExecution.getLastUpdated());
        return mongoStepExecution;
    }

    private Update setStepExecutionUpdate(StepExecution stepExecution) {
        Update update = new Update();
        update.set("startTime", stepExecution.getStartTime());
        update.set("endTime", stepExecution.getEndTime());
        update.set("status", stepExecution.getStatus().name());
        update.set("commitCount", stepExecution.getCommitCount());
        update.set("readCount", stepExecution.getReadCount());
        update.set("filterCount", stepExecution.getFilterCount());
        update.set("writeCount", stepExecution.getWriteCount());
        update.set("exitCode", stepExecution.getExitStatus().getExitCode());
        update.set("exitMessage", stepExecution.getExitStatus().getExitDescription());
        update.set("readSkipCount", stepExecution.getReadSkipCount());
        update.set("processSkipCount", stepExecution.getProcessSkipCount());
        update.set("writeSkipCount", stepExecution.getWriteSkipCount());
        update.set("rollbackCount", stepExecution.getRollbackCount());
        update.set("lastUpdated", stepExecution.getLastUpdated());
        return update;
    }

    private StepExecution setStepExecution(MongoStepExecution mongoStepExecution, JobExecution jobExecution) {
        StepExecution stepExecution = null;
        if(jobExecution != null) {
            stepExecution = new StepExecution(mongoStepExecution.getStepName(), jobExecution, mongoStepExecution.getId());
            stepExecution.setVersion(mongoStepExecution.getVersion());
            stepExecution.setStartTime(mongoStepExecution.getStartTime());
            stepExecution.setEndTime(mongoStepExecution.getEndTime());
            stepExecution.setStatus(BatchStatus.valueOf(mongoStepExecution.getStatus()));
            stepExecution.setCommitCount(mongoStepExecution.getCommitCount());
            stepExecution.setReadCount(mongoStepExecution.getReadCount());
            stepExecution.setFilterCount(mongoStepExecution.getFilterCount());
            stepExecution.setWriteCount(mongoStepExecution.getWriteCount());
            stepExecution.setExitStatus(new ExitStatus(mongoStepExecution.getExitCode(), mongoStepExecution.getExitMessage()));
            stepExecution.setReadSkipCount(mongoStepExecution.getReadSkipCount());
            stepExecution.setWriteSkipCount(mongoStepExecution.getWriteSkipCount());
            stepExecution.setProcessSkipCount(mongoStepExecution.getProcessSkipCount());
            stepExecution.setRollbackCount(mongoStepExecution.getRollbackCount());
            stepExecution.setLastUpdated(mongoStepExecution.getLastUpdated());
        }
        return stepExecution;
    }

    private JobExecution setJobExecution(MongoJobExecution mongoJobExecution) {
        JobExecution jobExecution = new JobExecution(mongoJobExecution.getId());
        jobExecution.setStartTime(mongoJobExecution.getStartTime());
        jobExecution.setEndTime(mongoJobExecution.getEndTime());
        jobExecution.setStatus(BatchStatus.valueOf(mongoJobExecution.getStatus()));
        jobExecution.setExitStatus(new ExitStatus(mongoJobExecution.getExitCode(), mongoJobExecution.getExitMessage()));
        jobExecution.setCreateTime(mongoJobExecution.getCreatedTime());
        jobExecution.setLastUpdated(mongoJobExecution.getLastUpdated());
        jobExecution.setVersion(mongoJobExecution.getVersion());
        return jobExecution;
    }

    private MongoJobExecution getJobExecution(Long id) {
        return mongoJobExecutionRepo.getJobExecutionByID(id);
    }
}
