package com.springbatch.mongojobrepository.dao;

import com.mongodb.client.result.UpdateResult;
import com.springbatch.mongojobrepository.docrepo.MongoJobExecutionRepo;
import com.springbatch.mongojobrepository.docrepo.MongoJobInstanceRepo;
import com.springbatch.mongojobrepository.documents.MongoJobExecution;
import com.springbatch.mongojobrepository.documents.MongoJobExecutionParam;
import com.springbatch.mongojobrepository.documents.MongoJobInstance;
import com.springbatch.mongojobrepository.services.SequenceGeneratorService;
import com.springbatch.mongojobrepository.utils.LockUtil;
import org.springframework.batch.core.*;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.NoSuchObjectException;
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

import java.util.*;

@Service
public class MongoJobExecutionDao implements JobExecutionDao {

    @Autowired
    private MongoJobExecutionRepo mongoJobExecutionRepo;

    @Autowired
    private MongoJobInstanceRepo mongoJobInstanceRepo;

    @Autowired
    private MongoOperations mongoOperations;

    @Override
    public void saveJobExecution(JobExecution jobExecution) {
        validateJobExecution(jobExecution);
        jobExecution.incrementVersion();
        jobExecution.setId(SequenceGeneratorService.generateSequence(MongoJobExecution.SEQUENCE_NAME));
        MongoJobExecution mongoJobExecution = new MongoJobExecution(jobExecution.getId());
        setMongoJobExecution(jobExecution, mongoJobExecution);
        mongoJobExecutionRepo.insert(mongoJobExecution);
    }

    @Override
    public void updateJobExecution(JobExecution jobExecution) {
        validateJobExecution(jobExecution);
        Assert.notNull(jobExecution.getId(),
                "JobExecution ID cannot be null. JobExecution must be saved before it can be updated");

        Assert.notNull(jobExecution.getVersion(),
                "JobExecution version cannot be null. JobExecution must be saved before it can be updated");
        synchronized (LockUtil.acquireJobLock(jobExecution.getId())) {
            if(mongoJobExecutionRepo.existById(jobExecution.getId()) != null) {
                Update update = setJobExecutionUpdate(jobExecution);
                update.set("version", jobExecution.getVersion() + 1);
                UpdateResult updateResult = mongoOperations.updateFirst(Query.query(Criteria.where("id").is(jobExecution.getId())
                        .and("version").is(jobExecution.getVersion())), update, MongoJobExecution.class);
                if(updateResult.getModifiedCount() == 0) {
                    throw new OptimisticLockingFailureException("Attempt to update job execution id="
                            + jobExecution.getId() + " with wrong version (" + jobExecution.getVersion() + ")");
                }
            } else {
                throw new NoSuchObjectException("Invalid JobExecution, ID " + jobExecution.getId() + " not found.");
            }
            jobExecution.incrementVersion();
            LockUtil.releaseJobLock(jobExecution.getId());
        }
    }

    @Override
    public List<JobExecution> findJobExecutions(JobInstance jobInstance) {
        List<MongoJobExecution> mongoJobExecutions = mongoJobExecutionRepo.findJobExecutionByInstance(jobInstance.getInstanceId());
        List<JobExecution> jobExecutions = new ArrayList<>();
        if(mongoJobExecutions != null && !mongoJobExecutions.isEmpty()) {
            for (MongoJobExecution mongoJobExecution : mongoJobExecutions) {
                jobExecutions.add(setJobExecution(mongoJobExecution));
            }
        }
        return jobExecutions;
    }

    @Override
    public JobExecution getLastJobExecution(JobInstance jobInstance) {

        List<MongoJobExecution> mongoJobExecutions = mongoJobExecutionRepo
                .iterableJobExecutionByInstance(jobInstance.getInstanceId(),
                        PageRequest.of(0, 1, Sort.by(Sort.Order.desc("createdTime"))));
        JobExecution jobExecution = null;
        if(mongoJobExecutions != null && !mongoJobExecutions.isEmpty()) {
            jobExecution = setJobExecution(mongoJobExecutions.get(0));
        }
        return jobExecution;
    }

    @Override
    public Set<JobExecution> findRunningJobExecutions(String jobName) {
        List<MongoJobExecution> mongoJobExecutions = mongoJobExecutionRepo.findRunningByJobName(jobName);
        final Set<JobExecution> jobExecutions = new HashSet<>();
        if(mongoJobExecutions != null && !mongoJobExecutions.isEmpty()) {
            for (MongoJobExecution mongoJobExecution: mongoJobExecutions) {
                jobExecutions.add(setJobExecution(mongoJobExecution));
            }
        }
        return jobExecutions;
    }

    @Override
    public JobExecution getJobExecution(Long executionId) {
        Optional<MongoJobExecution> mongoJobExecutionOpt = mongoJobExecutionRepo.findById(executionId);
        JobExecution jobExecution = null;
        if(mongoJobExecutionOpt.isPresent()) {
            jobExecution = setJobExecution(mongoJobExecutionOpt.get());
        }
        return jobExecution;
    }

    @Override
    public void synchronizeStatus(JobExecution jobExecution) {
        MongoJobExecution mongoJobExecution = mongoJobExecutionRepo.findJobExecutionVersionAndStatus(jobExecution.getId());
        if(mongoJobExecution != null && mongoJobExecution.getVersion().intValue() != jobExecution.getVersion().intValue()) {
            jobExecution.upgradeStatus(BatchStatus.valueOf(mongoJobExecution.getStatus()));
            jobExecution.setVersion(mongoJobExecution.getVersion());
        }
    }

    private void validateJobExecution(JobExecution jobExecution) {
        Assert.notNull(jobExecution, "jobExecution cannot be null");
        Assert.notNull(jobExecution.getJobId(), "JobExecution Job-Id cannot be null.");
        Assert.notNull(jobExecution.getStatus(), "JobExecution status cannot be null.");
        Assert.notNull(jobExecution.getCreateTime(), "JobExecution create time cannot be null");
    }

    private MongoJobExecution setMongoJobExecution(JobExecution jobExecution, MongoJobExecution mongoJobExecution) {
        mongoJobExecution.setCreatedTime(jobExecution.getCreateTime());
        mongoJobExecution.setStartTime(jobExecution.getStartTime());
        mongoJobExecution.setEndTime(jobExecution.getEndTime());
        mongoJobExecution.setStatus(jobExecution.getStatus().name());
        mongoJobExecution.setExitCode(jobExecution.getExitStatus().getExitCode());
        mongoJobExecution.setExitMessage(jobExecution.getExitStatus().getExitDescription());
        mongoJobExecution.setVersion(jobExecution.getVersion());
        mongoJobExecution.setLastUpdated(jobExecution.getLastUpdated());
        mongoJobExecution.setJobInstance(getMongoJobInstance(jobExecution.getJobId()));
        mongoJobExecution.setParams(setMongoJobExecutionParams(jobExecution.getJobParameters()));
        mongoJobExecution.setJobConfigurationLocation(jobExecution.getJobConfigurationName());
        return mongoJobExecution;
    }

    private JobExecution setJobExecution(MongoJobExecution mongoJobExecution) {
        JobExecution jobExecution = new JobExecution(mongoJobExecution.getId(), setJobParameters(mongoJobExecution.getParams()),
                mongoJobExecution.getJobConfigurationLocation());
        jobExecution.setStartTime(mongoJobExecution.getStartTime());
        jobExecution.setEndTime(mongoJobExecution.getEndTime());
        jobExecution.setStatus(BatchStatus.valueOf(mongoJobExecution.getStatus()));
        jobExecution.setExitStatus(new ExitStatus(mongoJobExecution.getExitCode(), mongoJobExecution.getExitMessage()));
        jobExecution.setCreateTime(mongoJobExecution.getCreatedTime());
        jobExecution.setLastUpdated(mongoJobExecution.getLastUpdated());
        jobExecution.setVersion(mongoJobExecution.getVersion());
        if (mongoJobExecution.getJobInstance() != null) {
            jobExecution.setJobInstance(setJobInstance(mongoJobExecution.getJobInstance()));
        }
        return jobExecution;
    }

    private Update setJobExecutionUpdate(JobExecution jobExecution) {
        Update update = new Update();
        update.set("createdTime", jobExecution.getCreateTime());
        update.set("startTime", jobExecution.getStartTime());
        update.set("endTime", jobExecution.getEndTime());
        update.set("status", jobExecution.getStatus().name());
        update.set("exitCode", jobExecution.getExitStatus().getExitCode());
        update.set("exitMessage", jobExecution.getExitStatus().getExitDescription());
        update.set("lastUpdated", jobExecution.getLastUpdated());
        return update;
    }

    private MongoJobInstance getMongoJobInstance(long instanceId) {
        return mongoJobInstanceRepo.fetchById(instanceId);
    }

    private JobInstance setJobInstance(MongoJobInstance mongoJobInstance) {
        JobInstance jobInstance = new JobInstance(mongoJobInstance.getId(), mongoJobInstance.getJobName());
        jobInstance.setVersion(mongoJobInstance.getVersion());
        return jobInstance;
    }

    private List<MongoJobExecutionParam> setMongoJobExecutionParams(JobParameters jobParameters) {
        List<MongoJobExecutionParam> mongoJobExecutionParams = new ArrayList<>();
        for (Map.Entry<String, JobParameter> entry : jobParameters.getParameters()
                .entrySet()) {
            mongoJobExecutionParams.add(setMongoJobExecutionParam(entry.getValue().getType(), entry.getKey(),
                    entry.getValue().getValue(), entry.getValue().isIdentifying()));
        }
        return mongoJobExecutionParams;
    }

    private MongoJobExecutionParam setMongoJobExecutionParam(JobParameter.ParameterType type, String key,
                                                             Object value, boolean identifying) {
        return new MongoJobExecutionParam(type.name(), key, value, (identifying ? "Y":"N"));
    }

    private JobParameters setJobParameters(List<MongoJobExecutionParam> mongoJobExecutionParams) {
        Map<String, JobParameter> parameterMap = new HashMap<>();
        if(mongoJobExecutionParams != null) {
            for (MongoJobExecutionParam mongoJobExecutionParam : mongoJobExecutionParams) {
                parameterMap.put(mongoJobExecutionParam.getKeyName(), setJobParameter(mongoJobExecutionParam));
            }
        }
        return new JobParameters(parameterMap);
    }

    private JobParameter setJobParameter(MongoJobExecutionParam mongoJobExecutionParam) {
        JobParameter.ParameterType type = JobParameter.ParameterType.valueOf(mongoJobExecutionParam.getTypeCd());
        JobParameter value = null;
        if (type == JobParameter.ParameterType.STRING) {
            value = new JobParameter((String) mongoJobExecutionParam.getValue(),
                    mongoJobExecutionParam.getIdentifying().equalsIgnoreCase("Y"));
        } else if (type == JobParameter.ParameterType.LONG) {
            value = new JobParameter((Long) mongoJobExecutionParam.getValue(),
                    mongoJobExecutionParam.getIdentifying().equalsIgnoreCase("Y"));
        } else if (type == JobParameter.ParameterType.DOUBLE) {
            value = new JobParameter((Double) mongoJobExecutionParam.getValue(),
                    mongoJobExecutionParam.getIdentifying().equalsIgnoreCase("Y"));
        } else if (type == JobParameter.ParameterType.DATE) {
            value = new JobParameter((Date) mongoJobExecutionParam.getValue(),
                    mongoJobExecutionParam.getIdentifying().equalsIgnoreCase("Y"));
        }
        return value;
    }
}
