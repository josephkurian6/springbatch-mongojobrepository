package com.springbatch.mongojobrepository.dao;

import com.springbatch.mongojobrepository.docrepo.MongoJobExecutionRepo;
import com.springbatch.mongojobrepository.docrepo.MongoJobInstanceRepo;
import com.springbatch.mongojobrepository.documents.MongoJobExecution;
import com.springbatch.mongojobrepository.documents.MongoJobInstance;
import org.springframework.batch.core.*;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;

@Service
public class MongoJobInstanceDao implements JobInstanceDao {

    @Autowired
    private MongoJobInstanceRepo mongoJobInstanceRepo;

    @Autowired
    private MongoJobExecutionRepo mongoJobExecutionRepo;

    @Autowired
    private MongoOperations mongoOperations;

    private final JobKeyGenerator<JobParameters> jobKeyGenerator = new DefaultJobKeyGenerator();

    @Override
    public JobInstance createJobInstance(String jobName, JobParameters jobParameters) {
        Assert.notNull(jobName, "Job name must not be null.");
        Assert.notNull(jobParameters, "JobParameters must not be null.");
        List<MongoJobInstance> mongoJobInstances = mongoJobInstanceRepo.findByJobNameAndJobKey(jobName,
                jobKeyGenerator.generateKey(jobParameters));
        Assert.state(mongoJobInstances == null || mongoJobInstances.isEmpty(),
                "JobInstance must not already exist");
        MongoJobInstance mongoJobInstance = new MongoJobInstance(jobName, jobKeyGenerator.generateKey(jobParameters));
        mongoJobInstance.incrementVersion();
        mongoJobInstanceRepo.insert(mongoJobInstance);
        return setJobInstance(mongoJobInstance);
    }

    @Override
    @Nullable
    public JobInstance getJobInstance(String jobName, JobParameters jobParameters) {
        Assert.notNull(jobName, "Job name must not be null.");
        Assert.notNull(jobParameters, "JobParameters must not be null.");
        List<MongoJobInstance> mongoJobInstances = mongoJobInstanceRepo.findByJobNameAndJobKey(jobName,
                jobKeyGenerator.generateKey(jobParameters));
        JobInstance jobInstance = null;
        if(mongoJobInstances != null && !mongoJobInstances.isEmpty()) {
            Assert.state(mongoJobInstances.size() == 1, "instance count must be 1 but was " + mongoJobInstances.size());
            jobInstance = setJobInstance(mongoJobInstances.get(0));
        }
        return jobInstance;
    }

    @Override
    @Nullable
    public JobInstance getJobInstance(Long instanceId) {
        MongoJobInstance mongoJobInstance = mongoJobInstanceRepo.fetchById(instanceId);
        JobInstance jobInstance = null;
        if(mongoJobInstance != null) {
            jobInstance = setJobInstance(mongoJobInstance);
        }
        return jobInstance;
    }

    @Override
    @Nullable
    public JobInstance getJobInstance(JobExecution jobExecution) {
        MongoJobExecution mongoJobExecution = mongoJobExecutionRepo.findJobExecutionInstance(jobExecution.getId());
        MongoJobInstance mongoJobInstance = mongoJobInstanceRepo
                .fetchById(mongoJobExecution.getJobInstance().getId());
        JobInstance jobInstance = null;
        if(mongoJobInstance != null) {
            jobInstance = setJobInstance(mongoJobInstance);
        }
        return jobInstance;
    }

    @Override
    public List<JobInstance> getJobInstances(String jobName, int start, int count) {
        List<MongoJobInstance> mongoJobInstances = mongoJobInstanceRepo.findByJobName(jobName,
                PageRequest.of(start, count, Sort.by(Sort.Order.desc("id"))));
        List<JobInstance> jobInstances = new ArrayList<>();
        if(mongoJobInstances != null && !mongoJobInstances.isEmpty()) {
            for(MongoJobInstance mongoJobInstance: mongoJobInstances) {
                jobInstances.add(setJobInstance(mongoJobInstance));
            }
        }
        return jobInstances;
    }

    @Override
    @Nullable
    public JobInstance getLastJobInstance(String jobName) {
        List<MongoJobInstance> mongoJobInstances = mongoJobInstanceRepo.findByJobName(jobName,
                PageRequest.of(0, 1, Sort.by(Sort.Order.desc("id"))));
        JobInstance jobInstance = null;
        if(mongoJobInstances != null && !mongoJobInstances.isEmpty()) {
            jobInstance = setJobInstance(mongoJobInstances.get(0));
        }
        return jobInstance;
    }

    @Override
    public List<String> getJobNames() {
        return mongoOperations.findDistinct("jobName", MongoJobInstance.class, String.class);
    }

    @Override
    public List<JobInstance> findJobInstancesByName(String jobName, int start, int count) {
        List<MongoJobInstance> mongoJobInstances = mongoJobInstanceRepo.findByJobNameRegex(jobName,
                PageRequest.of(start, count, Sort.by(Sort.Order.desc("id"))));
        List<JobInstance> jobInstances = new ArrayList<>();
        if(mongoJobInstances != null && !mongoJobInstances.isEmpty()) {
            for(MongoJobInstance mongoJobInstance: mongoJobInstances) {
                jobInstances.add(setJobInstance(mongoJobInstance));
            }
        }
        return jobInstances;
    }

    @Override
    public int getJobInstanceCount(String jobName) {
        return mongoJobInstanceRepo.countByJobName(jobName);
    }

    private JobInstance setJobInstance(MongoJobInstance mongoJobInstance) {
        JobInstance jobInstance = new JobInstance(mongoJobInstance.getId(), mongoJobInstance.getJobName());
        jobInstance.setVersion(mongoJobInstance.getVersion());
        return jobInstance;
    }
}
