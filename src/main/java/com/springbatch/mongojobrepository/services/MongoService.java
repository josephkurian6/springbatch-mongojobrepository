package com.springbatch.mongojobrepository.services;

import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.*;

@Service
public class MongoService {

    @Autowired
    JobLauncher jobLauncher;

    @Autowired
    Job printJob;

    public Object runJob(String param) throws JobParametersInvalidException,
            JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {

        Map<String, JobParameter> jobParameterMap = new HashMap<>();
        jobParameterMap.put("param", new JobParameter(param));
        JobParameters jobParameters = new JobParameters(jobParameterMap);

        jobLauncher.run(printJob, jobParameters);
        return printJob.getName();
    }

}
