package com.springbatch.mongojobrepository.controller;

import com.springbatch.mongojobrepository.services.MongoService;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BatchController {

    @Autowired
    MongoService mongoService;

    @RequestMapping("/job/{param}")
    public Object handle(@PathVariable String param) throws JobParametersInvalidException,
            JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
        return mongoService.runJob(param);
    }
}
