package com.springbatch.mongojobrepository;

import com.springbatch.mongojobrepository.controller.BatchController;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Date;

@SpringBootTest
class SpringBatchMongoApplicationTests {

	@Autowired
	BatchController batchController;

	@Test
	void contextLoads() throws JobParametersInvalidException, JobExecutionAlreadyRunningException,
			JobRestartException, JobInstanceAlreadyCompleteException {
		String response = (String) batchController.handle(new Date().toString());
		Assertions.assertEquals("printJob", response);
	}

}
