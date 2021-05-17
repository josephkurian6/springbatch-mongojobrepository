package com.springbatch.mongojobrepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@EnableBatchProcessing
@Configuration
public class SpringBatchJob {

    static final String JOB_NAME = "printJob";

    Logger logger = LoggerFactory.getLogger(SpringBatchJob.class);

    @Bean
    public Job printJob(JobBuilderFactory jobBuilderFactory, Step printStep) {
        return jobBuilderFactory.get(JOB_NAME)
                .incrementer(new RunIdIncrementer())
                .start(printStep).build();
    }

    @Bean
    Step printStep(StepBuilderFactory stepBuilderFactory) {
        return stepBuilderFactory.get("displayStep").tasklet((contribution, chunkContext) -> {
            logger.info("--Print Job--");
            return RepeatStatus.FINISHED;
        }).build();
    }
}
