package com.springbatch.mongojobrepository.documents;

import com.springbatch.mongojobrepository.services.SequenceGeneratorService;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Document
public class MongoJobInstance {

    @Transient
    public static final String SEQUENCE_NAME = "job_instance_sequence";

    @Id
    private long id;

    private volatile Integer version;

    private String jobName;

    private String jobKey;

    public MongoJobInstance() {
    }

    public MongoJobInstance(String jobName) {
        this.id = SequenceGeneratorService.generateSequence(SEQUENCE_NAME);
        this.jobName = jobName;
    }

    public MongoJobInstance(String jobName, String jobKey) {
        this.id = SequenceGeneratorService.generateSequence(SEQUENCE_NAME);
        this.jobName = jobName;
        this.jobKey = jobKey;
    }

    public void incrementVersion() {
        if (version == null) {
            version = 0;
        } else {
            version = version + 1;
        }
    }

}
