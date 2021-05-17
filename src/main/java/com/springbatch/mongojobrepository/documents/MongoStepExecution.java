package com.springbatch.mongojobrepository.documents;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;
import java.util.Map;

@Data
@Document
public class MongoStepExecution {

    @Transient
    public static final String SEQUENCE_NAME = "step_execution_sequence";

    @Id
    private long id;

    private volatile Integer version;

    @DBRef
    @Setter(AccessLevel.NONE)
    private MongoJobExecution mongoJobExecution;

    private long jobInstanceId;

    private String stepName;

    private Date startTime;

    private Date endTime;

    private String status;

    private int commitCount;

    private int readCount;

    private int filterCount;

    private int writeCount;

    private int readSkipCount;

    private int writeSkipCount;

    private int processSkipCount;

    private int rollbackCount;

    private String exitCode;

    private String exitMessage;

    private Date lastUpdated;

    private Map<String, Object> shortContext;

    public MongoStepExecution() {}

    public MongoStepExecution(long id) {
        this.id = id;
    }

    public void setMongoJobExecution(MongoJobExecution mongoJobExecution) {
        this.mongoJobExecution = mongoJobExecution;
        this.jobInstanceId = mongoJobExecution.getJobInstance().getId();
    }

}
