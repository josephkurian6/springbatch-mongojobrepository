package com.springbatch.mongojobrepository.documents;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;
import java.util.List;
import java.util.Map;

@Data
@Document
public class MongoJobExecution {

    @Transient
    public static final String SEQUENCE_NAME = "job_execution_sequence";

    @Id
    private long id;

    private volatile Integer version;

    @DBRef
    @Setter(AccessLevel.NONE)
    private MongoJobInstance jobInstance;

    private String jobName;

    private Date createdTime;

    private Date startTime;

    private Date endTime;

    private String status;

    private String exitCode;

    private String exitMessage;

    private Date lastUpdated;

    private String jobConfigurationLocation;

    private Map<String, Object> shortContext;

    private List<MongoJobExecutionParam> params;

    public MongoJobExecution() {}

    public MongoJobExecution(long id) {
        this.id = id;
    }

    public void setJobInstance(MongoJobInstance jobInstance) {
        this.jobInstance = jobInstance;
        this.jobName = jobInstance.getJobName();
    }
}
