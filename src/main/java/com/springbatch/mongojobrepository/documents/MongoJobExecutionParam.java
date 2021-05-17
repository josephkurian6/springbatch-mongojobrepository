package com.springbatch.mongojobrepository.documents;

import lombok.Data;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Document
public class MongoJobExecutionParam {

    private String typeCd;

    private String keyName;

    private Object value;

    private String identifying;

    public MongoJobExecutionParam(){}

    public MongoJobExecutionParam(String typeCd, String keyName, Object value, String identifying) {
        this.typeCd = typeCd;
        this.keyName = keyName;
        this.value = value;
        this.identifying = identifying;
    }
}
