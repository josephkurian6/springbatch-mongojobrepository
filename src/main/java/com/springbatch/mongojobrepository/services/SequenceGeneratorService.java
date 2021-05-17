package com.springbatch.mongojobrepository.services;

import com.springbatch.mongojobrepository.documents.DatabaseSequence;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Objects;
import static org.springframework.data.mongodb.core.FindAndModifyOptions.options;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

@Service
public class SequenceGeneratorService {

    @Autowired
    MongoOperations mongoOperations;

    private static MongoOperations staticMongoOperations;

    @PostConstruct
    public synchronized void init() {
        staticMongoOperations = mongoOperations;
    }

    public static long generateSequence(String seqName) {
        DatabaseSequence counter = staticMongoOperations.findAndModify(query(where("_id").is(seqName)),
                new Update().inc("seq",1), options().returnNew(true).upsert(true),
                DatabaseSequence.class);
        return !Objects.isNull(counter) ? counter.getSeq() : 1;
    }

}
