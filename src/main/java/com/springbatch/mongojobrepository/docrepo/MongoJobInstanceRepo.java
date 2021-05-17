package com.springbatch.mongojobrepository.docrepo;

import com.springbatch.mongojobrepository.documents.MongoJobInstance;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface MongoJobInstanceRepo extends MongoRepository<MongoJobInstance, Long> {
    List<MongoJobInstance> findByJobNameAndJobKey(String jobName, String jobKey);
    List<MongoJobInstance> findByJobName(String jobName, Pageable page);
    List<MongoJobInstance> findByJobNameRegex(String jobName, Pageable page);
    int countByJobName(String jobName);

    @Query(value="{'id':?0}", fields = "{'id':1, 'jobName':1, 'version':1}")
    MongoJobInstance fetchById(Long id);
}
