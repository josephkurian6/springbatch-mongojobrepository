package com.springbatch.mongojobrepository.docrepo;

import com.springbatch.mongojobrepository.documents.MongoJobExecution;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface MongoJobExecutionRepo extends MongoRepository<MongoJobExecution, Long> {

    @Query(value = "{'id':?0}")
    MongoJobExecution getJobExecutionByID(Long id);

    @Query(value = "{'id':?0}", fields = "{'shortContext':1}")
    MongoJobExecution findContextById(Long id);

    @Query(value = "{'id':?0}", fields = "{'id':1}")
    MongoJobExecution existById(Long id);

    @Query(value = "{'id':?0}", fields = "{'jobInstance':1}")
    MongoJobExecution findJobExecutionInstance(Long id);

    @Query(value = "{'id':?0}", fields = "{'version':1, 'status':1}")
    MongoJobExecution findJobExecutionVersionAndStatus(Long id);

    @Query(value = "{'jobInstance.$id':?0}")
    List<MongoJobExecution> findJobExecutionByInstance(Long instanceId);

    @Query(value = "{'jobInstance.$id':?0}", sort = "{'createdTime':-1}")
    List<MongoJobExecution> iterableJobExecutionByInstance(Long instanceId, Pageable page);

    @Query("{'jobName':?0, 'startTime':{$ne:null}, 'endTime':null}")
    List<MongoJobExecution> findRunningByJobName(String jobName);
}
