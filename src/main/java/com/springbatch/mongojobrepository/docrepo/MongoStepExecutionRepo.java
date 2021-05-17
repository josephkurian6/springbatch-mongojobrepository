package com.springbatch.mongojobrepository.docrepo;

import com.springbatch.mongojobrepository.documents.MongoStepExecution;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface MongoStepExecutionRepo extends MongoRepository<MongoStepExecution, Long> {

    @Query(value = "{'id':?0}", fields = "{'shortContext':1}")
    MongoStepExecution findContextById(Long id);

    @Query(value = "{'id':?0}", fields = "{'id':1}")
    MongoStepExecution existById(Long id);

    @Query(value = "{'id':?0, 'mongoJobExecution.$id':?1}")
    MongoStepExecution findByStepExecutionAndJobExecution(Long stepId, Long jobId);

    @Query(value = "{'mongoJobExecution.$id':?0}", sort = "{'id':-1}")
    List<MongoStepExecution> findStepExecutionsByJobID(Long jobId);

    @Query(value = "{'jobInstanceId':?0, 'stepName':?1}", sort = "{'id':-1}")
    List<MongoStepExecution> findLastStepExecutionByJobInstanceIDAndName(Long jobInstanceId, String stepName, Pageable page);

    @Query(value = "{'jobInstanceId':?0, 'stepName':?1}", count = true)
    int countStepExecutions(Long jobInstanceId, String stepName);
}
