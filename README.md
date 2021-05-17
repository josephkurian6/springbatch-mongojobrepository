# springbatch-mongojobrepository
Spring batch job repository implemnation using mongo document repositories. 
Implemented JobExecutionDAO, StepExecutionDAO, JobInstanceDAO and ExecutionContextDAO using mongodb and have mongo transcational support (Version 4.0+ needed)

# Getting Started

## Set-Up
1. Clone project to local environment
2. Enter your mongo-url in application.properties file
3. Optional - Configure your required transactionOptions of "MongoTransactionManager" in "config/MongoConfiguration.java" file
   (This project is developed using Mongo Atlas cluster with 3 replica, readConcern as readConcern.LOCAL and writeConcern as WriteConcern.W1)
   
## Build and Test
1. Test using: junit
2. Build and run using: mvn spring-boot:run

# Read Me First
The following was discovered as part of building this project:

### Reference Documentation
For further reference, please consider the following sections:

* [Official Apache Maven documentation](https://maven.apache.org/guides/index.html)
* [Spring Boot Maven Plugin Reference Guide](https://docs.spring.io/spring-boot/docs/2.4.5/maven-plugin/reference/html/)
* [Spring Web](https://docs.spring.io/spring-boot/docs/2.4.5/reference/htmlsingle/#boot-features-developing-web-applications)
* [Spring Data MongoDB](https://docs.spring.io/spring-boot/docs/2.4.5/reference/htmlsingle/#boot-features-mongodb)
* [Spring Boot DevTools](https://docs.spring.io/spring-boot/docs/2.4.5/reference/htmlsingle/#using-boot-devtools)
* [Spring Batch](https://docs.spring.io/spring-boot/docs/2.4.5/reference/htmlsingle/#howto-batch-applications)

### Guides
The following guides illustrate how to use some features concretely:

* [Building a RESTful Web Service](https://spring.io/guides/gs/rest-service/)
* [Building REST services with Spring](https://spring.io/guides/tutorials/bookmarks/)
* [Accessing Data with MongoDB](https://spring.io/guides/gs/accessing-data-mongodb/)
* [Creating a Batch Service](https://spring.io/guides/gs/batch-processing/)



