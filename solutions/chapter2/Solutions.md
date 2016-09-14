# Chapter 2 
## Exercise 2.1
In your local Flink installation update the `$FLINK_HOME/conf/flink-conf.yaml` file to set the configuration property defining the number of task slots to 10 as follows, `taskmanager.numberOfTaskSlots: 10`. Set this value higher than the number of cores on your machine(I have 8 cores on my machine) Go to the Job Manager UI `http://localhost:8081` from your browser. What is the number of available task slots? What is the relationship between a JVM instance, Task Manager and a Task Slot? 
### Answer
The JobManager UI shows there is 1 task manager and 10 slots. Each Task Manager in Flink is a separate JVM. Each Task Slot is a thread within the JVM. It is ideal not to have more task slots than there are CPU cores. We are using 8 core machines. Yet we can start a local Flink cluster with 10 slots. This will increase resource contention for the CPU cores but it is allowed.

## Exercise 2.2
Using the Flink configuration as setup in the Exercise 2.1, start Flink locally using the command `$FLINK_HOME/bin/start-local`. Execute the following command from `$FLINK_HOME` folder - `bin\flink run examples\batch\WordCount.jar`. What is the parallelism of each task? (Hint: Go to the Job Manager UI `http://localhost:8081`)
### Answer
By default the local cluster in Flink uses a default parallelism of 1. Each operator will use this default parallelism

## Exercise 2.3
Next update the `$FLINK_HOME/conf/flink-conf.yaml` to set the configuration property `parallelism.default: 5`. Once again execute the word count application by executing the following command from `$FLINK_HOME` folder - `bin\flink run examples\batch\WordCount.jar`. What is the parallelism of each task? (Hint: Go to the Job Manager UI `http://localhost:8081`)
### Answer
Now the WordCount job uses a default parallelism of 5. Each operator will attempt to have 5 instances running in 5 separate slots. 

## Exercise 2.4
Now execute the word count application by executing the following command from `$FLINK_HOME` folder - `bin\flink run -p 11 examples\batch\WordCount.jar.` Note that the parallelism (`-p 11`) is higher than the total number of task slots configured. What is the result of running this job?
### Answer
This job will fail because there are not enough slots per operator. Note that we could have successfully executed this job using the setting `-p 10`. Even though we have the map operator taking 10 slots and the reduce operator taking 10 slots, Flink does not fail by saying there are not enough slots available when we set `-p 10`. Although each operator instance (10 each) is assigned a slot these slots are shared between the map and the reduce operators. Thus in this scenario there is resource contention but no failure. But if we run the job with parallelism of 11 (`-p 11`) Flink expect there to be at least 11 task slots in the Flink cluster to work. 

To summarize there must be atleast as many task slots in a Flink cluster as the parallelism for the job. Note that we do not say available task slots. Simply that that the Flink cluster must have as many task slots. As long as the parallelism of the job is lower than the total number of task slots in the cluster Flink will manage these jobs although they will contend for resources. But you cannot start a job with a parallelism higher than the total number of task slots available in the cluster.

## Exercise 2.5
Now execute the word count application twice in two separate windows by executing the following command from `$FLINK_HOME` folder - `bin\flink run -p 10 examples\batch\WordCount.jar`. Note that the parallelism (`-p 10`) of each job is equal to the total number of available task slots. What is the result of running these jobs simultaneously? What does this tell you about the relationship between multiple jobs and the total number of available task slots in Flink?
### Answer
For reasons discussed in the answer to exercise 2.4 this will work. 

