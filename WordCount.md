WordCount Test
=======

This is a guide to how to do a basic WordCount test.

The WordCount program is a MapReduce job that takes a bunch of text documents and counts the number of instances of each word in the documents.

To produce a test input, I used the `yes` unix command. `yes [word]` can take an argument and repeat it indefinitely. I made a document consisting of a mix of words by piping it as follows:

```
$ yes mike | head -1234000 >> ~/word_count_test.txt && yes nick | head -2345000 >> ~/word_count_test.txt && yes kahlil | head -3456000 >> ~/word_count_test.txt && yes eden | head -4567000 >> ~/word_count_test.txt
```
The above command makes a text file with all our names in the home directory (with different numbers for each so we can verify the count is correct).

After its made, copy it to HDFS with:

```
hadoop fs -copyFromLocal ~/word_count_test.txt input/word_count_test/
```

Check if this was successful with `hadoop fs -ls input`.

This should show you the file `word_count_test.txt` in the directory.

Now that this file is on HDFS you can execute a MapReduce job on it as follows:

```
$HADOOP_HOME/bin/yarn jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.3.0-cdh5.0.2.jar wordcount -Dmapreduce.job.queuename=hadoop01 input/word_count_test.txt word_count_test_output
```

The above command runs the `wordcount` MapReduce example on the text file we made, and exports the result to the word_count_test_output folder on the HDFS. This folder must not already exist!

The following is the output I got on the MapReduce job:

```
14/09/21 00:04:43 INFO input.FileInputFormat: Total input paths to process : 1
14/09/21 00:04:43 INFO mapreduce.JobSubmitter: number of splits:1
14/09/21 00:04:44 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1409756600294_0025
14/09/21 00:04:44 INFO impl.YarnClientImpl: Submitted application application_1409756600294_0025
14/09/21 00:04:44 INFO mapreduce.Job: The url to track the job: http://deerstalker.cs.brandeis.edu:8088/proxy/application_1409756600294_0025/
14/09/21 00:04:44 INFO mapreduce.Job: Running job: job_1409756600294_0025
14/09/21 00:04:51 INFO mapreduce.Job: Job job_1409756600294_0025 running in uber mode : false
14/09/21 00:04:51 INFO mapreduce.Job:  map 0% reduce 0%
14/09/21 00:05:02 INFO mapreduce.Job:  map 21% reduce 0%
14/09/21 00:05:05 INFO mapreduce.Job:  map 41% reduce 0%
14/09/21 00:05:08 INFO mapreduce.Job:  map 55% reduce 0%
14/09/21 00:05:11 INFO mapreduce.Job:  map 66% reduce 0%
14/09/21 00:05:12 INFO mapreduce.Job:  map 100% reduce 0%
14/09/21 00:05:22 INFO mapreduce.Job:  map 100% reduce 100%
14/09/21 00:05:22 INFO mapreduce.Job: Job job_1409756600294_0025 completed successfully
14/09/21 00:05:22 INFO mapreduce.Job: Counters: 49
	File System Counters
		FILE: Number of bytes read=157
		FILE: Number of bytes written=178104
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=64922145
		HDFS: Number of bytes written=54
		HDFS: Number of read operations=6
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=2
	Job Counters 
		Launched map tasks=1
		Launched reduce tasks=1
		Rack-local map tasks=1
		Total time spent by all maps in occupied slots (ms)=74660
		Total time spent by all reduces in occupied slots (ms)=31824
		Total time spent by all map tasks (ms)=18665
		Total time spent by all reduce tasks (ms)=7956
		Total vcore-seconds taken by all map tasks=18665
		Total vcore-seconds taken by all reduce tasks=7956
		Total megabyte-seconds taken by all map tasks=76451840
		Total megabyte-seconds taken by all reduce tasks=32587776
	Map-Reduce Framework
		Map input records=11602000
		Map output records=11602000
		Map output bytes=111330000
		Map output materialized bytes=52
		Input split bytes=145
		Combine input records=11602007
		Combine output records=11
		Reduce input groups=4
		Reduce shuffle bytes=52
		Reduce input records=4
		Reduce output records=4
		Spilled Records=15
		Shuffled Maps =1
		Failed Shuffles=0
		Merged Map outputs=1
		GC time elapsed (ms)=3007
		CPU time spent (ms)=42900
		Physical memory (bytes) snapshot=522223616
		Virtual memory (bytes) snapshot=17220075520
		Total committed heap usage (bytes)=342884352
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=64922000
	File Output Format Counters 
		Bytes Written=54
```

Now this job is complete, check the output on the HDFS by executing `hadoop fs -ls word_count_test_output` which should show the output produced.

Once the file report is on the HDFS, we can copy it locally by executing `hadoop fs -copyToLocal word_count_test_output ~/`.

The folder congaing the report should be in your home directory (part-r-00000). It is the following:

```
eden	4567000
kahlil	3456000
mike	1234000
nick	2345000
```

That's a MapReduce job!
