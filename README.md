# scalaLogs
  This demonstration uses a generated json file of mock data with a handful of different fields of data. Using Scala and Spark I created a unique Scala object schema which is used for only the fields of data we are interested in to create a DataFrame.
  Then using Spark SQL, I select those fields of interest from the DataFrame and convert it to a Dataset for the Scala object case class. 
  From there, the user can do any filtering to that DataFrame for any queries it may be that they are interested in. 
   
  Using the lines:
  
  ```scala
      ds.filter($"transaction_amount" > 1000).show()
      ds.filter(ds("successful_transaction")==="true").show()
  ```
  
  ![alt text](https://github.com/harshnoiise/scalaSparkDemos/blob/master/scalaLogsTable.png)

  the resulting table shows transactions that cost more than $1000. and that were true for being successfully completed.
  
# healthCareScala
  This demonstration is very similar to the scalaLogs one, except instead of starting with a json file of alot of data, I start with a massive CSV file from Open Payments, has been a federal program that collects information about payments drug and device companies make to physicians and teaching hospitals for things like travel, research, gifts, speaking fees, and meals.
   
  Using the lines:
   
  ```scala
  ds.filter($"amount" > 9000).show()
  ds.groupBy("Nature_of_payment").count().orderBy(desc("count")).show()
  ```
  ![alt text](https://github.com/harshnoiise/scalaSparkDemos/blob/master/healthCareScalaTable.png)

  the resulting table shows payments that were made that were above $9000, grouped by the nature of the payments.
     
# scalaCount
  A simple word count implementation using The Complete Works of William Shakespeare as input.
  
  Here we are essentially tokenizing each word in a line to an array of strings in the file by spliting on the space in   between each word using flatMap, and then using reduceByKey, resulting in a tuple with a word and total word count.
 ```scala
 val counts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
 ```
 ```
 Total number of words: 76389
 ```
