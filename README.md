# scalaLogs
  This demonstration uses a generated json file of mock data with a handful of different fields of data. Using Scala and Spark I created a unique Scala object schema which is used for only the fields of data we are interested in to create a DataFrame.
  Then using Spark SQL, I select those fields of interest from the DataFrame and convert it to a Dataset for the Scala object case class. 
  From there, the user can do any filtering to that DataFrame for any queries it may be that they are interested in. 
  
# healthCareScala
  This demonstration is very similar to the scalaLogs one, except instead of starting with a json file of alot of data, I start with a massive CSV file from Open Payments, has been a federal program that collects information about payments drug and device companies make to physicians and teaching hospitals for things like travel, research, gifts, speaking fees, and meals.

