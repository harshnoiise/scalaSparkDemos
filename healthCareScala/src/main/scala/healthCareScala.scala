import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object healthCareScala {

  case class Payment(physician_id: String, date_payment: String, record_id: String, payer: String, amount: Double, physician_specialty: String, nature_of_payment: String) extends Serializable

  def main(args: Array[String]) {
    
    // SparkSession read method loads csv file and returns the result as a DataFrame.
    val spark: SparkSession = SparkSession.builder().appName("payment").config("spark.master", "local").getOrCreate();
   
    // User-defined method used to convert the "amount" column from string to double. 
    val toDouble = udf[Double, String]( _.toDouble)
    val df = spark.read.option("header", "true").csv("/Users/keatonkhonsari/health.csv")
    val df2 = df.withColumn("amount", toDouble(df("Total_Amount_of_Payment_USDollars")))
    df2.first
    
    // Local temporary view is created for better ease of use with sql.
    df2.createOrReplaceTempView("payments")

    import spark.implicits._
    
    // Spark SQL is used to select the fields we want from the DataFrame and convert 
    // this to a Dataset[Payment] by providing the Payment class. Then the payment view is replaced.
    val ds: Dataset[Payment] = spark.sql("select Physician_Profile_ID as physician_id, Date_of_Payment as date_payment, Record_ID as record_id, Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name as payer,  amount, Physician_Specialty, Nature_of_Payment_or_Transfer_of_Value as Nature_of_payment from payments ").as[Payment]
    ds.first
    ds.createOrReplaceTempView("payments")
    ds.filter($"amount" > 1000).show()
    ds.groupBy("Nature_of_payment").count().orderBy(desc("count")).show()

  }
}

