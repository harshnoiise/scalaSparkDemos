import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row


object logsTest {

  // Define the Transaction Log object Schema we want to use to only retrieve only the
  // fields we are interested in out of the data.
  case class Transaction(transaction_id: String, transaction_amount: Double, date: String, product_brand: String, successful_transaction: String) extends Serializable

  def main(args: Array[String]) {

    val spark: SparkSession = SparkSession.builder().appName("mockLogs").config("spark.master", "local").getOrCreate();
    val mdf = spark.read.option("multiline", "true").json("/Users/keatonkhonsari/mock_data1.json")

    // Type cast conversion for transaction_amount from string to double.
    val toDouble = udf[Double, String]( _.toDouble)
    val mdf2 = mdf.withColumn("transaction_amount", toDouble(mdf("Transaction_Amount")))
    mdf2.first
    mdf2.createOrReplaceTempView("transactions")

    // Use spark SQL to select the fields we're interested in from the dataframe
    // by using the Transaction class.
    import spark.implicits._
    val ds: Dataset[Transaction] = spark.sql(sqlText = "select Transaction_Id as transaction_id, transaction_amount, Date as date, Product_Brand as product_brand, Successful_Transaction as successful_transaction from transactions ").as[Transaction]
    ds.cache
    ds.first
    ds.createOrReplaceTempView("transactions")
    ds.show
    ds.filter($"transaction_amount" > 1000).show()
    ds.filter(ds("successful_transaction")==="true").show()



  }
}
