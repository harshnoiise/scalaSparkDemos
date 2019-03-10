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

  case class PaymentwId(_id: String, physician_id: String, date_payment: String, payer: String, amount: Double, physician_specialty: String,
                        nature_of_payment: String) extends Serializable

  def createPaymentwId(p: Payment): PaymentwId = {
    val id = p.physician_id + '_' + p.date_payment + '_' + p.record_id
    PaymentwId(id, p.physician_id, p.date_payment, p.payer, p.amount, p.physician_specialty, p.nature_of_payment)
  }

  def main(args: Array[String]) {

    val spark: SparkSession = SparkSession.builder().appName("payment").config("spark.master", "local").getOrCreate();

    val toDouble = udf[Double, String]( _.toDouble)
    val df = spark.read.option("header", "true").csv("/Users/keatonkhonsari/health.csv")
    val df2 = df.withColumn("amount", toDouble(df("Total_Amount_of_Payment_USDollars")))
    df2.first
    df2.createOrReplaceTempView("payments")
    // df.show()

    import spark.implicits._
    val ds: Dataset[Payment] = spark.sql("select Physician_Profile_ID as physician_id, Date_of_Payment as date_payment, Record_ID as record_id, Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name as payer,  amount, Physician_Specialty, Nature_of_Payment_or_Transfer_of_Value as Nature_of_payment from payments ").as[Payment]
    ds.cache
    ds.first
    ds.createOrReplaceTempView("payments")
    ds.show
    ds.filter($"amount" > 1000).show()
    ds.groupBy("Nature_of_payment").count().orderBy(desc("count")).show()



  }

}

