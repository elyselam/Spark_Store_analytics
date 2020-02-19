package finalProj

import java.util.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object DataFrameProj extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("AppName")
      .config("spark.master", "local")
      .getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("WARN")
  import spark.implicits._


  val masterDateSchema = StructType(Array(StructField("CalendarID", StringType), StructField("CalendarDate", StringType)))
  val productSchema = StructType(Array(StructField("ProductId", StringType), StructField("ProductName", StringType)))
  val salesSchema = StructType(Array(StructField("CalendarDate", StringType), StructField("ProductId", StringType), StructField("StoreId", StringType)))
  val storeSchema = StructType(Array(StructField("StoreId", StringType), StructField("StoreName", StringType)))

  val file = "/Users/elyselam/Downloads/finalProject/src/main/scala/finalProj"

  //"header", true: data has header so first line has to be skipped
  val sales = spark.read.schema(salesSchema).option("header", true).csv(s"file:///$file/data/Sales.csv")
  val product = spark.read.schema(productSchema).option("header", true).csv(s"file:///$file/data/Product.csv")
  val masterDate = spark.read.schema(masterDateSchema).option("header", true).csv(s"file:///$file/data/Masterdate.csv")
  val store = spark.read.schema(storeSchema).option("header", true).csv(s"file:///$file/data/Store.csv")

  //broadcast masterDate because it's very small
  val joinDate = sales.join(broadcast(masterDate), usingColumn = "CalendarDate")
  val joinproductID = joinDate.join(product, usingColumn = "ProductId")
  //now join on storeid
  val df = joinproductID.join(store, usingColumn ="StoreId").cache()
  println(sales.count())
  println(df.count()) //1001 same number so everything joined ok

  df.explain()
  //to see  BroadcastHashJoin and how it's more efficient on masterDate

  val SalesFact = df.select("CalendarDate", "ProductName", "StoreName")
  SalesFact.show()


  //***************** 1)  top 10 product sold in August **********************
  SalesFact.filter($"CalendarDate" rlike "\\d{4}-08-\\d{2}")
    //groupby ProductName and count the number of records for each product name
    //same as reduceByKey in rdd
    .groupBy($"ProductName").agg(count("*") as "totalproducts")
    .sort($"totalproducts".desc)
    .limit(10).repartition(1) //to prevent default of 200 partitions
    .write.mode("overwrite").parquet(s"file:///$file//results/DFquestion1")


  //***************** 2) number of product sold in any stores *********************
//count # of records after grouping by storeName
  SalesFact.groupBy($"StoreName").agg(count($"*") as "totalProductsPerStore")
    .repartition(1)
    .write.mode("overwrite").parquet(s"file:///$file/results/DFquestion2")

  // ****************** 3) top 10 store that sold most products  ****************
  SalesFact.groupBy($"StoreName")
    .agg(count($"*") as "totalProductsSold")
    .sort($"totalProductsSold".desc).limit(10)
    .repartition(1)
    .write.mode("overwrite").parquet(s"file:///$file/results/DFquestion3")

}
