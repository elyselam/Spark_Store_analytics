package finalProj

import finalProj.DataFrameProj.file
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object RddProj extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("AppName")
      .config("spark.master", "local")
      .getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  import spark.implicits._


  case class MasterDate(CalendarID: String, CalendarDate: String)

  case class Product(ProductId: String, ProductName: String)

  case class Sales(CalendarDate: String, ProductId: String, StoreId: String)

  case class Store(StoreId: String, StoreName: String)

  val file = "/Users/elyselam/Downloads/finalProject/src/main/scala/finalProj"

  val salesRdd = sc.textFile(s"file:///$file/data/Sales.csv")
    .map(_.split(",")).map(c => Sales(c(0), c(1), c(2)))

  val productRdd = sc.textFile(s"file:///$file/data/Product.csv")
    .map(_.split(",")).map(c => Product(c(0), c(1)))

  val storeRdd = sc.textFile(s"file:///$file/data/Store.csv")
    .map(_.split(",")).map(c => Store(c(0), c(1)))

  //convert salesRdd into a pairrdd of productId followed by all of sales records
  val salesPair: RDD[(String, Sales)] = salesRdd.map(r => (r.ProductId, r))
  //same
  val productPair: RDD[(String, String)] = productRdd.map(r => (r.ProductId, r.ProductName))

  //get everything from the left and only productName from the right, don't need the rest of the records
  val salesAndProductPair: RDD[(String, (Sales, String))] = salesPair.join(productPair)


  //convert to a new case class.
  case class SalesAndProduct(CalendarDate: String, StoreId: String, ProductName: String)

  //pairRdd
  val salesAndProduct: RDD[SalesAndProduct] =
    salesAndProductPair
      .values
      .map {
        case (sale, productName) =>
          SalesAndProduct(sale.CalendarDate, sale.StoreId, productName)
      }


  case class SaleRecord(CalendarDate: String, StoreName: String, ProductName: String)

  val rdd =
    salesAndProduct.map(r => (r.StoreId, r))
      .join(
        storeRdd.map(r => (r.StoreId, r.StoreName))
      )
      .values
      .map {
        case (record, storeName) =>
          SaleRecord(record.CalendarDate, storeName, record.ProductName)
      }

  rdd.take(10).foreach(println)
  //SaleRecord(2018-10-02,Birmingham,Melon)


  //***************** 1)  top 10 product sold in August **********************
  val q1 =
    rdd.filter(r => r.CalendarDate.matches("\\d{4}-08-\\d{2}"))
      .map(r => (r.ProductName, 1))
        .reduceByKey(_+_)

      //(ProductName, Iterable[SalesRecord]) so we want to count how many products were sold.
      .map {
        case (productName, salesCount) =>
          (productName, salesCount) // returns tuple which we can sort by sales.size
      }
      .sortBy(_._2, false)
      .zipWithIndex() // (record, position)
      .filter(_._2 < 10) //filter by 2nd element, which are the top 10
      .map {
        case ((name, count), position) =>
          (position + 1) + ". " + name + ": " + count
      }
  q1.saveAsTextFile(s"file:///$file/results/RddQuestion1")
  //  q1.foreach(println)


  //********************** 2) number of product sold in any stores **********************

  val q2 =
  //    rdd.groupBy(r => r.StoreName)
  //  .map {
  //    case (storeName, productName) =>
  //      storeName + " "+ productName.size
  //  }
    rdd.map(r => (r.StoreName, 1))
      .reduceByKey(_ + _)
      .map {
        case (storeName, count) =>
          storeName + " " + count
      }
      .saveAsTextFile(s"file:///$file/results/RddQuestion2")


  // ********************** 3) top 10 store that sold most products  **********************


  val q3 = rdd.groupBy(r => r.StoreName) //just want to use groupBy to compare the method with reduceByKey. groupBy is not optimal
    .map {
      case (storeName, productsSold) =>
        (storeName, productsSold.size)
    }
    .sortBy(_._2, false)
    .zipWithIndex() // (record, position)
    .filter(_._2 < 10) //filter by 2nd element, which are the top 10
    .map {
      case ((storeName, productSold), position) =>
        (position + 1) + ". " + storeName + ": " + productSold
    }
  q3.saveAsTextFile(s"file:///$file/results/RddQuestion3")


}
