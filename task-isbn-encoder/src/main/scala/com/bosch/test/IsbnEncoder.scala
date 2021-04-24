package com.bosch.test.IsbnEncoder

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.explode
import org.apache.spark.sql.implicit_


object IsbnEncoder {
  case class Isbn(Name:String, Year:Int, ISBN:String)

  def main(args :Array[String]): Unit = {
    explodeIsbn
    ISBNEncode
}

  def explodeIsbn():DataFrame = {

    val conf = new SparkConf().setAppName("IsbnEncoderTest").setMaster("local[*]")
    val sc = new SparkContext(conf)

     val spark =
      SparkSession
        .builder()
        .appName("IsbnEncoderTest")
        .master("local")
        .getOrCreate()

      println("ISBN Table")

      /** Input the table with ISBN number as an example**/

      val r1 = Isbn("Learning Spark: Lightning-Fast Big Data Analysis", 2015, "ISBN: 978-1449358624")

      val records = Seq(r1)

      /** To create a DataFrame from a sequence stored in r1**/
      val df = spark.createDataFrame(records)
      val df_r = df.select(df("Name"),df("Year"),explode(array(df("ISBN"))).alias("ISBN"))

      /** Creates a temporary table with columns Name,Year,ISBN to query on given data**/

      val isbn_df = df_r.toDF()
      isbn_df.createOrReplaceTempView("isbn_table")

      /** select ISBN column to replace special characters **/

      val df_temp = spark.sqlContext.sql("Select ISBN from isbn_table").collect.mkString
      val mergeisbnno = df_temp.replaceAll("[-|]","") // ISBN: 978-14 4935 862 4


      /** Split the Part of ISBN Number **/

      val ean_no = "ISBN-EAN:" + mergeisbnno.slice(6,9)
      val group = "ISBN-GROUP:" + mergeisbnno.slice(9,11)
      val publisher = "ISBN-PUBLISHER:" + mergeisbnno.slice(12,15)
      val title = "ISBN-TITLE:" + mergeisbnno.slice(15,18)

      println("OUTPUT TABLE")

      /** Here we pass the 4 elements of ISBN code to sequence **/

      val rdd1 = sc.parallelize(Seq(("Learning Spark: Lightning-Fast Big Data Analysis", 2015, Seq("ISBN: 978-1449358624",ean_no,group,publisher,title))))
      val fdsg = rdd1.toDF("name", "year","ISBN")

      /** Using the explode function separate each element of ISBN values **/
      val exploded = fdsg.withColumn("ISBN", explode($"ISBN"))
      val finalTable = exploded.toDF()
      finalTable.show()

      finalTable
    }

  def ISBNEncode(): Unit = {

    val conf = new SparkConf().setAppName("IsbnEncoderTest").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName("IsbnEncoderTest").master("local").getOrCreate()

    val r1 = Isbn("Learning Spark: Lightning-Fast Big Data Analysis", 2015, "ISBN: 978-1449358624")

    val records = Seq(r1)
    val df = spark.createDataFrame(records)
    val df_r = df.select(df("Name"), df("Year"), explode(array(df("ISBN"))).alias("ISBN"))

    /** select ISBN column to replace special characters **/

    val isbn_df = df_r.toDF()
    isbn_df.createOrReplaceTempView("isbn_table")

    /** select ISBN column to replace special characters **/

    val df_temp = spark.sqlContext.sql("Select ISBN from isbn_table").collect.mkString
    val mergeisbnno = df_temp.replaceAll("[-|]", "")
    val isbnnumber = mergeisbnno.slice(6,19)

    val numberList = isbnnumber.toList.map(lambda:x => x.toString.toInt)

    println("Check whether given ISBN Input string is a valid 13 digit Number or not?")
    val sum = (0 until numberList.size - 1).map{ i => if (i % 2 == 0) numberList(i) * 1 else numberList(i) * 3}.foldLeft(0){(r, x) =>  r+x}
    if (sum % 10 == 0) println("The Given ISBN is a Valid ISBN") else  println("The given ISBN is an Invalid ISBN")

  }
}
