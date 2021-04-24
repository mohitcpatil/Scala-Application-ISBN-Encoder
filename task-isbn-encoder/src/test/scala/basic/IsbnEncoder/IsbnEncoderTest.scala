package basic

import com.bosch.test.IsbnEncoder._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.implicits._
import org.junit.{After, Before, Test}
import org.apache.spark.sql.functions.explode


@Test
class IsbnEncoderTest {

  private var spark: SparkSession = null

  /** Create Spark context before tests **/
  @Before
  def setUp(): Unit =
  {
    val spark = {
      SparkSession
        .builder()
        .appName("IsbnEncoderTest")
        .master("local")
        .getOrCreate()
      }
  }

  @Test
  def TestValidIsbn(): Unit = {
    val r1 = Isbn("Learning Spark: Lightning-Fast Big Data Analysis", 2015, "ISBN: 978-1449358624")

    val records = Seq(r1)
    val df = spark.createDataFrame(records)
    val df_r = df.select(df("Name"),df("Year"),explode(array(df("ISBN"))).alias("ISBN"))

    /** Creates a temporary table with columns Name,Year,ISBN **/

    val isbn_df = df_r.toDF()
    isbn_df.createOrReplaceTempView("isbn_table")

    /** select ISBN column to replace special characters **/

    val df_temp = spark.sqlContext.sql("Select ISBN from isbn_table").collect.mkString
    val mergeisbnno = df_temp.replaceAll("[-|]","") // ISBN: 978-14 4935 862 4

    val ean_no = "ISBN-EAN:" + mergeisbnno.slice(6,9)
    val group = "ISBN-GROUP:" + mergeisbnno.slice(9,11)
    val publisher = "ISBN-PUBLISHER:" + mergeisbnno.slice(12,15)
    val title = "ISBN-TITLE:" + mergeisbnno.slice(15,18)

    val r2 = Isbn("Learning Spark: Lightning-Fast Big Data Analysis", 2015, ean_no)
    val r3 = Isbn("Learning Spark: Lightning-Fast Big Data Analysis", 2015, group)
    val r4 = Isbn("Learning Spark: Lightning-Fast Big Data Analysis", 2015, publisher)
    val r5 = Isbn("Learning Spark: Lightning-Fast Big Data Analysis", 2015, title)

    /** Merge df and df2 to create final ISBN dataframe **/

    val records2 = seq(r2,r3,r4,r5)
    val df2 = spark.createDataFrame.(records2)
    val ISBNFinaldata = seq(df, df2)

    ISBNFinaldata.reduce(_union_).show()
    val reducedISBN = ISBNFinaldata.reduce(_union_).toDF()

    assert(5 == reducedISBN.count())
    assert(5 == reducedISBN.filter(col("name") === "Learning Spark: Lightning-Fast Big Data Analysis").count())
    assert(5 == reducedISBN.filter(col("year") === 2015).count())
    assert(1 == reducedISBN.filter(col("isbn") === "ISBN: 978-1449358624").count())
    assert(1 == reducedISBN.filter(col("isbn") === "ISBN-EAN: 978").count())
    assert(1 == reducedISBN.filter(col("isbn") === "ISBN-GROUP: 14").count())
    assert(1 == reducedISBN.filter(col("isbn") === "ISBN-PUBLISHER: 4935").count())
    assert(1 == reducedISBN.filter(col("isbn") === "ISBN-TITLE: 862").count())
  }

  @Test
  def TestInvalidIsbn(): Unit = {
    val r1 = Isbn("My book", 2014, "7569HJ4-8U")

    val records = Seq(r1)
    val df = spark.createDataFrame(records)
    val df_r = df.select(df("Name"),df("Year"),explode(array(df("ISBN"))).alias("ISBN"))

    assert(1 == df_r.count())
    assert(df_r.first().get(2) == "7569HJ4-8U")
  }

  @Test
  def TestEmptyIsbn(): Unit = {
    val r1 = Isbn("My book", 2014, "")

    val records = Seq(r1)
    val df = spark.createDataFrame(records)

    val df_r = df.select(df("Name"),df("Year"),explode(array(df("ISBN"))).alias("ISBN"))

    assert(1 == df_r.count())
    assert(df_r.first().get(2) == "")
  }

  @Test
  def TestMixed(): Unit = {
    val r1 = Isbn("Learning Spark: Lightning-Fast Big Data Analysis", 2015, "ISBN: 978-1449358624")
    val r2 = Isbn("Scala Tutorial", 2016, "87664-U6")

    val records = Seq(r1, r2)
    val df = spark.createDataFrame(records)

    val df_r = df.select(df("Name"),df("Year"),explode(array(df("ISBN"))).alias("ISBN"))

    assert(6 == df_r.count())
    assert(5 == df_r.filter(col("name") === "Learning Spark: Lightning-Fast Big Data Analysis").count())
    assert(1 == df_r.filter(col("name") === "Scala Tutorial").count())
  }

  @Test
  def ISBNEncode(): Unit = {

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

    val isbnnumber = mergeisbnno.slice(6,9)


    val numberList = isbnnumber.toList.map(x => x.toString.toInt)

    println("Check whether given ISBN Input string is a valid 13 digit Number or not?")
    val sum = (0 until numberList.size - 1).map{ i => if (i % 2 == 0) numberList(i) * 1 else numberList(i) * 3}.foldLeft(0){(r, x) =>  r+x}
    if (sum % 10 == 0) println("The Given ISBN is a Valid ISBN") else  println("The given ISBN is an Invalid ISBN")

    assert(sum % 10 == 0)
  }


   /** Stop Spark context after tests **/

  @After
  def tearDown(): Unit = {
    spark.stop()
    spark = null
  }

  case class Isbn(name: String, year: Int, isbn: String)
}
