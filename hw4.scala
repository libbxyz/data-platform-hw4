import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object SparkAssignment4 {
  def main(args: Array[String]): Unit = {
    // Запустить сессию Apache Spark под управлением YARN, развернутого кластера в предыдущих заданиях.
    val spark = SparkSession.builder()
      .appName("Spark Assignment 4")
      .master("yarn")
      .getOrCreate()

    import spark.implicits._

    // Подключиться к кластеру HDFS, развернутому ранее.
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)

    val inputPath = new Path("/input/input.txt") // Replace with actual HDFS path
    if (!hdfs.exists(inputPath)) {
      println(s"Input path does not exist: $inputPath")
      sys.exit(1)
    } else {
      println(s"Input path exists: $inputPath")
    }

    val outputPath = new Path("/output/wordcount_partitioned")
    if (hdfs.exists(outputPath)) {
      println(s"Output path already exists, deleting: $outputPath")
      hdfs.delete(outputPath, true)
    }

    // Используя созданную ранее сессию Spark прочитать данные, которые были предварительно загружены на HDFS.
    val data = spark.read.textFile(inputPath.toString)

    println("Data from HDFS:")
    data.show(false)

    //  Провести несколько трансформаций данных
    //  +Для повышения оценки: применить 5 трансформаций разных видов, сохранить данные как партиционированную таблицу.

    // Count the number of rows
    val rowCount = data.count()
    println(s"Total rows: $rowCount")

    // Extract words and convert to DataFrame
    val words = data.flatMap(_.split(" ")).toDF("word")

    // Group by word and count occurrences
    val wordCounts = words.groupBy("word").count()

    // Sort by count in descending order
    val sortedWordCounts = wordCounts.orderBy($"count".desc)

    // Add a column for word length
    val withLength = sortedWordCounts.withColumn("length", $"word".length)

    println("Transformed Data:")
    withLength.show(false)

    // Сохранить данные как таблицу
    withLength.write.partitionBy("length").mode("overwrite").parquet(outputPath.toString)

    println(s"Partitioned output saved to: $outputPath")

    spark.stop()
  }
}
