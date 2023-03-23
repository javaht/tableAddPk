import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import java.util

object oneData {
  def main(args: Array[String]): Unit = {


    val session = SparkSession.builder.appName("").master("local[*]").getOrCreate

    val df: DataFrame = orginTable(session)


    
    df.collect().map(
      data=>{
        val tablename: String = data.getString(0)
        val readDf: DataFrame = readTable(session, tablename)
        readDf.printSchema()
        writeTable(readDf,"bak_"+tablename)
      }
    )


    session.close()
  }




  def writeTable(dataFrame: DataFrame,tablename: String): Unit = {
    dataFrame.write.mode(SaveMode.Overwrite).format("jdbc")
      .option("url", "jdbc:postgresql://127.0.0.1:5432/cdctest?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8&useSSL=false")
      .option("driver", "org.postgresql.Driver")
      .option("user", "postgres")
      .option("password", "123456")
      .option("dbtable",tablename).saveAsTable(tablename)
  }

  def readTable(session: SparkSession,tablename: String): DataFrame = {
    session.read.format("jdbc")
      .option("url", "jdbc:postgresql://127.0.0.1:5432/cdctest?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8&useSSL=false")
      .option("driver", "org.postgresql.Driver")
      .option("user", "postgres")
      .option("password", "123456")
      .option("dbtable", "(select distinct * from  "+tablename+") as tablename ").load
  }


  def orginTable(session: SparkSession): DataFrame = {
    session.read.format("jdbc")
      .option("url", "jdbc:postgresql://127.0.0.1:5432/cdctest?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8&useSSL=false")
      .option("driver", "org.postgresql.Driver")
      .option("user", "postgres")
      .option("password", "123456")
      .option("dbtable", " (select tablename from pg_tables where schemaname='public') as data ").load
  }




}
