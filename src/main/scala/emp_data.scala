import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, length, regexp_replace, substring}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object emp_data {
  def main(args: Array[String]): Unit = {

    readdata()
  }

  // Function to read file
  def readdata(): Unit = {
    System.setProperty("hadoop.home.dir","C:\\winutil\\")
    import org.apache.spark.sql.functions.regexp_replace
    val sc = SparkSession.builder().appName("EmpData").master("local").getOrCreate()
    val spark = sc.sparkContext
    spark.hadoopConfiguration.set("fs.cos.impl", "com.ibm.stocator.fs.ObjectStoreFileSystem")
    spark.hadoopConfiguration.set("fs.stocator.scheme.list", "cos")
    spark.hadoopConfiguration.set("fs.stocator.cos.impl", "com.ibm.stocator.fs.cos.COSAPIClient")
    spark.hadoopConfiguration.set("fs.stocator.cos.scheme", "cos")
    spark.hadoopConfiguration.set("fs.cos.myCos.access.key", "699c208709e14c73bbc238ad52bda157")
    spark.hadoopConfiguration.set("fs.cos.myCos.endpoint", "s3.us-east.cloud-object-storage.appdomain.cloud")
    spark.hadoopConfiguration.set("fs.cos.myCos.secret.key", "b10ccc0ba00102fc6c8e42ae694e19068a7862be104517d1")
    spark.hadoopConfiguration.set("fs.cos.service.v2.signer.type", "false")
    val schema = StructType(Array(StructField("Name", StringType, true), StructField("Gender", StringType, true), StructField("Department", StringType, true), StructField("Salary", StringType, true), StructField("Loc", StringType, true), StructField("Rating", StringType, true)))
    val df = sc.read.schema(schema).csv("cos://emp-data-pallavi.myCos/emp-data.csv")
    val df_final = df.filter(df("Name") =!= "Name")

    //Creating table on DB2 database
    df_final.write.format("jdbc").options(Map(
      "url" -> "jdbc:db2://0c77d6f2-5da9-48a9-81f8-86b520b87518.bs2io90l08kqb1od8lcg.databases.appdomain.cloud:31198/bludb:user=hsl03264;password=TGYSbb38kc5JJEaV;sslConnection=true;",
      "driver" -> "com.ibm.db2.jcc.DB2Driver",
      "dbtable" -> "EMPLOYEE")).mode("overwrite").saveAsTable("Employee")

    //Creating dataframe to read data from DB2 Employee table
    val df_read=sc.read.format("jdbc").options(Map(
      "url" -> "jdbc:db2://0c77d6f2-5da9-48a9-81f8-86b520b87518.bs2io90l08kqb1od8lcg.databases.appdomain.cloud:31198/bludb:user=hsl03264;password=TGYSbb38kc5JJEaV;sslConnection=true;",
      "driver" -> "com.ibm.db2.jcc.DB2Driver",
      "dbtable" -> "EMPLOYEE")).load()

    //Cleaning the salary data
    val df_read1 = df_read.withColumn("sal",expr("substring(Salary, 2, length(Salary))"))
    val read2=df_read1.withColumn("sal_inter",regexp_replace(df_read1("sal"),",",""))
    val df_read2 = read2.withColumn("sal_final",(read2("sal_inter").cast(IntegerType)))

    //Registering the dataframe as temp view
    df_read2.createOrReplaceTempView("Employee")

    //Calaculating the data for Question 5 a,b,c
     val df_ratio= sc.sql("select sum(case when Gender='Male' then 1 else 0 End)/sum(case when Gender='Female' then 1 else 0 End) as ratio,Department from Employee group by Department")
     val avg_salary=sc.sql("select avg(sal_final) as avg_sal,Department from Employee group by Department ")
     val sal_gap=sc.sql("Select max(case when Gender='Male' then sal_final END)-max(case when Gender='Female' then sal_final END) as sal_gap,Department from Employee group by Department")

    //Display data for Question 5 a,b,c
    df_ratio.show(false)
    avg_salary.show(false)
    sal_gap.show(false)

    //Writing the calculated data back to COS
    df_ratio.write.format("parquet").mode("overwrite").save("cos://emp-data-pallavi.myCos/ratio.parquet")
    avg_salary.write.format("parquet").mode("overwrite").save("cos://emp-data-pallavi.myCos/avg_sal.parquet")
    sal_gap.write.format("parquet").mode("overwrite").save("cos://emp-data-pallavi.myCos/sal_gap.parquet")
  }

}