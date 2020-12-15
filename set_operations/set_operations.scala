// Databricks notebook source
//    https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/8363166053443355/3144059327038241/3462821094046267/latest.html

// COMMAND ----------

val august = sc.textFile("FileStore/tables/nasa_august.tsv")
val july = sc.textFile("FileStore/tables/nasa_july.tsv")

// COMMAND ----------

val julyhost = july.map( x => x.split("\t")(0)).filter(row=> row != "host")
val augusthost = august.map( x => x.split("\t")(0)).filter(row=> row != "host")

julyhost.take(10)
augusthost.take(10)

// COMMAND ----------

val intersect = julyhost.intersection(augusthost)
intersect.collect()

// COMMAND ----------

val unionrdd = julyhost.union(augusthost)
unionrdd.collect()

// COMMAND ----------


