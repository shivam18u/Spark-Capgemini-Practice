// Databricks notebook source
println("Hello World!")

// COMMAND ----------

val data = List(1,2,3,4,5)
val creationRDD = sc.parallelize(data)

// COMMAND ----------

creationRDD.count()
creationRDD.partitions.size

// COMMAND ----------

val maprdd = creationRDD.map( x => x*x*x).collect()

// COMMAND ----------

maprdd.take(4)
maprdd.filter( x => x%2==0 )

// COMMAND ----------

val mainrdd = sc.parallelize(List("Hey","hello","shivam","yo"))
mainrdd.collect()

// COMMAND ----------

mainrdd.flatMap( x => x.split(",")).collect()

// COMMAND ----------

val data = sc.textFile("FileStore/tables/airports.text")

// COMMAND ----------

val ussAirport = data.filter(line => line.split(",")(3) == "\"United States\"").collect()

// COMMAND ----------

ussAirport.take(2)

// COMMAND ----------


