// Databricks notebook source
//  https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/8363166053443355/3144059327038250/3462821094046267/latest.html

// COMMAND ----------

val data = List("Shivam","Ubarhande","football","meta","Shivam")

// COMMAND ----------

val datardd = sc.parallelize(data)
datardd.count()

// COMMAND ----------

datardd.countByValue()

// COMMAND ----------

val data2rdd = sc.parallelize(List(1,2,3,4,5))
val productrdd= data2rdd.reduce((x,y) => x*y)
productrdd

// COMMAND ----------

val numbers = sc.textFile("FileStore/tables/numberData.csv")
val header = numbers.first()
val numbersdata=numbers.filter( row => row != header).map(_.toInt)
numbersdata.collect()

// COMMAND ----------

def isPrime( a:Int) : Int = {
  var x = 0;
  if (a<=1) return 0
  else if (a==2) return a
  for( x <- 2 to a/2){
    if (a%x==0){
      return 0
    }
  }
  return a
}

// COMMAND ----------

val primenums =numbersdata.map(isPrime).filter(row => row != 0)

// COMMAND ----------

primenums.collect()

// COMMAND ----------

val listData = sc.parallelize(List("Shivam 1239", "Tushar 3432","Abhinav 2334"))
val tp = listData.map(row => (row.split(" ")(0),row.split(" ")(1).toInt))
tp.collect()

// COMMAND ----------

tp.mapValues(x=>x+10).collect()

// COMMAND ----------

val Airdata = sc.textFile("FileStore/tables/airports.text")

// COMMAND ----------

val Air_time = Airdata.map(row=> (row.split(",")(1),row.split(",")(11).toLowerCase()))

// COMMAND ----------

Air_time.take(5)

// COMMAND ----------

val property = sc.textFile("FileStore/tables/Property_data.csv")

// COMMAND ----------

val propertydata = property.filter(row => !row.contains("Price"))

// COMMAND ----------

val roomproperty = propertydata.map(row => (row.split(",")(3).toInt,(1,row.split(",")(2).toDouble)))
roomproperty.take(4)

// COMMAND ----------

val totalrooms= roomproperty.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))

// COMMAND ----------

val finalrdd=totalrooms.mapValues(row=> (row._2/row._1))

// COMMAND ----------

for ((bedroom,avg) <- finalrdd.collect()){
  println(bedroom + " : " + avg)
}

// COMMAND ----------


