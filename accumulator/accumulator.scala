// Databricks notebook source
//https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/8363166053443355/2593651523577547/3462821094046267/latest.html

// COMMAND ----------

val stack = sc.textFile("FileStore/tables/StackOverflow_survey_responses.csv")

// COMMAND ----------

val totalRows = sc.longAccumulator
val missingSal = sc.longAccumulator


// COMMAND ----------

val afghan = stack.filter(row => { val splitData =row.split(",",-1); 
                     totalRows.add(1)
                     if(splitData(14).isEmpty){
                       missingSal.add(1)
                     }
                     splitData(2)=="Afghanistan"
                    })

// COMMAND ----------

afghan.collect()
print(totalRows)

// COMMAND ----------

print(missingSal)

// COMMAND ----------


