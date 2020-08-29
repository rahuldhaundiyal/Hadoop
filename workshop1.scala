// Databricks notebook source
// Load the data into dataframe
val countrydf = spark.read.format("csv")
  .option("header","true")
  .option("inferSchemma","true")
  .load("/FileStore/tables/Country.csv")
countrydf.printSchema()

// COMMAND ----------

// Create a temp table using the dataFrame
countrydf.createOrReplaceTempView("CountryTempTable")
spark.sql("select * from CountryTempTable").show()

// COMMAND ----------

// MAGIC %sql
select * from CountryTempTable limit 10;

// COMMAND ----------

// Load the indicator file into dataframe
val Indicatordf = spark.read.format("csv")
  .option("header","true")
  .option("inferSchemma","true")
  .load("/FileStore/tables/Indicators.csv")
Indicatordf.printSchema()

// COMMAND ----------

// Cretae temp table using Dataframe
Indicatordf.createOrReplaceTempView("IndicatortempTable")
spark.sql("select * from IndicatortempTable").show()

// COMMAND ----------

//Problem Statement 1: Get the Gini Index using Dataframe. 

val GiniIndex = Indicatordf.select("year","value").filter($"IndicatorCode"==="SI.POV.GINI" and $"CountryCode"==="IND").orderBy($"year" desc)
GiniIndex.show()


// COMMAND ----------
---GET THE GINI INDEX USING SQL IN SPARK----
 %sql
 select year,value from 
 IndicatorTempTable
where IndicatorCode = "SI.POV.GINI"
 and CountryCode = "IND"
 order by year desc;

// COMMAND ----------

//Probllem Statement2 : Get the Litracy rate using Scala Dataframe.

val joindf = Indicatordf.join(countrydf,Indicatordf("CountryCode")===countrydf("CountryCode"))
val filtdf = joindf.select("value","ShortName").filter($"IndicatorCode"==="SE.ADT.1524.LT.ZS" and $"year"==="1991")
val sortdf = filtdf.withColumnRenamed("value","Youth_Lit_Rat").orderBy($"Youth_Lit_Rat" desc)
sortdf.show()

// COMMAND ----------
---GET THE LITERACY RATE USING SQL IN SPARK----
%sql
select value as Youth_Lit_Rate,ShortName
from CountryTempTable c join IndicatortempTable i
on c.CountryCode = i.CountryCode 
where IndicatorCode = "SE.ADT.1524.LT.ZS" and year = 1991
order By Youth_Lit_Rate desc;

// COMMAND ----------
// Problem Statement 3 : Get the trade percantage of gdp INDIA & CHINA. 

val tradedf = Indicatordf.select("Value","Year","CountryCode").filter($"IndicatorCode" === "NE.TRD.GNFS.ZS" and $"CountryCode".isin("IND","CHN")).orderBy ($"year" desc).show()

// COMMAND ----------
---GET THE TRADE PERCENTAGE OF GDP INDIA & CHINA USING SQL IN SPARK
%sql
select value,Year,CountryCode from IndicatortempTable
where IndicatorCode = "NE.TRD.GNFS.ZS" and CountryCode in ("IND","CHN")
order by year desc;

// COMMAND ----------

//Problem Statement 4 : Display the export of goods & services. 
val exportdf = Indicatordf.select("Value","Year","CountryName").filter($"IndicatorCode" === "NE.EXP.GNFS.ZS" and $"CountryCode" === "IND").orderBy($"Year" desc).show()

// COMMAND ----------
Export of goods & Services using spark sql
%sql
select value,year,CountryCode,CountryName from IndicatortempTable
where IndicatorCode="NE.EXP.GNFS.ZS" and CountryCode ="IND"
order by year desc
