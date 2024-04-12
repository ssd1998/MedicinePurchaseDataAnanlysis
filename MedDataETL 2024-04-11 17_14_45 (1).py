# Databricks notebook source
# MAGIC %md
# MAGIC Mounting Silver container of Data Lake storage

# COMMAND ----------

# dbutils.fs.mount(

#       source = "wasbs://silver@newprojstorage12.blob.core.windows.net",

#       mount_point = "/mnt/data/silver",

#       extra_configs = {'fs.azure.account.key.newprojstorage12.blob.core.windows.net': "j+SLxhWH3cEnJaQ50k/0rN/nZ93LQOSfiXxC96WORQOet550WKVbFoFir2Dv+DFsDYZeWXIqD34z+AStG+dn5A=="}

#       #extra_configs = {'fs.azure.sas.' + blobContainerName + '.' + storageAccountName + '.blob.core.windows.net': sasToken}

#     )


# COMMAND ----------

# MAGIC %md
# MAGIC Creating dataframe of Medicine and Customers file

# COMMAND ----------

df_Patient = spark.read.format("csv").option("header",True).load("dbfs:/mnt/data/silver/Daily_Patient_Purchase_Data.csv")
df_Medicine = spark.read.format("csv").option("header",True).load("dbfs:/mnt/data/silver/MedicineData.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Importing necessary functions

# COMMAND ----------

from pyspark.sql.functions import col, to_date, current_date, date_diff, year, floor, current_timestamp
from pyspark.sql.types import DateType, IntegerType


# COMMAND ----------

# MAGIC %md
# MAGIC Joining Medicine and Patient/Customers dataframe

# COMMAND ----------

df_join_MedicinePatient = df_Patient.join(df_Medicine,df_Patient.Productid == df_Medicine.MedicineID,"inner").drop(df_Medicine.Quantity)


# COMMAND ----------

# MAGIC %md
# MAGIC Converting columns consisting date values, integer values from string datatype to date datatype and integer datatype
# MAGIC Renaming medicine name and medicine description column 

# COMMAND ----------

df_joined_MedicinePatient = df_join_MedicinePatient\
    .withColumn('Productid',col('Productid').cast(IntegerType()))\
        .withColumn('createdAt',(to_date(col('createdAt'),'MM/dd/yyyy').alias('createdAt')).cast(DateType()))\
            .withColumn('updatedAt',(to_date(col('updatedAt'),'MM/dd/yyyy').alias('updatedAt')).cast(DateType()))\
                .withColumn('birthdate',(to_date(col('birthdate'),'MM/dd/yyyy').alias('birthdate')).cast(DateType()))\
                    .withColumn('LastAmountPaid',col('LastAmountPaid').cast(IntegerType()))\
                        .withColumn('TotalAmount_TillUpdatedDt',col('TotalAmount_TillUpdatedDt').cast(IntegerType()))\
                            .withColumnRenamed('Name','Medicine_Purchased')\
                                .withColumnRenamed('Description','Medicine_Description')


# COMMAND ----------

# MAGIC %md
# MAGIC Calculating Age of the patients/customers

# COMMAND ----------

df_joined_MedicinePatient = df_joined_MedicinePatient.withColumn('Age',floor(date_diff(current_date(), to_date(col('birthdate'), 'M/d/yyyy'))/365.25))

# COMMAND ----------

# MAGIC %md
# MAGIC Getting data of terminated members till today and loading it to separate file

# COMMAND ----------


df_Terminated_Customers = df_joined_MedicinePatient.filter((col('Status')=='Terminated') & (col('updatedAt') == to_date(current_date(), 'MM/dd/yyyy')))


# COMMAND ----------

# MAGIC %md
# MAGIC Mounting gold container from Data Lake

# COMMAND ----------

# dbutils.fs.mount(

#       source = "wasbs://gold@newprojstorage12.blob.core.windows.net",

#       mount_point = "/mnt/data/gold",

#       extra_configs = {'fs.azure.account.key.newprojstorage12.blob.core.windows.net': "j+SLxhWH3cEnJaQ50k/0rN/nZ93LQOSfiXxC96WORQOet550WKVbFoFir2Dv+DFsDYZeWXIqD34z+AStG+dn5A=="}

#       #extra_configs = {'fs.azure.sas.' + blobContainerName + '.' + storageAccountName + '.blob.core.windows.net': sasToken}

#     )

# COMMAND ----------

# MAGIC %md
# MAGIC Renaming the filename as per loaded data

# COMMAND ----------

df_Terminated_Customers_toPandas = df_Terminated_Customers.toPandas()

# COMMAND ----------

df_Terminated_Customers_toPandas.to_csv('/dbfs/mnt/data/gold/TerminatedMembersData.csv',index=False,header=True,mode='w')

# COMMAND ----------

# MAGIC %md
# MAGIC Getting Active patients/customers data and loading to separate file

# COMMAND ----------

df_Active_Members = df_joined_MedicinePatient.filter(col('Status')=='Active')

# COMMAND ----------

# MAGIC %md
# MAGIC Renaming the filename as per loaded data

# COMMAND ----------

df_Active_Members_toPandas = df_Active_Members.toPandas()

# COMMAND ----------

df_Active_Members_toPandas.to_csv('/dbfs/mnt/data/gold/ActiveMembersData.csv',index=False,header=True,mode='w')

# COMMAND ----------

# MAGIC %md
# MAGIC Getting newly added members 

# COMMAND ----------

df_NewlyJoined_Members = df_joined_MedicinePatient.where((col('createdAt')==to_date(current_date(),'MM/dd/yyy')) & (col('updatedAt')==to_date(current_date(),'MM/dd/yyy')) & (col('Status')=='Active'))

# COMMAND ----------

# MAGIC %md
# MAGIC Renaming Filename as per loaded data

# COMMAND ----------

df_NewlyJoined_Members_toPandas = df_NewlyJoined_Members.toPandas()


# COMMAND ----------

df_NewlyJoined_Members_toPandas.to_csv('/dbfs/mnt/data/gold/NewlyAddedMembersData.csv',index=False,header=True,mode='w')

# COMMAND ----------


