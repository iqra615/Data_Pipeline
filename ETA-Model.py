import findspark
findspark.init("C:\\Users\\ramkumar.ganesan\\spark-2.4.0-bin-hadoop2.7")
import pyspark 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,length,row_number
from pyspark.sql.window import Window
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
uri='mongodb://127.0.0.1/....'

#file Handling operations 
f=open("C:\\Users\\ramkumar.ganesan\\PLAN_INTERFACE.csv","r")
data=f.readlines()
first_lines=data[0]
replace_lines=first_lines.replace(' ','')
data[0]=replace_lines
f=open("C:\\Users\\ramkumar.ganesan\\PLAN_INTERFACE.csv","w")
f.writelines(data)
f.close()

#regex for dateformat:DD/MM/YYY :01/01/1900 to 31/12/2099
regex='^(0[1-9]|[12][0-9]|3[01])[- /.](0[1-9]|1[012])[- /.](19|20)\d\d$'

inputdf = spark.read.option("infer schema","true").option("header","true").option("delimiter","|").csv("C:\\Users\\ramkumar.ganesan\\PLAN_INTERFACE.csv")

#validations such as  not null & date format&maximunlength
validf=inputdf.filter(col("PLAN_INTERFACE_ID").isNotNull()&col("PLAN_ID").isNotNull()&col("PLAN_NO").isNotNull()&col("CREATED_DATE").rlike(regex)&col("MODIFIED_DATE").rlike(regex)&(length(col("BATCH_ID"))<30))
invalidf=inputdf.filter(col("PLAN_INTERFACE_ID").isNull()|col("PLAN_ID").isNull()|col("PLAN_NO").isNull()|~(col("CREATED_DATE").rlike(regex))| ~(col("MODIFIED_DATE").rlike(regex))|(length(col("BATCH_ID"))>30))

#separation of non-duplicate and duplicate records
nondupdf=validf.drop_duplicates(subset=['PLAN_INTERFACE_ID'])
windowSpec = Window.partitionBy("PLAN_INTERFACE_ID").orderBy("PLAN_INTERFACE_ID")
interdf=validf.withColumn("rownumber", row_number().over(windowSpec)).where("rownumber > 1")
dupdf=interdf.drop(col("rownumber"))
uniondf=invalidf.unionAll(dupdf)
nondupdf.show()
uniondf.show()

#storing results in hive table
nondupdf.write.saveAsTable("hive_valid_records11")
uniondf.write.saveAsTable("hive_invalid_records11")

#storing results in mongodb
nondupdf.write.format("com.mongodb.spark.sql.DefaultSource").options(uri=uri, collection="Mongo_valid_records").save()
uniondf.write.format("com.mongodb.spark.sql.DefaultSource").options(uri=uri, collection="Mongo_invalid_records").save()

