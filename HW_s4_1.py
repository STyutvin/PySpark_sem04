""" Есть займ 9 400 000 руб., срок 30 лет, ставка 10.6%. Добавьте для анализа два постоянных платежа 120 или 150 тыс. руб.
Добавьте графики с досрочным погашением по этим параметрам."""

import pyspark,time,platform,sys,os
from datetime import datetime
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col,lit,current_timestamp
import pandas as pd
import matplotlib.pyplot as plt 
from sqlalchemy import inspect,create_engine
from pandas.io import sql
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as sum1
import warnings,matplotlib
warnings.filterwarnings("ignore")
con=create_engine("mysql://root:D8rfvqzlo@localhost/spark")
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark=SparkSession.builder.appName("Hi").getOrCreate()


sql.execute("""
drop table if exists spark.HW_sem4_1""", con)
sql.execute("""
CREATE TABLE if not exists spark.HW_sem4_1 (
	`№` INT(10) NULL DEFAULT NULL,
	`Месяц` DATETIME NULL DEFAULT NULL,
	`Сумма платежа` FLOAT NULL DEFAULT NULL,
	`Платеж по основному долгу` FLOAT NULL DEFAULT NULL,
	`Платеж по процентам` FLOAT NULL DEFAULT NULL,
	`Остаток долга` FLOAT NULL DEFAULT NULL,
	`проценты` FLOAT NULL DEFAULT NULL,
	`долг` FLOAT NULL DEFAULT NULL
)
COLLATE='utf8mb4_0900_ai_ci'
ENGINE=InnoDB""",con)
w = Window.partitionBy(lit(1)).orderBy("№").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df1 = spark.read.format("com.crealytics.spark.excel")\
        .option("sheetName", "Sheet1")\
        .option("useHeader", "false")\
        .option("treatEmptyValuesAsNulls", "false")\
        .option("inferSchema", "true").option("addColorColumns", "true")\
	.option("usePlainNumberFormat","true")\
        .option("startColumn", 0)\
        .option("endColumn", 99)\
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")\
        .option("maxRowsInMemory", 20)\
        .option("excerptSize", 10)\
        .option("header", "true")\
        .format("excel")\
        .load("c:/spark/files/Seminar_04/HW_s4_1.xlsx").limit(1000)\
        .withColumn("проценты", sum1(col("Платеж по процентам")).over(w))\
        .withColumn("долг", sum1(col("Платеж по основному долгу")).over(w))
df1.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=D8rfvqzlo")\
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "HW_sem4_1")\
        .mode("append").save()

sql.execute("""
drop table if exists spark.HW_sem4_2""", con)
sql.execute("""
CREATE TABLE if not exists spark.HW_sem4_2 (
	`№` INT(10) NULL DEFAULT NULL,
	`Месяц` DATETIME NULL DEFAULT NULL,
	`Сумма платежа` FLOAT NULL DEFAULT NULL,
	`Платеж по основному долгу` FLOAT NULL DEFAULT NULL,
	`Платеж по процентам` FLOAT NULL DEFAULT NULL,
	`Остаток долга` FLOAT NULL DEFAULT NULL,
	`проценты` FLOAT NULL DEFAULT NULL,
	`долг` FLOAT NULL DEFAULT NULL
)
COLLATE='utf8mb4_0900_ai_ci'
ENGINE=InnoDB""",con)
q = Window.partitionBy(lit(1)).orderBy("№").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df2 = spark.read.format("com.crealytics.spark.excel")\
        .option("sheetName", "Sheet1")\
        .option("useHeader", "false")\
        .option("treatEmptyValuesAsNulls", "false")\
        .option("inferSchema", "true").option("addColorColumns", "true")\
	.option("usePlainNumberFormat","true")\
        .option("startColumn", 0)\
        .option("endColumn", 99)\
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")\
        .option("maxRowsInMemory", 20)\
        .option("excerptSize", 10)\
        .option("header", "true")\
        .format("excel")\
        .load("c:/spark/files/Seminar_04/HW_s4_2.xlsx").limit(1000)\
        .withColumn("проценты", sum1(col("Платеж по процентам")).over(q))\
        .withColumn("долг", sum1(col("Платеж по основному долгу")).over(q))
df2.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=D8rfvqzlo")\
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "HW_sem4_2")\
        .mode("append").save()

sql.execute("""
drop table if exists spark.HW_sem4_3""", con)
sql.execute("""
CREATE TABLE if not exists spark.HW_sem4_3 (
	`№` INT(10) NULL DEFAULT NULL,
	`Месяц` DATETIME NULL DEFAULT NULL,
	`Сумма платежа` FLOAT NULL DEFAULT NULL,
	`Платеж по основному долгу` FLOAT NULL DEFAULT NULL,
	`Платеж по процентам` FLOAT NULL DEFAULT NULL,
	`Остаток долга` FLOAT NULL DEFAULT NULL,
	`проценты` FLOAT NULL DEFAULT NULL,
	`долг` FLOAT NULL DEFAULT NULL
)
COLLATE='utf8mb4_0900_ai_ci'
ENGINE=InnoDB""",con)
x = Window.partitionBy(lit(1)).orderBy("№").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df3 = spark.read.format("com.crealytics.spark.excel")\
        .option("sheetName", "Sheet1")\
        .option("useHeader", "false")\
        .option("treatEmptyValuesAsNulls", "false")\
        .option("inferSchema", "true").option("addColorColumns", "true")\
	.option("usePlainNumberFormat","true")\
        .option("startColumn", 0)\
        .option("endColumn", 99)\
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")\
        .option("maxRowsInMemory", 20)\
        .option("excerptSize", 10)\
        .option("header", "true")\
        .format("excel")\
        .load("c:/spark/files/Seminar_04/HW_s4_3.xlsx").limit(1000)\
        .withColumn("проценты", sum1(col("Платеж по процентам")).over(x))\
        .withColumn("долг", sum1(col("Платеж по основному долгу")).over(x))
df3.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=D8rfvqzlo")\
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "HW_sem4_3")\
        .mode("append").save()


df1_plot = df1.toPandas()
df2_plot = df2.toPandas()
df3_plot = df3.toPandas()
# Get current axis 
ax = plt.gca()
ax.ticklabel_format(style='plain')
# bar plot
df1_plot.plot(kind='line', 
        x='№', 
        y='долг', 
        color='green', ax=ax)
df1_plot.plot(kind='line', 
        x='№', 
        y='проценты', 
        color='red', ax=ax)
df2_plot.plot(kind='line', 
        x='№', 
        y='долг', 
        color='blue', ax=ax)
df2_plot.plot(kind='line', 
        x='№', 
        y='проценты', 
        color='yellow', ax=ax)
df3_plot.plot(kind='line', 
        x='№', 
        y='долг', 
        color='violet', ax=ax)
df3_plot.plot(kind='line', 
        x='№', 
        y='проценты', 
        color='orange', ax=ax)


# set the title 
plt.title('Выплаты')
plt.grid ( True )
ax.set(xlabel=None)
# show the plot 
plt.show()


t0=time.time()
spark.stop()
t1=time.time()
print('finished',time.strftime('%H:%M:%S',time.gmtime(round(t1-t0))))
