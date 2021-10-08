##################################################
##########Import Libraries and set variables######
##################################################

from datetime import datetime
from pyspark.context import SparkContext
import pyspark.sql.functions as f

#Import glue modules
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

#Initialize contexts and sessions
spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
session = glue_context.spark_session

#Parameters
glue_db = "purchase"
glue_tbl = "data_files"
s3_write_path = "s3://adbassessment/glue_output/"


def revenue_cal(x):
    """

    This Method iterates through the product list attribute of the transaction
    and sums up all the revenue of the transaction if a purchase happens
    Parameters
    ----------
    X: dataframe column values, required
    Returns
    ------
    Total Sum: Integer
    """

    sum = 0
    for val in str(x).split(','):
        revenue = str(val).split(';')
        if len(revenue) > 3 and (revenue[3] != None and revenue[3] != "" and revenue[3] != " " and revenue[3] != "none" and revenue[3] != "None"):
            sum += float(revenue[3].strip())
    return sum


def purchase_trans_filter(x):
    """

    This Method iterates through the event list attribute of the transaction
    and filters the values for a purchase transactions.
    Parameters
    ----------
    X: dataframe column values, required
    Returns
    ------
    Purchase Transaction: Integer

    """

    event = []
    for event_trans in str(x).split(','):
        if str(event_trans) is None or str(event_trans) == "" or str(event_trans) == " " or str(
                event_trans) == "NaN" or str(event_trans) == "nan" or str(event_trans) == "none" or str(event_trans) == "None":
            pass
        elif int(float(event_trans)) == 1:
            event_trans = int(float(event_trans))
            print(event_trans)
            return event_trans

############################################
#############EXTRACT (READ DATA) ###########
#############################################

#Read data from Glue dynamic frame

dynamic_frame_read = glue_context.create_dynamic_frame.from_catalog(database=glue_db,table_name=glue_tbl)

#Convert dynamic frame to data frame to use standard pyspark functions

df = dynamic_frame_read.toDF()

############################################
############# (TRANSFORM DATA) ###########
#############################################

from pyspark.sql.functions import udf,col,regexp_extract
from pyspark.sql.types import StringType

udf_revenue_cal = udf(lambda x:revenue_cal(x),StringType() )

udf_purchase_trans_filter = udf(lambda x:purchase_trans_filter(x),StringType() )

formatted_df= df.withColumn("revenue",udf_revenue_cal(col("product_list"))) \
                .withColumn("even_list_values",udf_purchase_trans_filter(col("event_list"))) \
                .withColumn('search_domain', regexp_extract(col('referrer'), '(https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+)', 1)) \
                .withColumn('search_key', regexp_extract(col('referrer'), '(\W*\\?=([^&#]*))', 1)) \
                .where(col("even_list_values")=='1') \
                .select ('ip', 'search_key', 'even_list_values', 'revenue', 'search_domain', 'hit_time_gmt')


formatted_df.dropna()

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,sum

windowSpec_row_number  = Window.partitionBy("ip").orderBy("hit_time_gmt")

windowSpec_sum  = Window.partitionBy("ip")

revenue_df=formatted_df.withColumn("row",row_number().over(windowSpec_row_number)) \
           .withColumn("total_revenue", sum(col("revenue")).over(windowSpec_sum)) \
           .where(col("row")==1).select("search_key","search_domain","total_revenue")

revenue_df=revenue_df.repartition(1)


#Convert back to dynamic frame
revenue_frame_write = DynamicFrame.fromDF(revenue_df,glue_context,"revenue_frame_write")


##########################################################
############# PERSIST DATA ##################################
##########################################################

#Write back to S3

glue_context.write_dynamic_frame.from_options(
    frame = revenue_frame_write,
    connection_type ="s3",
    connection_options ={
        "path" : s3_write_path,
    },
    format = "csv"
)

