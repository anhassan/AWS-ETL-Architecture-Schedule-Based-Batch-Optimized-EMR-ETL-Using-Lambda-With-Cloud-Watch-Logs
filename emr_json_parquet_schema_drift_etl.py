# A helper utility function to find s3 bucket name from s3 path
def get_bucket_name(path):
    path_filtered = path[len("s3://"):]
    bucket_name = path_filtered[0:path_filtered.find("/")]
    return bucket_name

# A helper utility function to find s3 key name from s3 path
def get_key_name(path,source=True):
    path_filtered = path[len("s3://"):]
    if not source:
        key_name = path_filtered[path_filtered.rfind("/")+1:]
    else:
        key_name = path_filtered[path_filtered.find("/")+1:]
    return key_name


sc.install_pypi_package("boto3")


from pyspark.sql.functions import *
from pyspark.sql.types import *

# A utility function responsible for archiving or rehydrating s3 files for incremental data processing
def archive_s3_files(df,src_s3_path,dst_s3_path,archive_key_name="",rehydrate_key_name="",rehydrate=False):
    
    src_bucket = get_bucket_name(src_s3_path)
    dst_bucket = get_bucket_name(dst_s3_path)

    s3_resource = boto3.resource("s3")
    s3_client = boto3.client("s3")
    
    path = dst_s3_path if rehydrate else src_s3_path
    external_key_name = rehydrate_key_name if rehydrate else archive_key_name
    
    
    df = spark.read \
          .option("inferSchema",True) \
          .json(path) \
          .withColumn("input_file_name",input_file_name())
    
    file_name_rows = df.select("input_file_name").distinct().collect()
    
    src_keys = [get_key_name(row["input_file_name"]) for row in file_name_rows]
    dst_keys = [external_key_name +  "/" + get_key_name(row["input_file_name"],False) 
               for row in file_name_rows]
    
    for index,src_key in enumerate(src_keys):
        
        s3_resource.meta.client.copy({"Bucket" : src_bucket,
                                      "Key" : src_key },dst_bucket,dst_keys[index])
        s3_client.delete_object(Bucket = src_bucket, Key = src_key)




# A helper function to explode the array elements present in the json
def explode_data(df):
    for field in df.schema.fields:
        if isinstance(field.dataType,ArrayType):
            df = df.withColumn(field.name,explode_outer(field.name))
    return df



# A helper recursive function to flatten the nested keys present in the json
def get_flattened_cols(df_schema_fields,flat_cols=[],prefix=""):
    for field in df_schema_fields:
        if isinstance(field.dataType,StructType):
            get_flattened_cols(field.dataType.fields,flat_cols,field.name)
        else:
            flat_col = [field.name if len(prefix)==0 else prefix + "." + field.name ]
            flat_cols.append(flat_col[0])
    return dict.fromkeys(flat_cols)



# A helper function to standardize the column names of the flatten json schema
def standardize_col_name(col_name):
    col_name = list(col_name)
    for ind,col_char in enumerate(col_name):
        if col_char == "." and ind+1 < len(col_name):
            col_name[ind+1] = col_name[ind+1].upper()
    col_name_derived = "".join(col_name)
    return  col_name_derived.replace(".","")



from pyspark.sql.functions import *

# Main driver function to flatten the input nested json into a denormalized tabular structure
def flatten_json(data):
    df = explode_data(data)
    df_cols = list(dict.fromkeys(get_flattened_cols(df.schema.fields,[])))
    df_cols_renamed = [col(df_col).alias(standardize_col_name(df_col)) for df_col in df_cols]
    return df.select(*[df_cols_renamed])



# A helper function to get the schema of the data present in the s3 target folder for cataloging in lake formation
def get_schema_for_data_catalog(df):
    schema_list = []
    schema_swap = {"LongType" : "bigint", "IntegerType" : "int"}
    for field in df.schema.fields:
        field_datatype = schema_swap[str(field.dataType)] if str(field.dataType) in list(schema_swap.keys()) else str(field.dataType).replace("Type","").lower()
        schema_list.append(
            {"Name": str(field.name).lower(),
             "Type":   field_datatype                   
        })
    return schema_list



# A helper function to get the schema of the partition columns of the  data present in the s3 target folder for cataloging in lake formation
def get_partition_schema_for_data_catalog(df,partition_cols):
    schema_list = get_schema_for_data_catalog(df)
    return [ attribute for attribute in schema_list if attribute["Name"] in partition_cols]



# A helper function to compare two versions of the scehma [before,after] the ingestion of new data
def get_schema_match(curr_schema,prev_schema):
    curr_schema_cols = [ element["Name"] for element in curr_schema]
    prev_schema_cols = [element["Name"] for element in prev_schema]
    return len(curr_schema_cols) == len(prev_schema_cols) and all(element in prev_schema_cols for element in curr_schema_cols)


import boto3
import time

# A helper function that waits until the crawler has fully executed
def wait_for_crawler(crawler_name,region):
    glue_client = boto3.client("glue",region_name=region)
    crawler_status, wait_counter = "NOT-READY",0
    while(crawler_status != "READY" and wait_counter < 200):
        time.sleep(2)
        crawler_status = glue_client.get_crawler(Name=crawler_name)["Crawler"]["State"]
        wait_counter+=1
    

import boto3

# A helper function that executes query in Athena
def execute_athena_query(query,database_name,results_loc,region):
    athena_client = boto3.client("athena",region_name=region)
    response = athena_client.start_query_execution(
    QueryString = query,
    QueryExecutionContext = {
        'Database': database_name
        }, 
    ResultConfiguration = { 'OutputLocation': results_loc}
    )
    return response


import boto3
import time

# A helper function that waits until the query in Athena has fully executed
def wait_for_query_execution(query_response,region):
    athena_client = boto3.client("athena",region_name=region)
    queryExecution = athena_client.get_query_execution(QueryExecutionId=query_response['QueryExecutionId'])
    query_status = queryExecution["QueryExecution"]["Status"]["State"]
    counter = 0
    while (query_status != "SUCCEEDED" and counter < 9):
        time.sleep(2)
        queryExecution =athena_client.get_query_execution(QueryExecutionId=query_response['QueryExecutionId'])
        query_status = queryExecution["QueryExecution"]["Status"]["State"]
        counter+=1
    return True if counter < 9 else False



# A utility function to get the already existing partitions available in Athena
def get_partitions_from_athena(catalog_database_name,catalog_table_name,results_loc,region):
    athena_client = boto3.client("athena",region_name=region)
    query = "SHOW PARTITIONS `{}`.`{}`".format(catalog_database_name,catalog_table_name)
    query_response = execute_athena_query(query,catalog_database_name,results_loc,region)
    wait_for_query_execution(query_response,region)
    query_result = athena_client.get_query_results(QueryExecutionId=query_response['QueryExecutionId'])
    partitions = [row_data["Data"][0]["VarCharValue"] for row_data in query_result["ResultSet"]["Rows"]]
    return partitions



# A helper function to get partitions from the data which is being ingested
def get_partitions_from_data(df,partition_cols):
    df_rows = df.select(*partition_cols).distinct().collect()
    rows = [list(row) for row in df_rows]
    partitions = []
    for row in rows:
        partition_item = ""
        for index,element in enumerate(row):
            partition_item += "{}={}/".format(partition_cols[index],element)
        partition_item = partition_item[0:-1]
        partitions.append(partition_item)
    
    return partitions



# A helper function to create ddl for new partitions discovered from the data being ingested
def get_partitions_ddl(catalog_database_name,catalog_table_name,partitions):
    ddl_query = "ALTER TABLE `{}`.`{}` ADD".format(catalog_database_name,catalog_table_name) + "\n"
    for partition in partitions:
        partition_clause = ""
        partition_vals = partition.split("/")
        for val in partition_vals:
            partition_clause += "{} = '{}' ,".format(val.split("=")[0],val.split("=")[1])
        partition_clause = partition_clause[0:-1]
        ddl_query = ddl_query + "PARTITION ({})".format(partition_clause) + "\n"
    ddl_query = ddl_query[0:-1] + " ;"
    return ddl_query



# Main function which adds new partitions to athena if new partitons are present in the data being ingested
def add_partitions_athena(catalog_database_name,catalog_table_name,results_loc,partition_cols,df,region):
    prev_partitions = get_partitions_from_athena(catalog_database_name,catalog_table_name,results_loc,region)
    curr_partitions = get_partitions_from_data(merged_df,partition_cols)
    new_partitions = [partition for partition in curr_partitions if partition not in prev_partitions]
    if len(new_partitions) > 0:
        partitions_ddl = get_partitions_ddl(catalog_database_name,catalog_table_name,new_partitions)
        query_response = execute_athena_query(partitions_ddl,catalog_database_name,results_loc,region)
        wait_for_query_execution(query_response,region)
        print("New Partitions loaded into Athena")
        print("Partitions Added : {}".format(new_partitions))
        
    else:
        print("No new partitions are there to be added into athena")
    
    


import boto3

# Main driver function to either create/update data catalog with the schema of the data present in the target s3 location by using crawler
def create_update_data_catalog(df,catalog_database_name,catalog_table_prefix,
                                catalog_table_data_location,role_arn,crawler_name,region,results_loc,partition_cols):
    
    glue_client = boto3.client("glue",region_name=region)
    catalog_table_name = catalog_table_prefix + "_" + catalog_table_data_location[catalog_table_data_location[0:-1].rfind("/")+1:-1]
    response = glue_client.get_tables(DatabaseName=catalog_database_name)
    catalog_tables_list = [table_meta["Name"] for table_meta in response["TableList"]]
    if len(response["TableList"]) == 0 or catalog_table_name not in catalog_tables_list:
        if crawler_name not in glue_client.list_crawlers()["CrawlerNames"]:
            print("Creating Crawler : {} since it does not exists".format(crawler_name))
            glue_client.create_crawler(
                                                    Name=crawler_name,
                                                    Role=role_arn,
                                                    DatabaseName=catalog_database_name,
                                                    Targets={"S3Targets":[{"Path":catalog_table_data_location}]},
                                                    TablePrefix=catalog_table_prefix + "_",
                                                    SchemaChangePolicy={"UpdateBehavior":"UPDATE_IN_DATABASE",
                                                                                          "DeleteBehavior": "DEPRECATE_IN_DATABASE"}
                                                   )
            print("Starting Crawler : {}".format(crawler_name))
            glue_client.start_crawler(Name=crawler_name)
            wait_for_crawler(crawler_name,region)
        else:
            print("Starting Crawler : {}".format(crawler_name))
            glue_client.start_crawler(Name=crawler_name)
            wait_for_crawler(crawler_name,region)
            
    else:
        response = glue_client.get_table(DatabaseName=catalog_database_name,Name=catalog_table_name)
        previous_schema  = response["Table"]["StorageDescriptor"]["Columns"] + response["Table"]["PartitionKeys"]
        current_schema = get_schema_for_data_catalog(df)
        schema_match = get_schema_match(current_schema,previous_schema)
        if not schema_match:
            print("Starting Crawler due to schema drift")
            glue_client.start_crawler(Name=crawler_name)
            wait_for_crawler(crawler_name,region)
        else:
            add_partitions_athena(catalog_database_name,catalog_table_name,results_loc,partition_cols,df,region)
            
        


#-----------------------------------------ETL Driver Code--------------------------------------------------------#




from pyspark.sql import SparkSession

# Creating the spark session for using spark transformations and actions
spark = SparkSession \
    .builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.delta.logStore.class","org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()



# Reading the input configurations from s3 config file for the ETL process
input_config_path = "s3://datalake-platform/dependencies/configurations/config_emr.json"
configs = spark.read\
                         .option("multiline",True)\
                         .json(input_config_path)\
                         .collect()[0]
SOURCE_S3_PATH = configs["source_s3_path"]
SINK_S3_PATH = configs["sink_s3_path"]
ROLE_ARN = configs["role_arn"]
CATALOG_DB_NAME = configs["catalog_db_name"]
CATALOG_TABLE_PREFIX = configs["catalog_table_prefix"]
CRAWLER_NAME = configs["crawler_name"]
REGION = configs["region"]
ARCHIVE_FOLDER = configs["archival_folder"]
REHYDRATION_FOLDER = configs["rehydration_folder"]
RESULTS_LOC = configs["results_loc"]


from pyspark.sql.functions import *
from pyspark.sql.types import *

# Reading the input json files from the source s3 location
df = spark.read\
                 .option("inferSchema",True)\
                 .json(SOURCE_S3_PATH)\
                 .withColumn("input_file", input_file_name())



# Flattening the input json df and creating partition columns from the input data
df_denormalized = flatten_json(df)\
                               .withColumn("row_insert_tsp",from_unixtime("tts"))\
                               .withColumn("row_insert_date",to_date("row_insert_tsp"))\
                               .withColumn("year",year(col("row_insert_date")))\
                               .withColumn("month",month(col("row_insert_date")))\
                               .withColumn("day",date_format(col("row_insert_date"),"d"))



# Defining the partition columns in order
partition_cols = ["customerid","vehicle","category","year","month","day"]


# Writing the flattened json input messages according to the defined partitions
df_denormalized.write\
                           .mode("append")\
                           .partitionBy(*partition_cols)\
                           .save(SINK_S3_PATH)



# Reading for schema drifts if any
merged_df = spark.read\
                              .option("mergeSchema","true")\
                              .parquet(SINK_S3_PATH)



# Cataloging the most recent schema in the lake formation for making AdHoc Athena queries
create_update_data_catalog(merged_df,CATALOG_DB_NAME,CATALOG_TABLE_PREFIX,SINK_S3_PATH.replace("s3a://","s3://").replace("-","_"),
                           ROLE_ARN,CRAWLER_NAME,REGION,RESULTS_LOC,partition_cols)



# Archiving S3 files to ensure that incremental data processing takes place
archive_s3_files(df,SOURCE_S3_PATH,SOURCE_S3_PATH,ARCHIVE_FOLDER)

