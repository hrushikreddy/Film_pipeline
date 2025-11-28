
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, split, trim, monotonically_increasing_id, to_date, concat, lit, expr

def load_schema(path):
    with open(path) as f:
        raw=json.load(f)
    fields=[]
    for fdef in raw["fields"]:
        t=fdef["type"]
        if t=="string": dt=StringType()
        elif t=="integer": dt=IntegerType()
        elif t=="boolean": dt=BooleanType()
        elif t=="date": dt=DateType()
        else: dt=StringType()
        fields.append(StructField(fdef["name"],dt,fdef.get("nullable",True)))
    return StructType(fields)

def stage1(csv_path,schema_path,out_dir="output"):
    spark=SparkSession.builder.getOrCreate()
    raw=spark.read.option("header","true").csv(csv_path)
    schema=load_schema(schema_path)
    df=raw

    if "duration" in df.columns and "durationMins" not in df.columns:
        df=df.withColumnRenamed("duration","durationMins")

    if "release_year" in df.columns:
        df=df.withColumn("release_year_str",col("release_year").cast("string"))
        df=df.withColumn("release_year",to_date(concat(col("release_year_str"),lit("-01-01"))))
        df=df.drop("release_year_str")

    if "id" not in df.columns:
        df=df.withColumn("id",monotonically_increasing_id().cast("int"))

    for field in schema.fields:
        if field.name == "genres":
            continue
        if field.name in df.columns:
            df=df.withColumn(field.name,col(field.name).cast(field.dataType))

    df=df.withColumn("genres",split(col("genres"),","))
    df=df.withColumn("genres",expr("transform(genres,x->trim(x))"))

    out=f"{out_dir}/allFilms.parquet"
    df.write.mode("overwrite").parquet(out)
    spark.stop()

if __name__=="__main__":
    import sys
    stage1(sys.argv[1],sys.argv[2])
