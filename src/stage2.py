
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,col,trim
import os

def stage2(p,out="output/genres"):
    spark=SparkSession.builder.getOrCreate()
    df=spark.read.parquet(p)
    e=df.withColumn("genre",explode(col("genres")))
    e=e.withColumn("genre",trim(col("genre")))
    genres=[r.genre for r in e.select("genre").distinct().collect() if r.genre]
    os.makedirs(out,exist_ok=True)
    for g in genres:
        safe=g.replace(" ","_").replace("/","_").replace(",","_")
        e.filter(col("genre")==g).drop("genre").write.mode("overwrite").parquet(f"{out}/{safe}.parquet")
    spark.stop()

if __name__=="__main__":
    import sys
    stage2(sys.argv[1])
