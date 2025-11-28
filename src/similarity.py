
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, array
from pyspark.sql.types import DoubleType

def j(a,b):
    if not a or not b: return 0.0
    sa=set([x.lower().strip() for x in a if x])
    sb=set([x.lower().strip() for x in b if x])
    if not sa and not sb: return 0.0
    return len(sa & sb) / len(sa | sb)

def s_df(fid,t,p="output/allFilms.parquet"):
    spark=SparkSession.builder.getOrCreate()
    df=spark.read.parquet(p).cache()
    tgt=df.filter(col("id")==fid).limit(1).collect()[0].asDict()

    j_udf=udf(lambda a,b:j(a,b),DoubleType())
    y_udf=udf(lambda y: float(max(0.0,1.0-(abs(y.year - tgt["release_year"].year)/50))) if y and tgt.get("release_year") else 0.0,DoubleType())

    comp=df.withColumn("director_match",(col("director")==tgt.get("director")).cast("double")) \
           .withColumn("genre_sim", j_udf(col("genres"), array(*[x for x in tgt.get("genres") or []]))) \
           .withColumn("year_sim", y_udf(col("release_year"))) \
           .withColumn("similarity_score",
                       0.4*col("director_match")+
                       0.4*col("genre_sim")+
                       0.2*col("year_sim"))

    return comp.filter(col("similarity_score")>=t).filter(col("id")!=fid)

if __name__=="__main__":
    import sys
    fid=int(sys.argv[1]); th=float(sys.argv[2])
    s_df(fid,th).select("id","title","similarity_score").show()
