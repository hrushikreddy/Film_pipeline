
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def build(df,f):
    cond=None
    for k,v in f.items():
        if "between" in v:
            lo,hi=v["between"]
            c=(col(k)>=lo)&(col(k)<=hi)
        elif "in" in v:
            c=col(k).isin(v["in"])
        else:
            continue
        cond=c if cond is None else cond & c
    return df if cond is None else df.filter(cond)

def query(p="output/allFilms.parquet",f=None):
    spark=SparkSession.builder.getOrCreate()
    df=spark.read.parquet(p)
    return df if not f else build(df,f)

if __name__=="__main__":
    q={"release_year":{"between":["2000-01-01","2010-01-01"]}}
    query(f=q).show()
