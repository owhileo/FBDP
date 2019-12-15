import sys,codecs
sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())

from pyspark import SparkContext
from operator import add

logFile = "file:////data/million_user_log.csv"
sc = SparkContext("local", "million user count app")

def get_top10_review():
    logData = sc.textFile(logFile).cache()
    view_log = logData.filter(lambda x: x.split(',')[7]=='0')
    view_log=view_log.map(lambda x: (x.split(',')[4],x.split(',')[0])).distinct()
    view_populated=sorted(view_log.countByKey().items(),key=lambda x:x[1],reverse=True)
    print("\033[1;31m")
    for i,j in view_populated[:10]:
        print("%s:%i"%(i,j))
    print("\033[0m")

def get_topsale_in_province(key):
    # key=2:cat_id; key=1:item_id
    logData = sc.textFile(logFile).cache()
    buy_log = logData.filter(lambda x: x.split(',')[7]=='2')
    buy_log=buy_log.map(lambda x: ((x.split(',')[10],x.split(',')[key]),1)).reduceByKey(add)
    buy_log=buy_log.map(lambda x: (x[0][0],(x[0][1],x[1]))).groupByKey().map(lambda x:(x[0],sorted(x[1],key=lambda y:y[1],reverse=True)[:10]))
    buy_populated=buy_log.collect()

    print("\033[1;31m")
    for i,j in buy_populated:
        print("%s:"%(i,))
        for ii,jj in j:
            print("\t%s:%i"%(ii,jj))
    print("\033[0m")


if __name__=='__main__':
    get_topsale_in_province(2)
    get_topsale_in_province(1)
    get_top10_review()
