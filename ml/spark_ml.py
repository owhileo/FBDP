import sys,codecs
sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())

import matplotlib.pyplot as plt
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.classification import *
from pyspark.ml.feature import VectorAssembler,StringIndexer
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from sklearn.metrics import precision_score, recall_score, roc_auc_score, roc_curve, \
    accuracy_score, r2_score, f1_score
import numpy as np

from hyperopt import hp,STATUS_OK,Trials,fmin,tpe


conf=SparkConf().setAppName("ml classifier")
sc = SparkContext(conf=conf)
spark=SparkSession.builder.config(conf=conf).getOrCreate()

def get_csv(file):
    logFile = "file:////data/"+file
    logData = sc.textFile(logFile).cache()
    return logData

def get_train_data():
    df=spark.read.csv("file:////data/train_after.csv")
    for i in df.columns:
        df = df.withColumn(i, df[i].cast("float"))

    df=df.withColumnRenamed('_c4','label')
    df.groupBy('label').count().show()
    return df

def prepare_data():
    df=get_train_data()

    # categoryIndexer = StringIndexer(inputCol='_c1', outputCol='_c11')
    # categoryTransfomer = categoryIndexer.fit(df)
    # df = categoryTransfomer.transform(df)
    # categoryIndexer2 = StringIndexer(inputCol='_c2', outputCol='_c22')
    # categoryTransfomer2 = categoryIndexer2.fit(df)
    # df = categoryTransfomer2.transform(df)

    assembler = VectorAssembler(inputCols=['_c0','_c1','_c2','_c3'], outputCol= "features")
    df_s=assembler.transform(df)
    res_train=[]
    res_test=[]

    train,test=df_s.randomSplit([0.8,0.2])
    train.cache()
    test.cache()
    res=train.groupby('label').count().toPandas()['count']
    train_s=train.sampleBy('label', {1:1,0:res[0]/res[1]},seed=234)
    for i in range(9):
        train_s=train_s.union(train.sampleBy('label', {1:1,0:res[0]/res[1]}))
    res_train.append(train_s)
    res_test.append(test)

    return train_s,test

def train(df):
    # categoryIndexer = StringIndexer(inputCol='_c1', outputCol='_c11')
    # categoryTransfomer = categoryIndexer.fit(df)
    # df = categoryTransfomer.transform(df)
    # categoryIndexer2 = StringIndexer(inputCol='_c2', outputCol='_c22')
    # categoryTransfomer2 = categoryIndexer2.fit(df)
    # df = categoryTransfomer2.transform(df)

    assembler = VectorAssembler(inputCols=['_c0','_c1','_c2','_c3'], outputCol= "features")
    df_s=assembler.transform(df)
    train,test=df_s.randomSplit([0.8,0.2])
    train.cache()
    test.cache()
    res=train.groupby('label').count().toPandas()['count']
    train_s=train.sampleBy('label', {1:1,0:res[0]/res[1]},seed=234)
    for i in range(9):
        train_s=train_s.union(train.sampleBy('label', {1:1,0:res[0]/res[1]}))

    rf = RandomForestClassifier(numTrees=10, maxDepth=5)

    model=rf.fit(train_s)

    pred=model.transform(test)
    evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    auroc = evaluator.evaluate(pred)

    evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", metricName="areaUnderPR")
    aupr = evaluator.evaluate(pred)

    pred_pd=pred.toPandas()
    y_pred = np.array( [ np.array ( per_pd ) for per_pd in pred_pd['prediction'].values ] )
    y = np.array( [ np.array ( per_pd ) for per_pd in pred_pd['label'].values ] )
    print(sum([1 for x in y_pred if x==1.]))
    print(sum([1 for x in y if x==1.]))


    precision_score_res = precision_score(y, y_pred )
    print('precision_score_res:', precision_score_res)

    recall_score_res = recall_score(y, y_pred )
    print('recall_score_res:', recall_score_res)

    roc_curve_res = roc_curve(y, y_pred )
    print('roc_curve_res:', roc_curve_res)

    accuracy_score_res = accuracy_score(y, y_pred )
    print('accuracy_score_res:', accuracy_score_res)

    r2_score_res = r2_score(y, y_pred )
    print('r2_score_res:', r2_score_res)

    f1_score_res = f1_score(y, y_pred )
    print('f1_score_res:', f1_score_res)

    print("ROC:%f"%auroc)
    print("PR:%f"%aupr)


if __name__=='__main__':
#     df=get_train_data()
#     train(df)

    train_s,test=prepare_data()

    def hyperopt_train_test(params):
        rf=RandomForestClassifier(**params)
        model=rf.fit(train_s)
        pred=model.transform(test)

        # evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", metricName="areaUnderROC")
        # auroc = evaluator.evaluate(pred)

        pred_pd=pred.toPandas()
        y_pred = np.array( [ np.array ( per_pd ) for per_pd in pred_pd['prediction'].values ] )
        y = np.array( [ np.array ( per_pd ) for per_pd in pred_pd['label'].values ] )

        f1_score_res = f1_score(y, y_pred )

        return f1_score_res

    space4rf = {
        'maxDepth': hp.choice('maxDepth', range(1,20)),
        'maxBins': hp.choice('maxBins', range(8,100,8)),
        'numTrees': hp.choice('numTrees', range(1,20)),
        'impurity': hp.choice('impurity', ["gini", "entropy"]),
        'subsamplingRate': hp.choice('subsamplingRate', np.arange(1,0,-0.05)),
    }

    # space4lr = {
    #     'regParam': hp.choice('regParam', np.arange(1,0.5,-0.03)),
    #     'fitIntercept': hp.choice('fitIntercept', [True, False]),
    #     'elasticNetParam': hp.choice('elasticNetParam', np.arange(1,0,-0.05)),
    # }


    best = 0
    def f(params):
        global best
        acc = hyperopt_train_test(params)
        if acc > best:
            best = acc
        print('new best:', best, params)
        return {'loss': -acc, 'status': STATUS_OK}

    trials = Trials()
    best = fmin(f, space4rf, algo=tpe.suggest, max_evals=300, trials=trials)
    print('best:',best)

    parameters = ['maxDepth', 'maxBins', 'numTrees', 'impurity','subsamplingRate']
    f, axes = plt.subplots(nrows=1,ncols=5, figsize=(20,5))
    cmap = plt.cm.jet
    for i, val in enumerate(parameters):
        print(i, val)
        xs = np.array([t['misc']['vals'][val] for t in trials.trials]).ravel()
        ys = [-t['result']['loss'] for t in trials.trials]
        ys = np.array(ys)
        axes[i].scatter(xs, ys, s=20, linewidth=0.01, alpha=0.25, c=cmap(float(i)/len(parameters)))
        axes[i].set_title(val)

    plt.savefig('res.png')
