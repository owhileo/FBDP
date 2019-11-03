from matplotlib import pyplot as plt
import pandas as pd

n=10;
k=7;
path=r'clusteredInstances11/part-m-00000'

data=pd.read_csv(path,sep='[\t,]',header=None)
colors=['b','r','g','c','m','y','k','w','olive','orange']
for i in range(k):
    data_temp=data[data.iloc[:,2]==i+1]
    plt.scatter(data_temp.iloc[:,0],data_temp.iloc[:,1],color=colors[i])
plt.savefig("cluster_"+str(k)+"_"+str(n))
plt.show()

# k n out
#5 5 -
# 5 5 1
# 5 10 2
# 5 20 3
# 5 30 4
# 5 3 5
# 5 2 6
# 5 1 7
# 3 10 8
# 10 10 9
# 7 10 11
