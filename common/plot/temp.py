# library & dataset
import seaborn as sns

df = sns.load_dataset('iris')

# Make boxplot for one group only
sns.boxplot(y=df["sepal_length"])
sns.plt.show()


# # library & dataset
# import seaborn as sns
#
# df = sns.load_dataset('iris')
#
# sns.boxplot(x=df["species"], y=df["sepal_length"])
# sns.plt.show()


# #!/usr/bin/python
# # -*- encoding:utf-8 -*-
# # library & dataset
# import seaborn as sns
#
# df = sns.load_dataset('iris')
#
# sns.boxplot(data=df.ix[:, 0:2])
# sns.plt.show()


