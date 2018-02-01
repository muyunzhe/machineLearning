# Item2Vec

## 简介

#### Item2Vec是一种将商品进行向量化的操作，利用神经网络构建商品维度的特征，减少人工建特征的工作量和思维盲区。以下是一组对比：

#### 1.人工构造特征：


| Item | 金额 |票级别|机票类型|是否需要行程单 |清洗机票类型 |
| ----- | ----- |----- |----- |----- |----- |
| f_上海 to 深圳 | 1000| 1| 2| 1| 1|


#### 2.机器构造特征：

| Item | Feature1 |Feature2|Feature3|Feature4|Feature5|
| ----- | ----- |----- |----- |----- |----- |
| f_上海 to 深圳 | 0.123| -0.234| 0.223| 0.567| -0.643|



## 从Word2Vec到Item2Vec

#### Item2Vec是基于Word2Vec的思想构建的，Word2Vec是一种基于词的上下文来构建词与词关系的模型，通过这种模型，词可以转化成向量。Item2Vec参考了Word2Vec的方式，利用用户下单商品之间的关系构建模型，让商品得到了向量化的表达。

### 1.Word2Vec:
#### 在上下文中，相似的词在空间的距离会比较短。
![Rendering preferences pane](pict/word_relation.png)



### 2.Item2Vec:
#### 在下单顺序中，相近的单子相似度会比较高。
![Rendering preferences pane](pict/Flight_relation.png)


## Item2Vec的应用

### 1.丰富特征.
#### 1.1 A卡模型.将用户授信前的ItemVector进行用户维度聚合，以增加用户的信息维度。
```
模型:Q端拿去花A卡
KS:有1%个点的提升
('AUC:82.47%', 'KS:49.72') -- 1985维用户特征
('AUC:82.95%', 'KS:50.64') -- 1985维用户特征 + 192维Item Vector(平均聚合)

变量筛选:模型筛选后，只剩下334个特征，其中有82个特征来自于Item Vector

```
#### <mark>结论：虽然模型效果提升不大，但Item Vector已经可以对部分已有用户特征进行互补和替代，可减少人工建特征的工作量。<mark>


#### 1.2 订单维度模型.将ItemVector加入到每次订单的特征集合中，以增加信息维度。同时，还可以根据下单的顺序构建RNN模型。


### 2.推荐系统.
##### 以网易云音乐为例，利用大量用户听歌的顺序构建推荐模型，为用户推送可能喜欢听的歌曲。

![Rendering preferences pane](pict/music_recommendation.png)


## 代码实现

### [main_code](main_code/).