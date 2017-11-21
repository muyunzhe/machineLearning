
## 获取数据
本地有mnist数据，直接读取就可以了
```
from tensorflow.examples.tutorials.mnist import input_data
mnist = input_data.read_data_sets("本地路径", one_hot=True)
```

## 定义训练时变量
```angular2html
batch_size = 100
lr = 0.0002
train_epoch = 100

train_set = (mnist.train.images - 0.5) / 0.5  # normalization; range: -1 ~ 1
```

## 定义生成器判别器
全连接，暂时没用cnn

## 创建网络
1. 网络结构定义
```angular2html
# Combined network
discriminator.trainable = False
ganInput = Input(shape=(randomDim,))
x = generator(ganInput)
ganOutput = discriminator(x)
gan = Model(inputs=ganInput, outputs=ganOutput)
gan.compile(loss='binary_crossentropy', optimizer=adam)
```
核心就是定义Model以及compile方法

2. loss函数定义

3. 优化目标定义
```angular2html
discriminator.compile(loss='binary_crossentropy', optimizer=adam)
```
一句话就搞定了

## 训练
1. 全局初始化
2. 两层循环
内部代码为：
```angular2html
# Get a random set of input noise and images
noise = np.random.normal(0, 1, size=[batchSize, randomDim])
imageBatch = X_train[np.random.randint(0, X_train.shape[0], size=batchSize)]

# Generate fake MNIST images
generatedImages = generator.predict(noise)
# print np.shape(imageBatch), np.shape(generatedImages)
X = np.concatenate([imageBatch, generatedImages])

# Labels for generated and real data
yDis = np.zeros(2*batchSize)
# One-sided label smoothing
yDis[:batchSize] = 0.9

# Train discriminator
discriminator.trainable = True
dloss = discriminator.train_on_batch(X, yDis)

# Train generator
noise = np.random.normal(0, 1, size=[batchSize, randomDim])
yGen = np.ones(batchSize)
discriminator.trainable = False
gloss = gan.train_on_batch(noise, yGen)
```
先创建generatedImages和train 数据，其中generatedImages = generator.predict(noise)，也就是生成器按照noise创造出来的一个图片。
把这两部分数据已经他们的标签放入判别器，训练判别器
将noise 放入生成器，训练生成式。

## 可视化
1. 保存训练过程生成图

2. 保存训练过程loss曲线图