https://github.com/znxlwm/tensorflow-MNIST-GAN-DCGAN
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
# networks : generator
with tf.variable_scope('G'):
    z = tf.placeholder(tf.float32, shape=(None, 100))
    G_z = generator(z)

# networks : discriminator
with tf.variable_scope('D') as scope:
    drop_out = tf.placeholder(dtype=tf.float32, name='drop_out')
    x = tf.placeholder(tf.float32, shape=(None, 784))
    D_real = discriminator(x, drop_out)
    scope.reuse_variables()
    D_fake = discriminator(G_z, drop_out)
```
在 Tensorflow 当中有两种途径生成变量 variable, 一种是 tf.get_variable(), 另一种是 tf.Variable(). 如果在 tf.name_scope() 的框架下使用这两种方式,
使用 tf.Variable() 定义的时候, 虽然 name 都一样, 但是为了不重复变量名, Tensorflow 输出的变量名并不是一样的. 所以, 本质上 var2, var21, var22 并不是一样的变量. 而另一方面, 使用tf.get_variable()定义的变量不会被tf.name_scope()当中的名字所影响.
如果想要达到重复利用变量的效果, 我们就要使用 tf.variable_scope(), 并搭配 tf.get_variable() 这种方式产生和提取变量. 不像 tf.Variable() 每次都会产生新的变量, tf.get_variable() 如果遇到了同样名字的变量时, 它会单纯的提取这个同样名字的变量(避免产生新变量). 而在重复使用的时候, 一定要在代码中强调 scope.reuse_variables(), 否则系统将会报错, 以为你只是单纯的不小心重复使用到了一个变量.

2. loss函数定义
```angular2html
eps = 1e-2
D_loss = tf.reduce_mean(-tf.log(D_real + eps) - tf.log(1 - D_fake + eps))
G_loss = tf.reduce_mean(-tf.log(D_fake + eps))
```
上面我们对损失函数取负是因为它们需要最大化，而TensorFlow的优化器只能进行最小化。

3. 可训练参数定义
```angular2html
t_vars = tf.trainable_variables()
D_vars = [var for var in t_vars if 'D_' in var.name]
G_vars = [var for var in t_vars if 'G_' in var.name]
```

4. 优化目标定义
```angular2html
D_optim = tf.train.AdamOptimizer(lr).minimize(D_loss, var_list=D_vars)
G_optim = tf.train.AdamOptimizer(lr).minimize(G_loss, var_list=G_vars)
```

## 训练
1. 全局初始化
2. 两层循环
内部代码为：
```angular2html
# update discriminator
x_ = train_set[iter*batch_size:(iter+1)*batch_size]
z_ = np.random.normal(0, 1, (batch_size, 100))

loss_d_, _ = sess.run([D_loss, D_optim], {x: x_, z: z_, drop_out: 0.3})
D_losses.append(loss_d_)

# update generator
z_ = np.random.normal(0, 1, (batch_size, 100))
loss_g_, _ = sess.run([G_loss, G_optim], {z: z_, drop_out: 0.3})
G_losses.append(loss_g_)
```
sess.run 参数为优化参数[D_loss, D_optim]和最原始的输入{x: x_, z: z_, drop_out: 0.3}

## 可视化
1. 保存训练过程生成图

2. 保存训练过程loss曲线图