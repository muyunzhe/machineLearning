https://github.com/znxlwm/tensorflow-MNIST-GAN-DCGAN
tf.layers.batch_normalization 
训练时必须指定参数training=True
分卷积：
conv2d_transpose(x, 1024, [4, 4], strides=(1, 1), padding='valid')
x:1*1*100，100是channel数
1024:卷积核数量
4,4：卷积核大小
输出 4*4*1024

## 获取数据
略

## 定义训练时变量
```angular2html
batch_size = 100
lr = 0.0002
train_epoch = 100

train_set = (mnist.train.images - 0.5) / 0.5  # normalization; range: -1 ~ 1
```

## 定义生成器判别器
cnn 反卷积与卷积

## 创建网络
1. 网络结构定义
```angular2html
# variables : input
x = tf.placeholder(tf.float32, shape=(None, 64, 64, 1))
z = tf.placeholder(tf.float32, shape=(None, 1, 1, 100))
isTrain = tf.placeholder(dtype=tf.bool)

# networks : generator
G_z = generator(z, isTrain)

# networks : discriminator
D_real, D_real_logits = discriminator(x, isTrain)
D_fake, D_fake_logits = discriminator(G_z, isTrain, reuse=True)
```

2. loss函数定义
```angular2html
D_loss_real = tf.reduce_mean(tf.nn.sigmoid_cross_entropy_with_logits(logits=D_real_logits, labels=tf.ones([batch_size, 1, 1, 1])))
D_loss_fake = tf.reduce_mean(tf.nn.sigmoid_cross_entropy_with_logits(logits=D_fake_logits, labels=tf.zeros([batch_size, 1, 1, 1])))
D_loss = D_loss_real + D_loss_fake
G_loss = tf.reduce_mean(tf.nn.sigmoid_cross_entropy_with_logits(logits=D_fake_logits, labels=tf.ones([batch_size, 1, 1, 1])))
```
上面我们对损失函数取负是因为它们需要最大化，而TensorFlow的优化器只能进行最小化。

3. 可训练参数定义
```angular2html
T_vars = tf.trainable_variables()
D_vars = [var for var in T_vars if var.name.startswith('discriminator')]
G_vars = [var for var in T_vars if var.name.startswith('generator')]
```

4. 优化目标定义
```angular2html
with tf.control_dependencies(tf.get_collection(tf.GraphKeys.UPDATE_OPS)):
    D_optim = tf.train.AdamOptimizer(lr, beta1=0.5).minimize(D_loss, var_list=D_vars)
    G_optim = tf.train.AdamOptimizer(lr, beta1=0.5).minimize(G_loss, var_list=G_vars)
```

## 训练
1. 全局初始化
2. 两层循环
内部代码为：
```angular2html
# update discriminator
x_ = train_set[iter*batch_size:(iter+1)*batch_size]
z_ = np.random.normal(0, 1, (batch_size, 1, 1, 100))

loss_d_, _ = sess.run([D_loss, D_optim], {x: x_, z: z_, isTrain: True})
D_losses.append(loss_d_)

# update generator
z_ = np.random.normal(0, 1, (batch_size, 1, 1, 100))
loss_g_, _ = sess.run([G_loss, G_optim], {z: z_, x: x_, isTrain: True})
G_losses.append(loss_g_)
```
sess.run 参数为优化参数[D_loss, D_optim]和最原始的输入{x: x_, z: z_, drop_out: 0.3}

## 可视化
1. 保存训练过程生成图

2. 保存训练过程loss曲线图