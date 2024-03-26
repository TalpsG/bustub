# 坑
环境配置和`autograder`还是有一些坑的，下面讲一下我遇到的坑。
## 项目的工程
大家不要克隆`github`上最新的工程，要去`release`里找一下找到和你`autograder`相同学期的代码下载下来。
最新的代码里有一些东西是改了的，可能会导致`autograder`得零分，修改起来很麻烦。

## 环境
我写了一个`dockerfile`，大家不想用虚拟机可以用`docker`来做这个课程实验，不过我之后没有用。我发现我在我的`arch`上跑也没问题，就没再换了。
```dockerfile
FROM ubuntu:22.04
CMD bash

# Install Ubuntu packages.
# Please add packages in alphabetical order.
ARG DEBIAN_FRONTEND=noninteractive
RUN sed -i 's/archive.ubuntu.com/mirrors.aliyun.com/g' /etc/apt/sources.list
RUN sed -i 's/security.ubuntu.com/mirrors.aliyun.com/g' /etc/apt/sources.list
RUN apt-get -y update && \
    apt-get -y install \
      build-essential \
      clang-14 \
      clang-format-14 \
      clang-tidy-14 \
      cmake \
      doxygen \
      git \
      pkg-config \
      zlib1g-dev \
      libelf-dev \
      libdwarf-dev \
      vim \
      zip \
```
在该文件的目录下执行`docker build . -t bustub`然后等运行完景象就做好了。
之后运行`docker run --name database -itd bustub:latest`后台就运行了该容器,此后每次启动容器就用`docker start database`就可以了。
现在容器已经启动了，我们执行`docker exec -it database /bin/bash`就进入`shell`了
用完记得关闭`docker stop database`.




# copy-on-write trie
实现`copy-on-write`的`trie`树。

需要实现三个函数
1. `Get`
2. `Put`
3. `Remove`

## Get
最简单的一个函数，只需要查找然后`dynamic_cast`转换后返回即可


## Put
由于我们代码中的关于节点的指针都是`const`的，因此想要修改节点必须重新创建，这里注释中写的也很清楚。


下面说下思路：
我们是从上到下查找下来的，所以可以从上到下复制节点，直到查找到要插入的位置或者查找到目标节点存在的位置。此时还是需要新建节点然后替换或者添加到我们新建的树中。

具体一点，我们使用父节点来判断是否要在`children`上添加节点。(其实可以先大概写一些，然后去跑`test`看看哪里没过，调试的时候就会懂这棵树的规则了)

## Remove
`Remove`也要返回新的树，我们也需要新建节点来实现。

思路如下:
可以自下而上的进行复制，删除完节点后需要回到上一层判断之前新建的节点是否需要删除，也就是从上到下后还要从下返回到上.

# concurrent 
这节很简单，借助上一节实现的三个函数，读一读`get`注释里的伪码高低就明白了.

# debug
这一节本地我的答案和`autograder`的答案不一样，直接告诉大家吧，`7 2 30`,反正这个只要会`gdb`调一下立马就出来了。

# sql
注册一个`sql`函数，难度也不大。
我也不太懂程序是如何调用这个实现的，我感觉是在`plan_func_call`文件里面创建一个`string_expression`里的对象，这个对象封装着我们的函数以及函数的参数。然后之后在某个位置调用了我们填写的大小写转换的那个成员函数实现的该功能。


