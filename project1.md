# project1 踩坑调试记录
这个project我写了三天大概，两天多的时间是调试花掉的。没有测试用例是真难debug呀，无耻的我已经上cout传到autograder来打印调试信息了。
下面讲一下遇到的坑
## task1 
实现`lru-k`算法,基本上没遇到坑,并发问题直接用大锁了.
唯一一点需要注意的是`evict`的实现中，多个距离同为`inf`的比较的是`history`中最老的那条访问记录谁更早。
## task2 
task2 的部分错误不少，但大多都是细节上的错误，比如没写回，`unpin`丢掉了之类的错误。
我觉得需要说的有一点很容易丢掉，就是`fetchpage`时，如果页面在pool里面但是`pincount`为0,此时需要加一且把`isevictable`设置为`false`，我在这里卡了不少时间.
## task3 
autograder上有一个test我一直是死锁，百度后发现网上大佬的解决方法是`page_guard`的上锁不要放在构造函数里，而是放在`fetchpageread`之类的这几个函数里。
修改代码后问题解决



