## flink搭建
### 下载
    [下载地址](https://flink.apache.org/downloads.html)
### 配置web界面端口
     vi conf/flink-conf.yaml #若端口占用则修改端口
     rest.port: 
### 启动
    ./bin/start-local.sh    #linux
    bin/start-local.bat     #windows点击
### 浏览器打开
    ip:port

## demo构建方式
### 下载项目原型
    curl https://flink.apache.org/q/quickstart.sh | bash
### 编写代码
    cd /src/main/java/org/myorg/quickstart/
    1.[SocketWindowWordCount.java](https://github.com/apache/flink/blob/master/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/socket/SocketWindowWordCount.java)替换掉StreamingJob.java,并将package 与className都修改为
    2.直接将SocketWindowWordCount.java放置该目录，并修改根目录下pom.xml中定义的mainClass
### 打开监听端口
    nc -l 9000
### 提交job运行
    ./flink run /root/software/flink/zero_test/batch-stream-demo/target/quickstart-0.1.jar --port 9000
    或者在web界面*Submit new job*，把jar包上传
### 日志输出
    在nc -l 9000的命令行输入字符串
    在web端*Job Manager*->*Stdout* 看日志输出


## kafka to flink
    当同时打开kafkaConsumer与flink接收同一topic数据时，都能收到
    所以可以实现告警与数据存储并发执行