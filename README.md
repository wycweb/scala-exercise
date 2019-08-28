# 项目说明
大数据学习，练习大数据处理技巧

# 项目运行环境
    centos:release-7-6
    java:1.8
    scala:2.11.8
    spark:2.3.3
    hadoop:3.1.2
    hive:3.1.1
    maven:3.5.4
    
# 运行说明
* **配置host**
    * 192.168.199.243   bigdata
* **运行环境中出现的域名说明**
    * maven私服：使用阿里云服务器搭建的maven私服，地址：maven.wycweb.com，纯属个人玩玩
    * 项目中出现的bigdata域名为本地配置的host，个人学习在用一台旧电脑搭建的单节点环境(hadoop配置、hive配置)以及本机运行时中使用的hostname都为bigdata
* **idea开发、测试说明**
    * 项目中spark运行配置为yarn提交，在本地测试过程中，可将Run/Debug Configurations中对 配置VM options为：
    >-Dspark.master=local[10]
    * 在集群环境中由于添加了scala、spark相关包，本地运行时，在Run/Debug Configurations中对 Include dependencies with "Provide" scope设置为选中状态