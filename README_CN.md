# 欢迎关注 go-svdb 项目
------

# 博泉团队
  博泉团队（Boquan Team）是一个位于致力于推动和发展真正的比特币的团队。
  目前团队成功开发了：
  + 比特币矿池 mempool.com ( https://pool.mempool.com ), 矿池支持BTC和BSV挖矿，目前是中国最大的BSV矿池。
  + 比特币钱包 Lastpusre ( https://lastpurse.com )。
  
# 简介
  go-svdb 项目是一个有点类似 bitdb (https://bitdb.network/) 项目，它实现了一个bitcoin-sv上的查询服务。同时它也支持BTC。
  
  可是go-svdb并没有抄袭bitdb的任何代码，go-svdb是基于go语言从0开始构建的。
  
  go-svdb项目成立的目的是：bitdb并不能满足博泉团队对项目的需求。
  
  目前go-svdb服务已经用于boquan团队的两个三个项目中：
  
  + mempool矿池：pool.mempool.com，中文世界最大的bitcoin-sv矿池。
  + 打点钱包：lastpurse.com/ddpurse.com，世界用户量第一的微信bsv钱包。
  + btc/bsv区块链浏览器：bsv.mempool.com。

# 技术简报
  go-svdb主要通过三个进程，block_crawler、output_crawler、input_crawler来对链上数据进行爬取。

  设计上input_crawler依赖于output_crawler，output_crawler依赖于block_crawler。

  进程分工：
   + block_crawler爬取区块的头部信息，所以block_crawler爬取的速度最快；
   + output_crawler爬取每一笔交易的输出；
   + input_crawler 将区块上的交易整理成两张表: 一是以地址为核心的地址流水表，二是以交易为核心的交易表；
   + monitored_addr_crawler 将业务相关的数据拿到一个加速表里；
   + tx_listener 监听内存池中的交易。

  这种爬取方式针对于只使用一个进程进行爬取具有以下几个优点：

  如果只是使用一个进程，或者只是一个块一个块的抓取，那么数据库就不能及时的感知到出块。

  在连续出块的情况下，爬取一个完整的区块会消耗一定的时间，使得程序不能尽早的感知到新出的块。

  这会导致一些问题：比如孤块未能发现，比如浏览器未能及时跟上区块高度。svdb的block_crawler只爬取头部，从而其效率极高，能够在一定程度上解决上述问题。

  另外，由于比特币网络的特点，全节点只存储utxo，并不关心历史上某一个特定的交易的输入，所以svdb首先采用output_crawler较为快速的先将所有的输出爬取一遍，为后续input_crawler爬取完整的区块，解析交易的输入扫平道路。

  最后是input_crawler根据block_crawler、output_crawler准备的数据，沿着区块链一路从低到高，将链上的所有交易按照地址和txid分别统计成两张表。

  block_crawler、output_crawler、input_crawler三个进程建立起一个完整的链上数据集合，在此基础之上，monitored_addr_crawler则是为了我们需要的业务。

  因为整个区块链上的数据是及其庞大的，为了能个针对博泉团队的业务进行优化，我们采用另外一个进程来将input_crawler爬取到的数据中，业务相关的地址的数据重爬取到另外一张表上。

  另外，由于比特币网络约10分钟出一个块，为了能尽早的获取到相应的交易信息，达到零确认，tx_listener进程会不断的监听全节点内存池中的所有交易。将已经在内存池中的交易先按照高度-1填充到数据库中。

  go-svdb目前能够查询链上的所有数据，并能在孤块 等数据回退的情况下修正相应的数据。
  
# 部署方法
  目前go-svdb在博泉团队中，被部署在4台 8 Core CPU + 64G RAM + 4T DISK，其中两台跑BSV，两台跑BTC。

  数据库使用Docker 来跑Mongo。

  硬盘的大小随着区块链的大小不断增长，建议使用lvm技术。

  操作系统使用 CentOS 7.x。

  全速运行抓取区块链数据的时候，基本可以把八核心的CPU跑满。

# 路线图
  1. 大区块的支持，目前go-svdb内部版本已经在STN网络上测试。
  2. 无限扩容支持，由于go-svdb使用mongodb来存储数据。
  1. 基于Metanet的公开查询服务。
  2. Tokenized支持，可以解析区块链。

# 许可证
  go-svdb 基于Open BSV的许可证.请阅读LINCENSE。 

# 联系方式
  E-mail: James Chen (toorq@hotmail.com or cjq@boquaninc.com)