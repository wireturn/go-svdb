# Welcome to go-svdb Project
=============

#Boquan Team
   The Boquan is a team dedicated to promoting and developing true bitcoin.
   The team has successfully developed:

    + Bitcoin mine pool mempool.com (https://pool.mempool.com), the mine pool supports BTC and BSV mining, and is currently the largest BSV mine in China.
    + Bitcoin wallet Lastpusre ( https://lastpurse.com ).

# Introduce
  go-svdb Project is a bitdb (https://bitdb.network/) Project. It implements a query service on bitcoin-sv.
  
  However, go-svdb does not copy any code from bitdb, and go-svdb is a software based on the golang.

  The purpose of the go-svdb project is that Bitdb did not meet the needs of the boquan team for the project.
  
  The go-svdb service is currently used in three projects of the boquan team:
  
   + mempool mining pool: pool.mempool.com, the Chinese largest bitcoin-sv mine pool.
   + LastPurse: lastpurse.com/ddpurse.com, the WeChat bsv wallet witch has most user on the world.
   + btc/bsv blockchain browser: bsv.mempool.com.

   It also works for BTC.
   
   Because a reliable, complete go-svdb service requires more servers, the next boquan team will launch a Metanet-based query service.

# Technical briefing
   Go-svdb mainly crawls data on the chain through three processes, block_crawler, output_crawler, and input_crawler.

   The input_crawler is designed to depend on the output_crawler, and the output_crawler depends on the block_crawler.

   Process division:
      + block_crawler crawls the header information of the block, so block_crawler crawls fastest;
      + output_crawler crawls the output of each transaction;
      + input_crawler organizes the transactions on the block into two tables: one is the address inventory table with the address as the index, and the other is the transaction table with the txid as the index;
      + monitored_addr_crawler takes business-related data to an acceleration table;
      + tx_listener listens for transactions in the memory pool.

   This way of crawling for crawling with only one process has several advantages:

   If you use only one process,or just get data block by block, then the database can not be aware of the new block in time.

   In the case of continuous new blocks, crawling a complete block takes a certain amount of time, so that the program cannot perceive the new block as early as possible.

   This can lead to some problems: for example, the orphon block can't be found immediately, or the browser fails to keep up with the block height in time. Svdb's block_crawler only crawls the head, which is extremely efficient and can solve the above problems on some level.

   In addition, due to the characteristics of the Bitcoin network, the full node only stores utxo, and does not care about the input of a particular transaction in history. Therefore, svdb first uses the output_crawler to quickly crawl all the output first so that input_crawler can crawls complete block, parsing the input of the transaction easier

   Finally, the input_crawler according to the data prepared by block_crawler and output_crawler, along the blockchain all the way from low to high, all the transactions on the chain are counted into two tables according to the address(addr_tx) and txid(tx).

   The block_crawler, output_crawler, and input_crawler processes establish a complete set of data on the chain. On this basis, the monitored_addr_crawler is for the business we need.

   Because the data on the entire blockchain is huge, in order to optimize the business of the Boquan team, we use another process to crawl the data from the table addr_tx and table tx, and the data of the business-related will be insert into another table.

   In addition, because the Bitcoin network has a new block in about 10 minutes, in order to obtain the corresponding transaction information as early as possible, and zero confirmation , the tx_listener process will continuously listen to all transactions in the entire node memory pool. Transactions that are already in the memory pool are first populated into the database by height equal -1.

   Go-svdb is currently able to query all data on the chain and correct the data if the data such as a lone block is rolled back.
# How to deploy
   At present, go-svdb is deployed in the Boquan team, which is deployed in 4 8 Core CPUs + 64G RAM + 4T DISK, two of which run BSV and two run BTC.

   The database uses Docker to run Mongo.

   The size of the hard drive grows with the size of the blockchain. It is recommended to use lvm technology.

   The operating system uses CentOS 7.x.

   When you run the blockchain data at full speed, you can basically take advantage of the eight-core CPU.
# Route Map
   1. Large block support, the current go-svdb internal version has been tested on the STN network.
   2. Unlimited capacity support, since go-svdb uses mongodb to store data.
   1. A public inquiry service based on Metanet.
   2. Support parse the Tokenized on blockchain.
# Permisson
   Go-svdb is based on the Open BSV license. Please read LINCENSE.
# License
   go-svdb is released under the terms of the Open BSV license. See LICENSE for more information.

