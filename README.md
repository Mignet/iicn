# iicn
基于mapreduce的中文倒排索引简单实现

iich.go 负责读取文章，分词，并利用mapreduce建立倒排索引。然后将索引存入leveldb
httpserver.go 负责提供搜索服务，查询leveldb
