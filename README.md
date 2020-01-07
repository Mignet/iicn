# iicn
基于mapreduce的中文倒排索引简单实现

## 使用前说明
我使用的Go版本是go1.13.5
我使用了go mod来管理依赖

## 使用说明
> 1.做分词并生成倒排索引并写入db
  执行>``go run iich.go master sequential pg-ch01.txt pg-ch02.txt``

> 2.启动web服务
  执行>``go run server.go``

## 更新历史
### v1.1
将lvevldb换为boltdb
### v1.0
iich.go 负责读取文章，分词，并利用mapreduce建立倒排索引。然后将索引存入leveldb
httpserver.go 负责提供搜索服务，查询leveldb

