"# realtime-data-automatic-landing" 

"Real time data automatic landing based on spark streaming" 

"基于SparkStreaming的实时数据自动化落地"

"实现思路"

"1: 约定数据发送格式"

"2: 将元数据的表结构存储到分布式缓存中"

"3: 发送数据中的字段和元数据中表结构进行对比,同时更新分布式缓存"

"4: 对比出来差异的表字段，实时自动化更新元数据的表结构"

"5: 根据元数据最新的表结构，构建sql，实时写入数据库"

