# 导入xq_user_stock ----
# 先在 mongodb 中运行以下代码，进行flatten操作，最终生成 r_user_stock
db.getCollection('xq_user_stock_updt').aggregate([
{"$project":{"count":1, "isPublic":1, "stocks":1, "user_id":1}},
{"$unwind":"$stocks"},
{"$project":{"_id":0, "user_id":"$user_id", "count":1, "isPublic":1, "code":"$stocks.code", "comment":{"$ifNull":["$stocks.comment", ""]}, 
"sellPrice":"$stocks.sellPrice", "buyPrice":"$stocks.buyPrice", "portfolioIds":"$stocks.portfolioIds",
"createAt":"$stocks.createAt", "targetPercent":"$stocks.targetPercent", "isNotice":"$stocks.isNotice",
"stockName":{"$ifNull":["$stocks.stockName",""]}, 
"exchange":{"$ifNull":["$stocks.exchange",""]},
"stockType":{"$ifNull":["$stocks.stockType",""]}}},
{"$out":"r_user_stock"}
], {"allowDiskUse":true})

# 然后再把flatten后的数据集 (r_user_stock) 导入R
library(mongolite)
conn <- mongo(collection = 'xq_user_stock', db = 'XQ-1806', url = "mongodb://localhost:27018")
iter <- conn$iterate(field = '{"_id":0, "count":1, "isPublic":1, "stocks":1, "user_id":1}')
r.user.stock.1803 <- data.table()



system.time({
    while (!is.null(res <- iter$batch(size = 1e6))) {
        chunk <- rbindlist(res, use.names = T, fill = T)
        r.user.stock.1806 <- rbindlist(list(r.user.stock.1806, chunk), use.names = T, fill = T)
    }
}) # 7min@1e6 (batch.size = 1e6 比 1e7要快，因为1e7会撑爆内存)



res <- iter$batch(size = 1e5)