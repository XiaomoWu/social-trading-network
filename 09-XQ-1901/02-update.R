# 本脚本用于将新抓取的数据合并至旧数据中
# 本次执行的任务是将1901合并至mst.1901
# 注意！本次任务执行后生成的数据集（r-）具有重复的观测，将会在 02-filter中进行去重

# 合并user.info ----
# 先载入新旧两个文件
ld(r.user.info.mst.1810, T)
ld(r.user.info.1901, T)

# 将新旧两个文件“竖着”拼接，即保留每一次的记录
r.user.info.mst.1901 <- rbindlist(list(r.user.info.mst.1810, r.user.info.1901), use.names = T, fill = T) %>% unique()

# 去重标准：no duplicates on (user.id, lastcrawl), if there is any, select obs with largest 'status.id'
r.user.info.mst.1901 <- r.user.info.mst.1901[order(user.id, lastcrawl, - last.status.id)][, .SD[1], keyby = .(user.id, lastcrawl)]

#key(r.user.info.mst.1810) # "user.id"   "lastcrawl"

sv(r.user.info.mst.1901)

# 合并user.info.weibo ----
# 先载入新旧两个文件
ld(r.user.info.weibo.mst.1810, T)
ld(r.user.info.weibo.1901, T)

# 将新旧两个文件“竖着”拼接，即保留每一次的记录
r.user.info.weibo.mst.1901 <- rbindlist(list(r.user.info.weibo.mst.1810, r.user.info.weibo.1901), use.names = T, fill = T) %>% unique()

# 去重
# no duplicates on `(user.id, weibo.id)`; if there is any, select obs with largest `lastcrawl`
r.user.info.weibo.mst.1901 <- r.user.info.weibo.mst.1901[order(user.id, weibo.id, - lastcrawl)][, .SD[1], keyby = .(user.id, weibo.id)]

# setkey
setkey(r.user.info.weibo.mst.1901, user.id)
sv(r.user.info.weibo.mst.1901)

# 合并user.stock ----
# 先载入新旧两个文件
ld(r.user.stock.mst.1810, T) # 1.3 min
ld(r.user.stock.1901, T) # 30 s

# 将新旧两个文件“竖着”拼接，即保留每一次的记录
r.user.stock.mst.1901 <- rbindlist(list(r.user.stock.mst.1810, r.user.stock.1901), use.names = T, fill = T) %>% unique()

# 去重
# No duplicates on (code, comment, sellPrice, buyPrice, portfolioIds, createAt, targetPercent, isNotice, exchange, user.id, is.public, stockName);
# if there is still duplicate, select obs with largest 'lastcrawl'
# 从1901开始，所有createAt 都是 0！！！！因此从1903开始，应该使用以下被注销的语句（如果那时createAt仍旧为0的话）
#r.user.stock.mst.1901 <- unique(r.user.stock.mst.1901)

r.user.stock.mst.1901 <- r.user.stock.mst.1901[order(code, comment, sellPrice, buyPrice, portfolioIds, createAt, targetPercent, isNotice, exchange, user.id, is.public, stockName, - lastcrawl)
    ][, .SD[1], keyby = .(code, comment, sellPrice, buyPrice, portfolioIds, createAt, targetPercent, isNotice, exchange, user.id, is.public, stockName)]

# setkey
setkey(r.user.stock.mst.1901, user.id, code, createAt)
sv(r.user.stock.mst.1901) # 2.64 min

# 合并user.fans ----
# 先载入新旧两个文件，旧数据为其添加 old 后缀
ld(r.user.fans.1901, T)
ld(r.user.fans.mst.1810, T) 

# 将新旧两个文件“竖着”拼接，即保留每一次的记录
r.user.fans.mst.1901 <- rbindlist(list(r.user.fans.mst.1810, r.user.fans.1901), use.names = T, fill = T)

# 去重
# no duplicates on .(user.id, lastcrawl), i.e., every 'lastcrawl' is a snapshot of the network
r.user.fans.mst.1901 <- unique(r.user.fans.mst.1901, by = c('user.id', 'lastcrawl'))
setkey(r.user.fans.mst.1901, user.id, lastcrawl)
sv(r.user.fans.mst.1901) # 1 min

# 合并user.follow ----
# 先载入新旧两个文件
ld(r.user.follow.1901, T)
ld(r.user.follow.mst.1810, T)

# 将新旧两个文件“竖着”拼接，即保留每一次的记录
r.user.follow.mst.1901 <- rbindlist(list(r.user.follow.mst.1810, r.user.follow.1901), use.names = T, fill = T)

# 去重
# no duplicates on .(user.id, lastcrawl), i.e., every 'lastcrawl' is a snapshot of the network
r.user.follow.mst.1901 <- unique(r.user.follow.mst.1901, by = c("user.id", "lastcrawl"))

# setkey
setkey(r.user.follow.mst.1901, user.id, lastcrawl)
sv(r.user.follow.mst.1901)

# 合并user.cmt ----
# 先载入新旧两个文件
olddir <- str_c(Sys.getenv('OneDrive'), '/STN/08-XQ-1810/Rdata/r.user.cmt.mst.1810.Rdata')
load(olddir)
ld(r.user.cmt.1901) # 3 min

# 将新旧两个文件“竖着”拼接
r.user.cmt.mst.1901 <- rbindlist(list(r.user.cmt.mst.1810, r.user.cmt.1901), use.names = T, fill = T)

rm(r.user.cmt.1901)
rm(r.user.cmt.mst.1810)

# 去重
# no duplicates on ("id", "user.id", "created.at", "title", "lastcrawl", "source", "comment.id", "retweet.status.id"
r.user.cmt.mst.1901 <- r.user.cmt.mst.1901[order(id, user.id, created.at, title, source, comment.id, retweet.status.id, - lastcrawl)][, .SD[1], keyby = .(id, user.id, created.at, title, source, comment.id, retweet.status.id)]

# setkey
setkey(r.user.cmt.mst.1901, user.id, created.at)
sv(r.user.cmt.mst.1901) # 7.63 min

# 合并cube.info ----
# 先载入新旧两个文件
ld(r.cube.info.mst.1810, T) # 15 s
ld(r.cube.info.1901, T) # 4 s

# 将新旧两个文件“竖着”拼接，即保留每一次的记录
r.cube.info.mst.1901 <- rbindlist(list(r.cube.info.mst.1810, r.cube.info.1901), use.names = T, fill = T) %>% unique(by = c("cube.symbol", "lastcrawl", "owner.id", "create.date", "update.date"))

# 去重操作
# If duplicate on (cube.symbol, lastcrawl), keep the line with largest update.date; if not, just keep it
# output: for each (cube.symbol, lastcrawl), will only have one obs
# 这种去重是很安全的，因为(cube.symbol, lastcrawl)相同的obs是极少的，他们都来自抓取的误差
r.cube.info.mst.1901 <- r.cube.info.mst.1901[order(cube.symbol, lastcrawl, - update.date)][, .SD[1], keyby = .(cube.symbol, lastcrawl)]

# setkey
setkey(r.cube.info.mst.1901, cube.symbol, lastcrawl)
sv(r.cube.info.mst.1901) # 17 s

# 合并cube.ret ----
ld(r.cube.ret.1901, T) # 5 min
ld(r.cube.ret.mst.1810, T) # 6 min

# 截取transaction set最近的数据
r.cube.ret.1901 <- r.cube.ret.1901[date > as.Date('2018-03-01')]

# append r-prefix to f.mst
r.cube.ret.mst.1901 <- rbindlist(list(r.cube.ret.1901, r.cube.ret.mst.1810), use.names = T, fill = T)
rm(r.cube.ret.1901)
rm(r.cube.ret.mst.1810)

# 去重
# If duplicate on (cube.symbol, date) but has different 'value', keep all these different values; else if duplicate on (cube.symbol, date, value), only keep row with latest lastcral
# Due to memory limit, I didn't chain the data.table syntax here.
# delete cube.type to reduce size, will re-generate it at last
r.cube.ret.mst.1901[, 'cube.type' := NULL]
r.cube.ret.mst.1901 <- r.cube.ret.mst.1901[order(cube.symbol, date, value, - lastcrawl), verbose = T]
r.cube.ret.mst.1901 <- r.cube.ret.mst.1901[, head(.SD, 1), keyby = .(cube.symbol, date, value), verbose = T]
r.cube.ret.mst.1901[, 'cube.type' := str_sub(cube.symbol, 1, 2)]

# setkey
setkey(r.cube.ret.mst.1901, cube.symbol, date)

sv(r.cube.ret.mst.1901) # 8 min

# 合并cube.rb ----

# 先载入新旧两个文件
ld(r.cube.rb.1901, T) # 1.2 min
ld(r.cube.rb.mst.1810, T) # 2.11 min
rm(r.cube.rb.1901, r.cube.rb.mst.1810) # 这步不能省，否则内存不足

# 将新旧两个文件“竖着”拼接，即保留每一次的记录
r.cube.rb.mst.1901 <- rbindlist(list(r.cube.rb.mst.1810, r.cube.rb.1901), use.names = T, fill = T)

# 去重
# should be no duplicates on ("id", "rebalancing.id", "cube.symbol", "stock.symbol", "price", "created.at", "target.weight", "prev.weight.adjusted"); if there is any, select the obs with largest lastcrawl
r.cube.rb.mst.1901 <- r.cube.rb.mst.1901[order(id, rebalancing.id, cube.symbol, stock.symbol, price, created.at, target.weight, prev.weight.adjusted, - lastcrawl)
    ][, .SD[1], keyby = .(id, rebalancing.id, cube.symbol, stock.symbol, price, created.at, target.weight, prev.weight.adjusted)]

# key(r.cube.rb.mst.1901) # (id, rebalancing.id, cube.symbol, stock.symbol, price, created.at, target.weight, prev.weight.adjusted)

sv(r.cube.rb.mst.1901) # 3.5 min

