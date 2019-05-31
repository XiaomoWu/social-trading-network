# 本脚本用于将新抓取的数据合并至旧数据中
# 本次执行的任务是将1806合并至mst.1803
# 注意！本次任务执行后生成的数据集（r-）具有重复的观测，将会在 02-filter中进行去重

# 合并user.info ----
# 先载入新旧两个文件
ld(r.user.info.mst.1803, T)
ld(r.user.info.1806, T)

# 将新旧两个文件“竖着”拼接，即保留每一次的记录
r.user.info.mst.1806 <- rbindlist(list(r.user.info.mst.1803, r.user.info.1806), use.names = T, fill = T) %>% unique()
# 按照user.id和lastcrawl排序，以后要调用时，直接dt[, .SD[1], keyby = .(user.id, lastcrawl)] 即可调用最新的记录
setorder(r.user.info.mst.1806, user.id, -lastcrawl)
sv(r.user.info.mst.1806)

# 合并user.stock ----
# 先载入新旧两个文件
ld(r.user.stock.mst.1803, T)
ld(r.user.stock.1806, T)

# 将新旧两个文件“竖着”拼接，即保留每一次的记录
r.user.stock.mst.1806 <- rbindlist(list(r.user.stock.mst.1803, r.user.stock.1806), use.names = T, fill = T) %>% unique()
# 按照user.id和lastcrawl排序，以后要调用时，直接r.cube.stock[, .SD[1], keyby = .(cube.symbol)] 即可调用最新的记录
setorder(r.user.stock.mst.1806, user.id, code, -lastcrawl)
sv(r.user.stock.mst.1806)

# 合并user.fans ----
# 先载入新旧两个文件，旧数据为其添加 old 后缀
ld(r.user.fans.1806, T)
ld(r.user.fans.mst.1803, T)

# 将新旧两个文件“竖着”拼接，即保留每一次的记录
r.user.fans.mst.1806 <- rbindlist(list(r.user.fans.mst.1803, r.user.fans.1806), use.names = T, fill = T) %>% unique(by = c("user.id", "fans.count", "anonymous.fans.count", "lastcrawl"))
# 按照user.id和lastcrawl排序 ，以后要调用时，直接r.user.fans[, .SD[1], keyby = .(user.id)] 即可调用最新的记录
setorder(r.user.fans.mst.1806, user.id, -lastcrawl)
sv(r.user.fans.mst.1806)

# 合并user.follow ----
# 先载入新旧两个文件
ld(r.user.follow.1806, T)
ld(r.user.follow.mst.1803, T)

# 将新旧两个文件“竖着”拼接，即保留每一次的记录
r.user.follow.mst.1806 <- rbindlist(list(r.user.follow.mst.1803, r.user.follow.1806), use.names = T, fill = T) %>% unique(by = c("user.id", "lastcrawl"))
# 按照user.id和lastcrawl排序 ，以后要调用时，直接r.user.follow[, .SD[1], keyby = .(user.id)] 即可调用最新的记录
setorder(r.user.follow.mst.1806, user.id, -lastcrawl)
sv(r.user.follow.mst.1806)

# 合并user.info.weibo ----
# 先载入新旧两个文件
ld(r.user.info.weibo.mst.1803, T)
ld(r.user.info.weibo.1806, T)

# 将新旧两个文件“竖着”拼接，即保留每一次的记录
r.user.info.weibo.mst.1806 <- rbindlist(list(r.user.info.weibo.mst.1803, r.user.info.weibo.1806), use.names = T, fill = T) %>% unique()
# 按照user.id和lastcrawl排序，以后要调用时，直接dt[, .SD[1], keyby = .(user.id, lastcrawl)] 即可调用最新的记录
setorder(r.user.info.weibo.mst.1806, user.id, -lastcrawl)
sv(r.user.info.weibo.mst.1806)

# 合并user.cmt ----
# 先载入新旧两个文件
ld(r.user.cmt.mst.1803, T)
ld(r.user.cmt.1806, T)

# 将新旧两个文件“竖着”拼接
r.user.cmt.mst.1806 <- rbindlist(list(r.user.cmt.mst.1803, r.user.cmt.1806), use.names = T, fill = T) %>% unique(by = c("id", "user.id", "created.at", "title", "text", "lastcrawl", "source", "comment.id", "retweet.status.id"))

setorder(r.user.cmt.mst.1806, id, -lastcrawl)
sv(r.user.cmt.mst.1806)

# 合并cube.info ----
# 先载入新旧两个文件
ld(r.cube.info.mst.1803, T)
ld(r.cube.info.1806, T)
r.cube.info.old <- r.cube.info.mst.1803
r.cube.info.new <- r.cube.info.1806
rm(r.cube.info.mst.1803)
rm(r.cube.info.1806)
# 将新旧两个文件“竖着”拼接，即保留每一次的记录
r.cube.info.mst.1806 <- rbindlist(list(r.cube.info.old, r.cube.info.new), use.names = T, fill = T) %>% unique(by = c("cube.symbol", "lastcrawl", "owner.id", "create.date", "update.date"))
# 按照symbol和update.date排序，以后要调用时，直接r.cube.info[, .SD[1], keyby = .(cube.symbol)] 即可调用最新的记录
setorder(r.cube.info.mst.1806, cube.symbol, - lastcrawl)
sv(r.cube.info.mst.1806)

# 合并cube.ret ----
ld(r.cube.ret.1806, T)
ld(r.cube.ret.mst.1803, T)

r.cube.ret.mst.1806 <- rbindlist(list(r.cube.ret.1806, r.cube.ret.mst.1803), use.names = T, fill = T)
rm(r.cube.ret.1806)
rm(r.cube.ret.mst.1803)

r.cube.ret.mst.1806 <- unique(r.cube.ret.mst.1806, by = c('cube.symbol', 'date', "lastcrawl"))
sv(r.cube.ret.mst.1806)

# 合并cube.rb ----
# 先载入新旧两个文件
ld(r.cube.rb.1806, T)
ld(r.cube.rb.mst.1803, T)

# 将新旧两个文件“竖着”拼接，即保留每一次的记录
r.cube.rb.mst.1806 <- rbindlist(list(r.cube.rb.mst.1803, r.cube.rb.1806), use.names = T, fill = T)
rm(r.cube.rb.1806, r.cube.rb.mst.1803)

# 去重
r.cube.rb.mst.1806 <- unique(r.cube.rb.mst.1806, by = c("id", "rebalancing.id", "cube.symbol", "stock.symbol", "price", "created.at", "target.weight", "prev.weight.adjusted", "lastcrawl")) 

sv(r.cube.rb.mst.1806)

