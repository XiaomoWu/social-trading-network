 # 本脚本用于将新抓取的数据合并至旧数据中
# 将新数据集添加 new 后缀，原有数据集 old 后缀
# 本次执行的任务是将1803合并至1709 (含1703)
# 注意！本次任务执行后生成的数据集（r-）具有重复的观测，将会在 02-filter中进行去重

# 合并cube.info ----
# 先载入新旧两个文件
ld(r.cube.info.mst.1709, T)
r.cube.info.old <- r.cube.info
ld(r.cube.info.1803, T)
r.cube.info.new <- r.cube.info
# 将新旧两个文件“竖着”拼接，即保留每一次的记录
r.cube.info <- rbindlist(list(r.cube.info.old, r.cube.info.new), use.names = T, fill = T) %>% unique()
# 更新 lastcrawl，其中早于2017-07，设置lastcrawl=1703，2017-07至2017-11，设置lastcrawl=1709，2017-11至2018-06，设置lastcrawl=1803；
r.cube.info.mst.1803 <- r.cube.info[, ":="(lastcrawl.date = as.Date(as.POSIXct(lastcrawl, origin = "1970-01-01")))
    ][, ":="(lastcrawl = ifelse(lastcrawl.date <= as.Date("2017-07-01"), 1703,
        ifelse(lastcrawl.date <= as.Date("2017-11-01"), 1709, 1803)))
    ][, ":="(lastcrawl.date = NULL)]
# 按照symbol和update.date排序，以后要调用时，直接r.cube.info[, .SD[1], keyby = .(cube.symbol)] 即可调用最新的记录
setorder(r.cube.info.mst.1803, cube.symbol, -update.date)
sv(r.cube.info.mst.1803)

# 合并cube.ret ----
# Must run in 211-Server!!!
# 我们从r.cube.ret.1803中提取日期大于 2017-07-01的观测，然后rbindlist至 r.cube.ret.mst.1709
ld(r.cube.ret.1803, T)
r.cube.ret.1803.append <- r.cube.ret.1803[date >= as.Date("2017-07-01")]
rm(r.cube.ret.1803)
ld(r.cube.ret.mst.1709, T)
# 如果保留 cube.type，那么内存就放不下了
#r.cube.ret.1803.append[, ':='(cube.type = NULL)]
#r.cube.ret.mst.1709[, ':='(cube.type = NULL)]

r.cube.ret.mst.1803 <- rbindlist(list(r.cube.ret.1803.append, r.cube.ret.mst.1709), use.names = T, fill = T) %>% unique(by = c('cube.symbol', 'date')) %>% setorder(cube.symbol, date)

rm(r.cube.ret.1803.append)
rm(r.cube.ret.mst.1709)
sv(r.cube.ret.mst.1803)

# 合并cube.rb ----
# 先载入新旧两个文件，旧数据为其添加 old 后缀
ld(r.cube.rb.1803, T)
ld(r.cube.rb.mst.1709, T)

# 将新旧两个文件“竖着”拼接，即保留每一次的记录
r.cube.rb.mst.1803 <- rbindlist(list(r.cube.rb.mst.1709, r.cube.rb.1803), use.names = T, fill = T)
rm(r.cube.rb.1803, r.cube.rb.mst.1709)

# 去重
r.cube.rb.mst.1803 <- unique(r.cube.rb.mst.1803, by = c("id", "rebalancing.id", "cube.symbol", "stock.symbol", "price", "created.at", "target.weight", "prev.weight.adjusted")) # distinct "id": 47461455

sv(r.cube.rb.mst.1803)

# 合并user.fans ----
# 先载入新旧两个文件，旧数据为其添加 old 后缀
ld(r.user.fans.1803, T)
ld(r.user.fans.mst.1709, T)

# mst.1709 的lastcrawl 是POSIXct，改成int（1709 or 1703）
r.user.fans.mst.1709[, ":="(last = ifelse(lastcrawl <= as.Date("2017-07-01"), 1703,
        ifelse(lastcrawl <= as.Date("2017-11-01"), 1709, 1803)))
        ][, ":="(lastcrawl = NULL)]
setnames(r.user.fans.mst.1709, "last", "lastcrawl")

# 将新旧两个文件“竖着”拼接，即保留每一次的记录
r.user.fans.mst.1803 <- rbindlist(list(r.user.fans.mst.1709, r.user.fans.1803), use.names = T, fill = T)
# 按照user.id和lastcrawl排序 ，以后要调用时，直接r.user.fans[, .SD[1], keyby = .(user.id)] 即可调用最新的记录
setkey(r.user.fans.mst.1803, user.id, lastcrawl)
sv(r.user.fans.mst.1803)

# 合并user.follow ----
# 先载入新旧两个文件
ld(r.user.follow.1803, T)
ld(r.user.follow.mst.1709, T)

# mst.1709 的lastcrawl 是date，改成int（1709 or 1703）
r.user.follow.mst.1709[, ":="(last = ifelse(lastcrawl <= as.Date("2017-07-01"), 1703,
        ifelse(lastcrawl <= as.Date("2017-11-01"), 1709, 1803)))
        ][, ":="(lastcrawl = NULL)]
setnames(r.user.follow.mst.1709, "last", "lastcrawl")

# 将新旧两个文件“竖着”拼接，即保留每一次的记录
r.user.follow.mst.1803 <- rbindlist(list(r.user.follow.mst.1709, r.user.follow.1803), use.names = T, fill = T)
# 按照user.id和lastcrawl排序 ，以后要调用时，直接r.user.follow[, .SD[1], keyby = .(user.id)] 即可调用最新的记录
setkey(r.user.follow.mst.1803, user.id, lastcrawl)
sv(r.user.follow.mst.1803)

# 合并user.info ----
# 先载入新旧两个文件
ld(r.user.info.mst.1709, T)
ld(r.user.info.1803, T)

# mst.1709 的lastcrawl 是date，改成int（1709 or 1703）
r.user.info.mst.1709[, ":="(last = ifelse(lastcrawl <= as.Date("2017-07-01"), 1703,
        ifelse(lastcrawl <= as.Date("2017-11-01"), 1709, 1803)))
        ][, ":="(lastcrawl = NULL)]
setnames(r.user.info.mst.1709, "last", "lastcrawl")

# 将新旧两个文件“竖着”拼接，即保留每一次的记录
r.user.info.mst.1803 <- rbindlist(list(r.user.info.mst.1709, r.user.info.1803), use.names = T, fill = T)
# 按照user.id和lastcrawl排序，以后要调用时，直接dt[, .SD[1], keyby = .(user.id, lastcrawl)] 即可调用最新的记录
setorder(r.user.info.mst.1803, user.id, lastcrawl)
sv(r.user.info.mst.1803)

# 合并user.info.weibo ----
# 先载入新旧两个文件
ld(r.user.info.weibo.mst.1709, T)
ld(r.user.info.weibo.1803, T)

# mst.1709 的lastcrawl 是date，改成int（1709 or 1703）
r.user.info.weibo.mst.1709[, ":="(last = ifelse(lastcrawl <= as.Date("2017-07-01"), 1703,
        ifelse(lastcrawl <= as.Date("2017-11-01"), 1709, 1803)))
        ][, ":="(lastcrawl = NULL)]
setnames(r.user.info.weibo.mst.1709, "last", "lastcrawl")

# 将新旧两个文件“竖着”拼接，即保留每一次的记录
r.user.info.weibo.mst.1803 <- rbindlist(list(r.user.info.weibo.mst.1709, r.user.info.weibo.1803), use.names = T, fill = T) %>% unique()
# 按照user.id和lastcrawl排序，以后要调用时，直接dt[, .SD[1], keyby = .(user.id, lastcrawl)] 即可调用最新的记录
setorder(r.user.info.weibo.mst.1803, user.id, lastcrawl)
sv(r.user.info.weibo.mst.1803)

# 合并user.stock ----
# 先载入新旧两个文件
ld(r.user.stock.mst.1709, T)
ld(r.user.stock.1803, T)

# mst.1709 的lastcrawl 是date，改成int（1709 or 1703）
r.user.stock.mst.1709[, ":="(last = ifelse(lastcrawl <= as.Date("2017-07-01"), 1703,
        ifelse(lastcrawl <= as.Date("2017-11-01"), 1709, 1803)))
        ][, ":="(lastcrawl = NULL)]
setnames(r.user.stock.mst.1709, "last", "lastcrawl")

# 将新旧两个文件“竖着”拼接，即保留每一次的记录
r.user.stock.mst.1803 <- rbindlist(list(r.user.stock.mst.1709, r.user.stock.1803), use.names = T, fill = T)
# 按照user.id和lastcrawl排序，以后要调用时，直接r.cube.stock[, .SD[1], keyby = .(cube.symbol)] 即可调用最新的记录
setorder(r.user.stock.mst.1803, user.id, lastcrawl)
sv(r.user.stock.mst.1803)

# 合并user.cmt ----
# 先载入新旧两个文件
ld(r.user.cmt.1709, T)
ld(r.user.cmt.1803, T)

# 将新旧两个文件“竖着”拼接
# 如果两条帖子的 id, title, text都一样，那么我们认为两条是一样的，只保留其一（使用unique函数）

# 注意！！！虽然这里是 r-prefix，但是经过了去重操作！！！

r.user.cmt.mst.1803 <- rbindlist(list(r.user.cmt.1709, r.user.cmt.1803), use.names = T, fill = T) %>% unique(by = c("id", "title", "text"))

setorder(r.user.cmt.mst.1803, id, lastcrawl)
sv(r.user.cmt.mst.1803)


