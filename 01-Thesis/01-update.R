# 本脚本用于将新抓取的数据合并至旧数据中
# 将新数据集添加 new 后缀，原有数据集 old 后缀
# 本次执行的任务是将2017-09合并至2017-03
# 注意！本次任务执行后生成的数据集（r-）具有重复的观测，将会在 02-filter中进行去重

# 合并cube.info ----
# 先载入新旧两个文件
ld(r.cube.info.old, T)
r.cube.info.old <- r.cube.info
ld(r.cube.info.new, T)
r.cube.info.new <- r.cube.info
# 将新旧两个文件“竖着”拼接，即保留每一次的记录
r.cube.info <- rbindlist(list(r.cube.info.old, r.cube.info.new), use.names = T, fill = T) %>% unique()
# 按照symbol和update.date排序，以后要调用时，直接r.cube.info[, .SD[.N], keyby = .(cube.symbol)] 即可调用最新的记录
setorder(r.cube.info, cube.symbol, update.date)
sv(r.cube.info)

# 合并cube.ret ----
# 先载入新旧两个文件，旧数据为其添加 old 后缀
ld(r.cube.ret.new, T)
r.cube.ret.new <- r.cube.ret
rm(r.cube.ret)
ld(r.cube.ret.old, T)
r.cube.ret.old <- r.cube.ret
rm(r.cube.ret)
# 将新旧两个文件“竖着”拼接，即保留每一次的记录
r.cube.ret <- rbindlist(list(r.cube.ret.old, r.cube.ret.new), use.names = T, fill = T) %>% unique() # 821047320 -> 468837556

setkey(r.cube.ret, cube.symbol, date)
sv(r.cube.ret

# 合并cube.rb ----
# 先载入新旧两个文件，旧数据为其添加 old 后缀
ld(r.cube.rb.new, T)
r.cube.rb.new <- r.cube.rb
rm(r.cube.rb)
ld(r.cube.rb.old, T)
r.cube.rb.old <- r.cube.rb
rm(r.cube.rb)
# 将新旧两个文件“竖着”拼接，即保留每一次的记录
r.cube.rb <- rbindlist(list(r.cube.rb.old, r.cube.rb.new), use.names = T, fill = T) %>% unique()
sv(r.cube.rb)

# 合并user.fans ----
# 先载入新旧两个文件，旧数据为其添加 old 后缀
ld(r.user.fans.new, T)
r.user.fans.new <- r.user.fans
rm(r.user.fans)
ld(r.user.fans.old, T)
r.user.fans.old <- r.user.fans
rm(r.user.fans)

# 将新旧两个文件“竖着”拼接，即保留每一次的记录
r.user.fans <- rbindlist(list(r.user.fans.old, r.user.fans.new), use.names = T, fill = T)
# 把lastcrawl转变成datetime
r.user.fans[lastcrawl >= 1e10, ":="(last = as.POSIXct(lastcrawl / 100, origin = "1970-01-01"))]
r.user.fans[lastcrawl <= 1e10, ":="(last = as.POSIXct(lastcrawl, origin = "1970-01-01"))]
r.user.fans[, ":="(lastcrawl = last)][, last := NULL]
# 按照user.id和lastcrawl排序 ，以后要调用时，直接r.user.fans[, .SD[.N], keyby = .(user.id)] 即可调用最新的记录
setkey(r.user.fans, user.id, lastcrawl)
sv(r.user.fans)

# 合并user.follow ----
# 先载入新旧两个文件，旧数据为其添加 old 后缀
ld(r.user.follow.new, T)
r.user.follow.new <- r.user.follow[, ":="(lastcrawl = as.Date("2017-10-01"))]
rm(r.user.follow)
ld(r.user.follow.old, T)
r.user.follow.old <- r.user.follow
rm(r.user.follow)
# 将新旧两个文件“竖着”拼接，即保留每一次的记录
r.user.follow <- rbindlist(list(r.user.follow.old, r.user.follow.new), use.names = T, fill = T)
# 按照user.id和lastcrawl排序 ，以后要调用时，直接r.user.follow[, .SD[.N], keyby = .(user.id)] 即可调用最新的记录
setkey(r.user.follow, user.id, lastcrawl)
sv(r.user.follow)

# 合并user.info ----
# 先载入新旧两个文件
ld(r.user.info.old, T)
r.user.info.old <- r.user.info[, lastcrawl := as.Date("2017-03-20")]
ld(r.user.info.new, T)
r.user.info.new <- r.user.info[, lastcrawl := as.Date("2017-10-01")]
rm(r.user.info)
# 将新旧两个文件“竖着”拼接，即保留每一次的记录
r.user.info <- rbindlist(list(r.user.info.old, r.user.info.new), use.names = T, fill = T)
# 按照user.id和lastcrawl排序，以后要调用时，直接dt[, .SD[.N], keyby = .(user.id, lastcrawl)] 即可调用最新的记录
setorder(r.user.info, user.id, lastcrawl)
sv(r.user.info)

# 合并user.info.weibo ----
# 先载入新旧两个文件
ld(r.user.info.weibo.old, T)
r.user.info.weibo.old <- r.user.info.weibo[, lastcrawl := as.Date("2017-03-20")]
ld(r.user.info.weibo.new, T)
r.user.info.weibo.new <- r.user.info.weibo[, lastcrawl := as.Date("2017-10-01")]
rm(r.user.info.weibo)
# 将新旧两个文件“竖着”拼接，即保留每一次的记录
r.user.info.weibo <- rbindlist(list(r.user.info.weibo.old, r.user.info.weibo.new), use.names = T, fill = T) %>% unique()
# 按照user.id和lastcrawl排序，以后要调用时，直接dt[, .SD[.N], keyby = .(user.id, lastcrawl)] 即可调用最新的记录
setorder(r.user.info.weibo, user.id, lastcrawl)
sv(r.user.info.weibo)

# 合并user.stock ----
# 先载入新旧两个文件
ld(r.user.stock.old, T)
r.user.stock.old <- r.user.stock[, lastcrawl := as.Date("2017-03-20")]
ld(r.user.stock.new, T)
r.user.stock.new <- r.user.stock[, lastcrawl := as.Date("2017-10-01")]
rm(r.user.stock)
# 将新旧两个文件“竖着”拼接，即保留每一次的记录
r.user.stock <- rbindlist(list(r.user.stock.old, r.user.stock.new), use.names = T, fill = T)
# 按照user.id和lastcrawl排序，以后要调用时，直接r.cube.stock[, .SD[.N], keyby = .(cube.symbol)] 即可调用最新的记录
setorder(r.user.stock, user.id, lastcrawl)
sv(r.user.stock)
