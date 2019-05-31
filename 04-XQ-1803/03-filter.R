# 必须要按顺序执行，不能当中跳过！
# 除了 cube.ret，其余都可以在211-PC上运行
# 本脚本用于对 r-prefix.mst（截至1803）进行去重、清洗（例如剔除存续期小于阈值的cube）。最终生成的 f-prefix 用于project-JEP

# 生成cid，用于对cube类进行剔除 ----
ld(r.cube.info.mst.1803)
# 0. cube.symbol有重复（因为包含了两次抓取的记录），选择lastcrawl大的
cube.info <- r.cube.info.mst.1803[order(cube.symbol, -lastcrawl)
    ][, .SD[1], keyby = .(cube.symbol)]
rm(r.cube.info.mst.1803)

# 1. r.cube.info: market = 'cn'
cid.cn <- cube.info[market == 'cn', unique(cube.symbol)]
sv(cid.cn) # 1,174,004 cubes

# 2. abnormal return
# 净值最低的1%以及净值超过150的剔除
cid.abret <- cube.info[net.value %between% c(quantile(net.value, 0.01), 150), unique(cube.symbol)]
sv(cid.abret)

# 3. exists in cube.rb
# r.cube.rb已经经过去重处理，不需要再去重。补充信息如下：
#> uniqueN(r.cube.rb.mst.1803, by = c("cube.symbol"))
#[1] 1261068
#> uniqueN(r.cube.rb.mst.1803, by = c("cube.symbol", "stock.id", "created.at"))
#[1] 47430187
#> uniqueN(r.cube.rb.mst.1803, by = c("id"))
#[1] 46538666
#> uniqueN(r.cube.rb.mst.1803)
#[1] 47461455 == nrow(r.cube.rb.mst.1803)
ld(r.cube.rb.mst.1803)
cid.rb <- r.cube.rb.mst.1803[, unique(cube.symbol)]
sv(cid.rb)

# 4. exists in cube.ret
ld(r.cube.ret.mst.1803)
cid.ret <- r.cube.ret.mst.1803[, unique(cube.symbol)]
sv(cid.ret)
rm(r.cube.ret.mst.1803)

# 5. 使用上述生成的cid.abret, cid.cn, cid.rb, cid.ret生成最终的cid
ld(cid.abret)
ld(cid.cn)
ld(cid.ret)
ld(cid.rb)
cid <- intersect(cid.abret, cid.cn) %>% intersect(cid.rb) %>% intersect(cid.ret)
sv(cid)


# 生成uid，用于对user类进行剔除 ----
# 1. 只有cid的owner才可能称为uid
uid.cidowner <- cube.info[cube.symbol %in% cid, unique(owner.id)]
sv(uid.cidowner)

# 2. exists in r.user.info
# r.user.info中的user.id有重复，选择lastcrawl大的那个
ld(r.user.info.mst.1803)
user.info <- r.user.info.mst.1803[order(user.id, - lastcrawl)][, .SD[1], keyby = .(user.id)]
uid.userinfo <- user.info[, unique(user.id)]
sv(uid.userinfo)
rm(r.user.info.mst.1803)

# 3. exists in r.user.stock
# r.user.stock需要去重，其主键是 （user.id, code, createAt）。在主键相同的情况下，选择lastcrawl大的
# 注意，stockName是不准的，因为公司可能改名，但还是同一家公司（stockName不同但code相同）。补充信息如下：
# uniqueN(r.user.stock.mst.1803, by = c("user.id", "code", "createAt")) # 33378799
ld(r.user.stock.mst.1803)
user.stock <- r.user.stock.mst.1803[order(user.id, code, createAt, - lastcrawl)][, .SD[1], keyby = .(user.id, code, createAt)]
rm(r.user.stock.mst.1803)
uid.userstock <- user.stock[, unique(user.id)]
sv(uid.userstock)

# 使用上述生成的uid.cidowner, uid.userinfo生成最终的uid
ld(uid.cidowner)
ld(uid.userinfo)
ld(uid.userstock)
uid <- intersect(uid.cidowner, uid.userinfo) %>% intersect(uid.userstock)
sv(uid)

# 使用cid和uid生成 f-prefix ----
# 生成cube.info
# 所有fans.count减1
f.cube.info.mst.1803 <- cube.info[cube.symbol %in% cid][, ":="(fans.count = (fans.count - 1))]
rm(f.cube.info.mst.1803, cube.info)

# 生成f.cube.rb
ld(r.cube.rb.mst.1803)
f.cube.rb.mst.1803 <- r.cube.rb.mst.1803[cube.symbol %in% cid
    ][, ":="(prev.weight.adjusted = as.numeric(prev.weight.adjusted))]
rm(r.cube.rb.mst.1803)
# target.weight与prev.weight只能是NA或者是[0,110]之间的数
# 有时不知什么原因，prev.weight可能略微超过100，这时不算错误
f.cube.rb.mst.1803 <- f.cube.rb.mst.1803[(is.na(target.weight) | target.weight %between% c(0, 110)) & (is.na(prev.weight.adjusted) | prev.weight.adjusted %between% c(0, 110))]
rm(f.cube.rb.mst.1803)

# 生成user.info
f.user.info.mst.1803 <- user.info[user.id %in% uid]
sv(f.user.info.mst.1803)
rm(user.info, f.user.info.mst.1803)

# 生成user.stock
f.user.stock.mst.1803 <- user.stock[user.id %in% uid]
f.user.stock.mst.1803 <- f.user.stock.mst.1803[, .(user.id, stock.symbol = code, cube.type = exchange, create.date = as.Date(as.POSIXct(createAt / 1000, origin = "1970-01-01")), buy.price = buyPrice, sell.price = sellPrice, is.notice = isNotice, target.percent = targetPercent)]
sv(f.user.stock.mst.1803)
rm(user.stock, f.user.stock.mst.1803)

# 生成f.cube.ret 
# 由于 cube.ret 实在太大，只能在211-Server中处理
ld(r.cube.ret.mst.1803)
f.cube.ret.mst.1803 <- r.cube.ret.mst.1803[cube.symbol %in% cid
    ][order(cube.symbol, date)]
rm(r.cube.ret.mst.1803)
# 考虑 quit / re-enter, 对f.cube.ret进行进一步剔除
# life定义为第一笔至最后一笔之间的时间, f.cubelife包含每个组合的起讫时间以及天数
# life至少大于1
ld(f.cube.rb.mst.1803)
f.cubelife.mst.1803 <- f.cube.rb.mst.1803[, .(start = as.IDate(min(created.at)), end = as.IDate(max(created.at)), trade.n = .N), keyby = .(cube.symbol)
    ][, ":="(life = as.integer(end - start))
    ][life >= 1]
# cid.1day: cube.symbols that last for more than one day
cid.1day <- unique(f.cubelife.mst.1803$cube.symbol)
sv(f.cubelife.mst.1803)
sv(cid.1day)

# 把 life>=1 这个条件应用于 f.cube.info, f.cube.rb ----
ld(cid.1day)
ld(f.cube.info.mst.1803)
ld(f.cube.rb.mst.1803)

f.cube.ret.mst.1803 <- f.cube.ret.mst.1803[cube.symbol %in% cid.1day]
f.cube.info.mst.1803 <- f.cube.info.mst.1803[cube.symbol %in% cid.1day]
f.cube.rb.mst.1803 <- f.cube.rb.mst.1803[cube.symbol %in% cid.1day]

sv(f.cube.ret.mst.1803)
sv(f.cube.info.mst.1803)
sv(f.cube.rb.mst.1803)

# 建立year-week和date之间的对应表 f.yw，用于绘图 ----
ld(f.cube.ret.mst.1803)
f.ywd.mst.1803 <- f.cube.ret.mst.1803[, .(date = unique(date))][order(date)][, ":="(year = year(date), week = week(date))][, tail(.SD, 1), keyby = .(year, week)]
sv(f.ywd.mst.1803)

# 计算cube周收益 ----
ld(f.cube.ret.mst.1803)
# 计算周收益
f.cube.wret.mst.1803 <- f.cube.ret.mst.1803[, ":="(year = year(date), week = week(date))
    ][order(cube.symbol, year, week, - date)
    ][, .SD[1], keyby = .(cube.symbol, year, week)
    ][, ":="(wret = growth(value) * 100), keyby = cube.symbol
    ][, ":="(label = NULL, date = NULL)
    ] %>% na.omit()
sv(f.cube.wret.mst.1803) 