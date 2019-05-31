# 本脚本所有代码均可在 TF 中运行
# 必须要按顺序执行，不能当中跳过！

# 生成cid，用于对cube类进行剔除 ----
ld(r.cube.info, T)
# 0. cube.symbol有重复（因为包含了两次抓取的记录），选择lastcrawl大的
cube.info <- r.cube.info[order(cube.symbol, - lastcrawl)][, .SD[1], keyby = .(cube.symbol)]
rm(r.cube.info)

# 1. r.cube.info: market = 'cn'
cid.cn <- cube.info[market == 'cn', unique(cube.symbol)]
sv(cid.cn)

# 2. exists in cube.rb
# r.cube.rb已经经过去重处理，不需要再去重。补充信息如下：
#> uniqueN(r.cube.rb, by = c("cube.symbol"))
#[1] 1116086
#> uniqueN(r.cube.rb, by = c("cube.symbol", "stock.id", "datetime"))
#[1] 40188907
#> uniqueN(r.cube.rb, by = c("id"))
#[1] 39358674
#> uniqueN(r.cube.rb)
#[1] 40213456 == nrow(r.cube.rb)
ld(r.cube.rb)
cid.rb <- r.cube.rb[, unique(cube.symbol)]
sv(cid.rb)

# 3. exists in cube.ret
# r.cube.ret经过去重处理，不需要再去重。补充信息如下：
#> uniqueN(r.cube.ret, by = c("cube.symbol", "date"))
#[1] 468832185
#> uniqueN(r.cube.ret)
#[1] 468837556 == nrow(r.cube.ret)
ld(r.cube.ret) # 6 min
cid.ret <- r.cube.ret[, unique(cube.symbol)]
sv(cid.ret)

# 4. abnormal return
# 净值最低的1%以及净值超过150的剔除
cid.abret <- cube.info[net.value %between% c(quantile(net.value, 0.01), 150), unique(cube.symbol)]
sv(cid.abret)

# 5. 使用上述生成的cid.abret, cid.cn, cid.rb, cid.ret生成最终的cid
cid <- intersect(cid.abret, cid.cn) %>% intersect(cid.rb) %>% intersect(cid.ret)
sv(cid)


# 生成uid，用于对user类进行剔除 ----
# 1. 只有cid的owner才可能称为uid
uid.cidowner <- cube.info[cube.symbol %in% cid, unique(owner.id)]
sv(uid.cidowner)

# 2. exists in r.user.info
# r.user.info中的user.id有重复，选择lastcrawl大的那个
ld(r.user.info)
user.info <- r.user.info[order(user.id, - lastcrawl)][, .SD[1], keyby = .(user.id)]
uid.userinfo <- user.info[, unique(user.id)]
sv(uid.userinfo)
rm(r.user.info)

# 3. exists in r.user.stock
# r.user.stock需要去重，其主键是 （user.id, code, createAt）。在主键相同的情况下，选择lastcrawl大的
# 注意，stockName是不准的，因为公司可能改名，但还是同一家公司（stockName不同但code相同）。补充信息如下：
# uniqueN(r.user.stock, by = c("user.id", "code", "createAt")) # 28103735
ld(r.user.stock)
user.stock <- r.user.stock[order(user.id, code, createAt, - lastcrawl)][, .SD[1], keyby = .(user.id, code, createAt)]
rm(r.user.stock)
uid.userstock <- user.stock[, unique(user.id)]
sv(uid.userstock)

# 使用上述生成的uid.cidowner, uid.userinfo生成最终的uid
uid <- intersect(uid.cidowner, uid.userinfo) %>% intersect(uid.userstock)
sv(uid)


# 使用cid和uid生成 f-prefix ----
# 生成cube.info
# 所有fans.count减1
f.cube.info <- cube.info[cube.symbol %in% cid][, ":="(fans.count = (fans.count - 1))]
sv(f.cube.info)
rm(cube.info, r.cube.info, f.cube.info)

# 生成f.cube.rb
f.cube.rb <- r.cube.rb[cube.symbol %in% cid][, ":="(prev.weight.adjusted = as.numeric(prev.weight.adjusted))]
rm(r.cube.rb)
# target.weight与prev.weight只能是NA或者是[0,110]之间的数
# 有时不知什么原因，prev.weight可能略微超过100，这时不算错误
f.cube.rb <- f.cube.rb[(is.na(target.weight) | target.weight %between% c(0, 110)) & (is.na(prev.weight.adjusted) | prev.weight.adjusted %between% c(0, 110))]
sv(f.cube.rb)
rm(f.cube.rb)

# 生成user.info
f.user.info <- user.info[user.id %in% uid]
sv(f.user.info)
rm(user.info, f.user.info)

# 生成user.stock
f.user.stock <- user.stock[user.id %in% uid]
f.user.stock <- f.user.stock[, .(user.id, stock.symbol = code, cube.type = exchange, create.date = as.Date(as.POSIXct(createAt / 1000, origin = "1970-01-01")), buy.price = buyPrice, sell.price = sellPrice, is.notice = isNotice, target.percent = targetPercent)]
sv(f.user.stock)
rm(user.stock, f.user.stock)

# 生成f.cube.ret 
f.cube.ret <- r.cube.ret[cube.symbol %in% cid]
rm(r.cube.ret)
# 考虑 quit / re-enter, 对f.cube.ret进行进一步剔除
# life定义为第一笔至最后一笔之间的时间, f.cubelife包含每个组合的起讫时间以及天数
# life至少大于1，此时有603174个cube
ld(f.cube.rb)
f.cubelife <- f.cube.rb[order(cube.symbol, datetime)][, .(start = as.Date(datetime[1]), end = as.Date(datetime[.N]), trade.n = .N), keyby = .(cube.symbol)][, ":="(life = as.integer(end - start))][life >= 1]
cid.1day <- unique(f.cubelife$cube.symbol)
cid <- intersect(cid, cid.1day)
sv(f.cubelife)
sv(cid.1day)
sv(cid)
# 只保留第一笔至最后一笔交易之间的净值记录，共有84042179条
f.cube.ret <- f.cube.ret[f.cubelife[, .(cube.symbol, start, end)], on = .(cube.symbol), nomatch = 0][between(date, start, end)][, ":="(start = NULL, end = NULL)]
sv(f.cube.ret)

# 把 life>=1 这个条件应用于 f.cube.info, f.cube.rb ----
ld(f.cube.info)
ld(f.cube.rb)
ld(f.cube.ret)
f.cube.info <- f.cube.info[cube.symbol %in% cid.1day]
f.cube.rb <- f.cube.rb[cube.symbol %in% cid.1day]
sv(f.cube.info)
sv(f.cube.rb)


