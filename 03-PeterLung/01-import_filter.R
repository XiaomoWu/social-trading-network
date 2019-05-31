# Import
# 03-PeterLung所使用的所有原始文件都来自 01-Thesis的“r-”或者“f-”，具体如下----
#ld(f.cube.info.mst.1803)
#ld(f.cube.rb.mst.1803)
#ld(f.cube.ret.mst.1803)
#ld(f.cubelife.mst.1803)
#ld(r.user.cmt.mst.1803)
#ld(f.user.stock.mst.1803)
#ld(f.user.info.mst.1803)
#ld(f.ywd.mst.1803)

SPDATE <- as.Date("2016-07-01")
sv(SPDATE)

# 对部分（不是全部）数据集进行进一步清洗，结果带有“p-”前缀
# p.cube.rb ----
# 删除条件：1）life >= 60 d；2）2016-07-01之后，因为只有在此之后才有实盘数据
ld(f.cubelife.mst.1803)
ld(f.cube.rb.mst.1803)
cid.90d <- f.cubelife.mst.1803[life >= 90, unique(cube.symbol)]
cid.60d <- f.cubelife.mst.1803[life >= 60, unique(cube.symbol)]
cid.30d <- f.cubelife.mst.1803[life >= 30, unique(cube.symbol)]
p.cube.rb <- f.cube.rb.mst.1803[cube.symbol %in% cid.60d
    ][, .(cube.symbol, cube.type, stock.symbol, pre.weight = prev.weight.adjusted, target.weight, created.at, date = as.Date(created.at))
    ][is.na(pre.weight), pre.weight := 0
    ][, ":="(amt = target.weight - pre.weight)
    ][order(cube.symbol, created.at)]
sv(p.cube.rb)

# p.cube.ret ----
ld(f.cube.ret.mst.1803)
p.cube.ret <- f.cube.ret.mst.1803[cube.symbol %in% cid.60d
    ][, .(n = .N, value, label, cube.type), keyby = .(cube.symbol, date)
    ][n >= 2, ":="(value = NA) # 如果同一天有两条记录，由于无法保证哪一条记录是对的，所以把这两天的记录全都删掉，用前一天的locf
    ][, ":="(value = na.locf(value, na.rm = F)), keyby = .(cube.symbol)
    ][!is.na(value)
    ][, ":="(n = .N), by = .(rleid(value))
    ][n <= 20 # 如果有个组合连续20个交易日的value都没有变动，就认为这个组合已死，从30天的第一天之后的数据都剔除
    ] %>% unique(by = c("cube.symbol", "date"))
sv(p.cube.ret)


# p.cube.wret ----
# 删除条件：1）2016-07-01之后; 2）1%上下winsorize
ld(f.cube.wret.mst.1803)
p.cube.wret <- copy(f.cube.wret)[, ":="(wret = winsorize(wret, probs = c(0.01, 0.99)))
    ][f.ywd, on = .(year, week), nomatch = 0]
sv(p.cube.wret)

# Market (daily/weekly) return / risk ----
ld(f.dmkt) # f.dmkt 来自 trd.cndalym，包含了综合A股与创业板市场的日收益、成交量
# p.dmkt：综合市场日统计
n <- 60 # risk 按照60日滚动
p.dmkt <- f.dmkt[, lapply((n + 1):.N, function(i) {
    dret <- dret.mkt[(i - n):i];
    skew <- skewness(dret);
    kurt <- kurtosis(dret);
    rv <- sd(dret);
    list(date = date[i], dret.mkt = dret.mkt[i], damt.mkt = damt.mkt[i], skew = skew, kurt = kurt, rv = rv)
}) %>% rbindlist()]
sv(p.dmkt)

## p.wmkt：综合市场周统计
#ld(p.dmkt)
#p.wmkt <- p.dmkt[, .(wret.mkt = prod(1 + dret.mkt / 100) - 1,
    #wamt.mkt = sum(damt.mkt),
    #skew = skewness(dret.mkt),
    #kurt = kurtosis(dret.mkt),
    #rv = sd(dret.mkt)),
    #keyby = .(year = year(date), week = week(date))
    #][f.ywd, on = .(year, week), nomatch = 0]

p.wmkt <- p.dmkt[, .(wret.mkt = prod(1 + dret.mkt / 100) - 1,
    wamt.mkt = sum(damt.mkt),
    skew = mean(skew),
    kurt = mean(kurt),
    rv = mean(rv)),
    keyby = .(year = year(date), week = week(date))
    ][f.ywd, on = .(year, week), nomatch = 0]
sv(p.wmkt)

# N (network 的人数) ----
# 使用每周累计的cube.n来proxy，因为cube有建立时间——deprecated！！因为这样cube只会多不会少
#ld(f.cube.info.mst.1803)
#p.nwk.size <- f.cube.info.mst.1803[, .(cube.symbol, year = year(create.date), week = week(create.date))
    #][, .(new.cube.n = uniqueN(cube.symbol), new.sp.n = uniqueN(cube.symbol[str_sub(cube.symbol, 1, 2) == "SP"])), keyby = .(year, week)
    #][, ":="(cube.n = cumsum(new.cube.n), sp.n = cumsum(new.sp.n))
    #][year <= 2018]
#sv(p.nwk.size)

# 使用f.cubelife.mst.1803来proxy
ld(f.cubelife.mst.1803)
itvl <- data.table(date = seq(as.Date("2014-01-01"), as.Date("2018-01-01"), by = "day"))[, ":="(year = year(date), week = week(date))
    ][, .(start = min(date), end = max(date)), keyby = .(year, week)]
setkey(itvl, start, end)
setkey(f.cubelife.mst.1803, start, end)

olap <- foverlaps(itvl, f.cubelife.mst.1803, type = "any", which = T, nomatch = 0)

p.nwk.size <- olap[, {cube.symbol <- f.cubelife.mst.1803$cube.symbol[yid];
    cube.n <- length(cube.symbol);
    sp.n <- sum(str_sub(cube.symbol, 1, 2) == "SP");
    year <- itvl$year[.BY[[1]]];
    week <- itvl$week[.BY[[1]]];
    list(year = year, week = week, cube.n = cube.n, sp.n = sp.n)},
    key = .(xid)
    ][, ":="(xid = NULL)]

sv(p.nwk.size)
rm(olap, itvl)

# cmt.n 发帖人数 ----
ld(r.user.cmt.mst.1803) # 用时 3.5 分钟
ld(f.cu)
ld(SDATE)
ld(f.ywd)
# p.cmt.n: 每个uid发帖统计。msg.n: 全部发帖；msg.n.user：用户发帖（排除“我刚刚”）
p.cmt.n <- r.user.cmt.mst.1803[date >= SDATE, .(msg.n = .N, msg.n.user = .N - sum(str_sub(text, 1, 3) == "我刚刚")), keyby = .(user.id, year = year(date), week = week(date))]
sv(p.cmt.n)

# p.nwk.cmt.n: 整个network的发帖统计
ld(f.sp.owner)
ld(p.nwk.size)
p.nwk.cmt.n <- p.cmt.n[, ":="(is.sp = ifelse(user.id %in% f.sp.owner$user.id, T, F))
    ][, .(sp.cmt.n = sum(msg.n[is.sp == T]), sp.cmt.n.user = sum(msg.n.user[is.sp == T]), cmt.n = sum(msg.n)), keyby = .(year, week)
    ][p.nwk.size, on = .(year, week), nomatch = 0]
sv(p.nwk.cmt.n)

p.nwk.cmt.n[f.ywd, on = .(year, week), nomatch = 0] %>% 
    ggplot(aes(x = date, y = sp.cmt.n.user)) +
    geom_line()