# Postfilter 后生成的数据集有的带有“f-”，说明可以给其它项目共用；有的带有“t-”，这是在“f-”基础上进一步filter得到的，只能01-Thesis自己用
# 计算 f.userlife ----
# userlife定义为：首先把一个user所有的cube汇总到一块，然后定义start为最早cube创建的时间，end为所有cube中最后一笔交易时间（需要结合cubelife）
ld(f.cubelife)
ld(f.cube.info)
f.userlife <- f.cubelife[f.cube.info[, .(cube.symbol, user.id = owner.id)], nomatch = 0
    ][, .(start = min(start), end = max(end)), keyby = .(user.id)
    ][, ":="(life = as.integer(end - start))]
sv(f.userlife)

# 计算cube周收益、月收益 ----
# 由于cube.ret太大，只能分块计算。首先将所有symbol通过ntile分成5份，然后把使用for循环对每一份进行取每周最后一天的观测，生成数据集week
# 周收益与月收益分开计算，否则内存不够
ld(f.cube.ret)
# 计算周收益
f.cube.wret <- f.cube.ret[, ":="(year = year(date), week = week(date))
    ][order(cube.symbol, year, week, -date)
    ][, .SD[1], keyby = .(cube.symbol, year, week)
    ][, ":="(wret = growth(value) * 100), keyby = cube.symbol
    ][, ":="(label = NULL, date = NULL)
    ] %>% na.omit()
sv(f.cube.wret)
# 周收益进行1%上下trim
t.cube.wret <- copy(f.cube.wret)[, ":="(wret = winsorize(wret, probs = c(0.01, 0.99)))]
sv(t.cube.wret)

# 计算月收益
f.cube.mret <- f.cube.ret[, ":="(year = year(date), month = month(date))][, tail(.SD, 1), keyby = .(cube.symbol, year, month)][, ":="(mret = growth(value) * 100), keyby = cube.symbol][, ":="(label = NULL, date = NULL)] %>% na.omit()
# 月收益进行1%上下缩尾
f.cube.mret <- f.cube.mret[mret %between% c(quantile(mret, 0.01), quantile(mret, 0.99))]
sv(f.cube.mret)

# f.user.nwk ----
# 筛选出所有follow的组合与实盘，然后按照周与月进行累加。即每一行观测都表明当前周/月所新增的组合以及累计follow的组合
# cube.type == ZHCN既包含组合，又包含实盘
ld(f.user.stock)
sadd <- function(x) {
    if (length(x) == 1) {
        x
    } else {
        as.list(Reduce(union, x, accumulate = T))
    }
}
ld(f.userlife)
CJ <- f.userlife[, .(date = seq(start, end, by = "day")), keyby = .(user.id)
    ][, .(user.id, year = year(date), week = week(date))
    ] %>% unique() # 为了下一步时间插值
system.time({
f.user.nwk <- f.user.stock[cube.type %in% c('ZHCN')
    ][, ":="(year = year(create.date), week = week(create.date))
    ][, .(follow.cube = list(stock.symbol)), keyby = .(user.id, year, week)
    ][CJ, on = .(user.id, year, week), nomatch = NA
    ][order(user.id, year, week)
    ][, ":="(nbr = sadd(follow.cube)), keyby = .(user.id)]
    }) # 

#full.yw <- copy(f.user.stock)[, .(user.id, year = year(create.date), week = week(create.date))][order(user.id, year, week)] %>% unique()
sv(f.user.nwk)



# 中签过的sp ----
ld(f.cube.ret)
f.iposp <- f.cube.ret[(label != '') & cube.type == 'SP', unique(cube.symbol)]
sv(f.iposp)


# 建立year-week和date之间的对应表 f.yw，用于绘图 ----
ld(f.cube.ret)
f.ywd <- f.cube.ret[, .(date = unique(date))][order(date)][, ":="(year = year(date), week = week(date))][, tail(.SD, 1), keyby = .(year, week)]
sv(f.ywd)

# 建立year-week和cube.ret之间的对应表 f.cyw----
ld(f.cube.wret)
f.cyw <- f.cube.wret[, .(cube.symbol, year, week)] %>% unique() %>% setorder(cube.symbol, year, week)
sv(f.cyw)

# 建立cube和owner之间的关系 ----
ld(f.cube.info)
f.cu <- f.cube.info[, .(cube.type = str_sub(cube.symbol, 1, 2), cube.symbol, user.id = owner.id)]
sv(f.cu)

f.sp.owner <- f.cu[cube.type == "SP", .(user.id, cube.symbol)
    ][order(user.id, -cube.symbol)][, .SD[1], keyby = .(user.id)]
sv(f.sp.owner)

