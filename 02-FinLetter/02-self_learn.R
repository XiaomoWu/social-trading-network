# 把SP的所有调仓分成两类，learn-trade & self-trade ----
ld(cube.rb) # 45s
ld(f.cu)
ld(f.stk.risk)
# rb：所有调仓记录的标的、调仓幅度、日期
# 1）排除ipo中标交易，2）只包含买入交易，3）一天多次交易只计算当天累计值，4）rb中的stock.symbol只能是在f.stk.risk中出现过的
rb <- cube.rb[, .(cube.symbol, cube.type, stock.symbol, date = as.Date(datetime), pre.weight = as.numeric(prev.weight.adjusted), target.weight)
    ][is.na(pre.weight), pre.weight := 0
    ][str_sub(stock.symbol, 3, 8) %in% f.stk.risk$stkcd
    ][target.weight > pre.weight, .(amt = sum(target.weight - pre.weight)), keyby = .(cube.symbol, cube.type, stock.symbol, date)
    ][f.cu[, .(cube.symbol, user.id)], on = .(cube.symbol), nomatch = 0] 
#rm(cube.rb)
sv(rb)

# Summary: 调仓幅度
rb[, .(amt = mean(amt)), keyby = .(cube.type)]

# 导入follow关系
ld(f.user.stock)
ld(f.cu) # cid与owner的关系
f.cu[, cube.type := str_sub(cube.symbol, 1, 2)] # 为f.cu添加cube.type变量

# follow：SP的自己的调仓与neighbor的调仓放到同一个数据集中
follow <- f.user.stock[cube.type == "ZHCN", .(user.id, cube.symbol = symbol, create.date) # ZHCH包含ZH和SP
    ][f.cu[, .(cube.symbol, owner.id = user.id)], on = .(cube.symbol), nomatch = 0][user.id != owner.id][, owner.id := NULL][user.id %in% f.cu[cube.type == "SP", user.id] # 1）只保留SP owner的follow；2）剔除自己follow自己的情况
    ][rb[, .(cube.symbol, stock.symbol, date, amt)], on = .(cube.symbol), nomatch = 0][order(user.id, cube.symbol, date)# 把rb的follow买入记录merge进来
    ][rb[cube.type == "SP", .(user.id, stock.symbol, spbuy.date = date, spbuy.amt = amt)], on = .(user.id, stock.symbol), nomatch = 0
    ][order(user.id, cube.symbol, date, spbuy.date)] %>% unique() # 把rb的user.id买入记录merge进来
sv(follow)

# Summary: Follower每周接受的signal数
#follow[, cube.type := str_sub(cube.symbol, 1, 2)
    #][cube.type == "SP", .(.N/uniqueN(cube.symbol)), keyby = .()]


# 1 = [-30, -16], 2 = [16, 30], 3 = [ -15, -1], 4 = [1, 15]
# amt: 大于此值的signal才算有效
ld(follow)
ld(rb)
learn.trade <- follow[amt >= 25
    ][between(spbuy.date, date - 30, date - 16), signal := 1
    ][between(spbuy.date, date - 15, date - 1), signal := 3
    ][between(spbuy.date, date + 1, date + 15), signal := 4
    ][between(spbuy.date, date + 16, date + 30), signal := 2
    ][!is.na(signal)
    ][order(user.id, stock.symbol, spbuy.date, signal),
    .SD[1], keyby = .(user.id, stock.symbol, spbuy.date)]

# rb.sp：最终数据集，把learn.trade与rb.sp合并。如果同一天多次买入同一只股票，那么算learn=1的那笔交易
rb.sp <- learn.trade[, .(user.id, stock.symbol, date = spbuy.date, signal)
    ][rb[cube.type == "SP"], on = .(user.id, stock.symbol, date)
    ][is.na(signal), signal := 0]
rb.sp[, table(signal)]
sv(rb.sp)


# 看一下所有交易中self/learning的比例
#rb.sp[, sum(signal) / .N]

# 计算learn/self trade的performance，并绘图 ----
# dret, edret 全都是 in decimal
ld(f.stk.dret)
ld(rb.sp)
rb.sp.nv <- rb.sp[, # 每一笔交易都填充后180天记录
    .(signal, end.date = seq(from = date, to = date + 180, by = "day"), amt), keyby = .(user.id, cube.symbol, stock.symbol = str_sub(stock.symbol, 3, 8), date)
    ][date != end.date # 买入当天的收益不计算
    ][f.stk.dret[, .(stkcd, date, dret, edret)], on = .(stock.symbol = stkcd, end.date = date), nomatch = 0][order(user.id, cube.symbol, stock.symbol, date, end.date)][, ":="(nv = cumprod(1 + dret), t = seq_len(.N)), keyby = .(user.id, cube.symbol, stock.symbol, date)] # 将stk.dret合并

rb.sp.nv.plot <- rb.sp.nv[date >= "2016-07-01", .(dret = sum(dret * (amt / sum(amt))), edret = sum(edret * (amt / sum(amt)))), keyby = .(signal, t)][order(signal, t)][, .(t, dret, edret, nv = cumprod(1 + dret), nv2 = cumprod(1 + edret)), keyby = .(signal)]

# t.test：检验两者的收益率是否有差别
#t.test(rb.sp.nv.plot[learn == 0 & t <= 120, dret * 100], rb.sp.nv.plot[learn == 1 & t <= 120, dret] * 100)

# Summary: daily / cummulative return
# average daily
rb.sp.nv.plot[, .(ret = mean(edret) * 100), keyby = .(signal)]
# cumulative
rb.sp.nv.plot[t == 90, .(ret = (nv2 - 1) * 100 ), keyby = .(signal)]

# 1 = [-30, -16], 2 = [16, 30], 3 = [ -15, -1], 4 = [1, 15]
ret <- rb.sp.nv.plot[t <= 90 & signal %in% c(0, 2)] %>%
    ggplot(aes(x = t, y = edret * 100, linetype = as.factor(signal), color = as.factor(signal))) +
    theme_bw() +
    geom_point(size = 2.25) +
    geom_line(size = 0.75) +
    scale_color_discrete(name = "", labels = c("Benchmark", "Lagging: 2-4 weeks")) +
    scale_linetype_discrete(name = "", labels = c("Benchmark", "Lagging: 2-4 weeks")) +
    xlab('') +
    ylab("Daily Excess Return (%)") +
    scale_x_continuous(breaks = c(0, 30, 60, 90)) +
    theme(legend.position = "bottom")

nv <- rb.sp.nv.plot[t <= 90 & signal %in% c(0, 2)] %>%
    ggplot(aes(x = t, y = nv2, linetype = as.factor(signal), color = as.factor(signal))) +
    theme_bw() +
    geom_line(size = 0.75) +
    scale_color_discrete(name = "", labels = c("Benchmark", "Lagging: 2-4 weeks")) +
    scale_linetype_discrete(name = "", labels = c("Benchmark", "Lagging: 2-4 weeks"))
    xlab('') +
    ylab("Cumulative Performance") +
    scale_x_continuous(breaks = c(0, 30, 60, 90)) +
    theme(legend.position = "bottom")

multiplot(ret, nv, cols = 2)
#ggsave("0_1.jpg")


# 下面是把四条线全画在一张图里
#ggplot() +
    #theme_bw() +
    #geom_line(data = rb.sp.nv.plot[t <= 120 & signal == 0], aes(x = t, y = nv), color = "#FFCC00", size = 1) +
    #geom_line(data = rb.sp.nv.plot[t <= 120 & signal == 1], aes(x = t, y = nv), color = "#FF3300", size = 1, linetype = "solid") +
     #geom_line(data = rb.sp.nv.plot[t <= 120 & signal == 3], aes(x = t, y = nv), color = "#FF9999", size = 1, linetype = "solid") +
     #geom_line(data = rb.sp.nv.plot[t <= 120 & signal == 2], aes(x = t, y = nv), color = "#0099FF", size = 1, linetype = 5) +
     #geom_line(data = rb.sp.nv.plot[t <= 120 & signal == 4], aes(x = t, y = nv), color = "#0033FF", size = 1, linetype = 5) +
    #xlab("") +
    #ylab("Cumulative Performance")
