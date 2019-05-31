# 设定研究中的常数 ----
# 样本区间
SDATE <- as.Date("2016-07-01)")
EDATE <- as.Date("2017-12-1")
sv(SDATE)
sv(EDATE)

# FinLetter中所有的原始数据都来自Thesis中以下文件 ----
ld(f.cube.rb, T) # 特征：1)market = cn, 2) category = user_rebalancing, 3) status = success
ld(f.cube.info, T)
ld(uid, T)
ld(cid, T)


# cube.rb ----
# life >= 60 d
# 2016-07-01之后
cubelife <- f.cube.rb[, ":="(date = as.Date(datetime))][, .(life = as.numeric(max(date) - min(date))), keyby = .(cube.symbol)]
cid.90d <- cubelife[life >= 90, unique(cube.symbol)]
cid.60d <- cubelife[life >= 60, unique(cube.symbol)]
cid.30d <- cubelife[life >= 30, unique(cube.symbol)]
cid <- intersect(cid, cid.60d)
cube.rb <- f.cube.rb[cube.symbol %in% cid][date >= SDATE]
sv(cube.rb)

# cube.ret ----
cube.ret <- f.cube.ret[cube.symbol %in% cid][date >= SDATE]
sv(cube.ret)

# Summary: 组合数以及调仓总数
ld(cube.rb)
cube.rb[, uniqueN(cube.symbol), keyby = .(cube.type)] # 组合数
cube.rb[, .N, keyby = .(cube.type)] # 调仓总数

# Summary: 调仓数据集（rb）中provider和follower的调仓频率
unique(cube.rb[, .(cube.type, n = .N / (as.numeric(max(date) - min(date)))), keyby = .(cube.symbol)
    ][is.finite(n)])[, .(min = min(n, na.rm = T), 
    q25 = quantile(n, 0.25, na.rm = T) * 7, 
    mean = mean(n, na.rm = T) * 7, 
    median = median(n, na.rm = T) * 7, 
    q75 = quantile(n, 0.75, na.rm = T) * 7, 
    max = quantile(n, 0.995) * 7, 
    std = sd(n) * 7),
    keyby = .(cube.type)]

z[, quantile(n, 0.995) * 7, keyby = .(cube.type)]

# cube.info ----
cube.info <- f.cube.info[cube.symbol %in% cid]
sv(cube.info)
# Summary: 收益(做上下2% trim)
ld(cube.info)
cube.info[, .(cube.type, n = (net.value - 1) * 100)
    ][n %between% quantile(n, c(0.02, 0.98))
    ][, .(min = min(n), q25 = quantile(n, 0.25, na.rm = T), mean = mean(n, na.rm = T), median = median(n, na.rm = T), q75 = quantile(n, 0.75, na.rm = T), max = max(n), sd = sd(n)), keyby = .(cube.type)]


# f.stk.dret：比r.stk.dret多了一个超额收益，超额收益的估计期为2016-07-01至2017-03-20 ----
# return用decimal表示，不是percentage
ld(r.dstk)
ld(r.d3f)
dret <- r.dstk[r.d3f[, .(date, mkt.dret = rm)], on = .(date), nomatch = 0]
model <- dret[date >= SDATE & date <= EDATE, as.list(coef(lm(dret ~ mkt.dret, .SD))), keyby = stkcd]
setnames(model, names(model), c("stkcd", "alpha", "beta"))
f.stk.dret <- dret[model, on = .(stkcd), nomatch = 0][, ":="(edret = dret - (alpha + mkt.dret * beta))]
sv(f.stk.dret)

#f.stk.risk： 先求出每个股票的vol和ivol，生成用于衡量股票风险的数据集 ----
# 先导入个股日收益数据、市场收益率
# r.dstk 和 r.d3f 都更新至 2017-09-29
ld(r.dstk)
ld(r.d3f)
# 个股收益 + 市场收益 
ret <- r.dstk[date %between% c(SDATE, EDATE), .(stkcd, date, dret)][r.d3f[date %between% c(SDATE, EDATE), .(date, mkt.dret = rm)], on = .(date), nomatch = 0]
rm(r.dstk, r.d3f)
# 生成每个股票的风险measure：f.stk.risk
library(PerformanceAnalytics)
library(dplyr)
system.time(
{
    f.stk.risk <-
    ret[date %between% c(SDATE, EDATE)
       ][,
         dret.100 := ntile(dret, 100),
         by = stkcd
       ][dret.100 %between% c(2, 99), # filter top/down 1% return outlie
         .SD[.N >= 100], # filter stk with less than 100 trding days
         by = stkcd
       ][,
         .(vol = sd(dret),
           ivol = sd(resid(lm(dret ~ mkt.dret, data = .SD))),
           skewness = skewness(dret, method = "sample"),
           kurtosis = kurtosis(dret, method = "sample_excess")
         ),
         by = stkcd
       ][,
         ":="(vol.10 = ntile(vol, 10),
              ivol.10 = ntile(ivol, 10),
              skewness.10 = ntile(skewness, 10),
              kurtosis.10 = ntile(kurtosis, 10))]
}) # 10 min
sv(f.stk.risk)
