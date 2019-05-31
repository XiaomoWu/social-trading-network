# 首先导入所有股价数据 ----
#f.dret.stk <- trd.dalyr[markettype %in% c("1", "4", "16"), .(stkcd, date = trddt, price = clsprc, vol = dnshrtrd, amt = dnvaltrd, dret = dretnd, adjprice = adjprcnd, cap = dsmvosd)]
#sv(f.dret.stk)
ld(f.dret.stk) # cap: 日流通市值；所有收益率均为不含红利收益

# D/P Ratio (自己写代码计算) ----
# 导入红利数据 date: 预案公告日，sp：送股比例，tr：转增比例，div：红利
# 不分红/分红一次占总样本的 40% 和 55%
div <- fread("C:/Users/rossz/OneDrive/SNT/03-PeterLung/firm.char/红利分配/CD_Dividend.txt", encoding = 'UTF-8')
setnames(div, names(div), tolower(names(div)))
div <- div[, .(stkcd = sprintf("%06d", stkcd), finyr = finyear, disyr = disye, date = as.Date(ppdadt), div = btperdiv)
    ][order(stkcd, date)
    ][, .SD[.N], keyby = .(stkcd, finyr, disyr) # 如果某些报告期有重复（例如某股谋财年有两个 disyr == 2），取时间晚那个。
    ][order(stkcd, date)
    ][!is.na(div), if.divdt := 1 # 标记出股息派发日
    ][, ":="(yrgrp = cumsum(ifelse(disyr == 2, 1, 0))), keyby = .(stkcd)
    ][!is.na(date)] # 有些比较早的观测日期缺失

# 对红利文件div进行日期插值，同时计算每个财年的累计红利
CJ <- div[, .(date = seq(min(date), max(date), by = "day")), keyby = stkcd]
div <- div[CJ, on = .(stkcd, date), nomatch = NA
    ][order(stkcd, date)
    ][is.na(div), div := 0
    ][is.na(if.divdt), if.divdt := 0
    ][, ":="(yrgrp = na.locf(yrgrp, na.rm = F)), keyby = stkcd
    ][, ":="(div = cumsum(div)), keyby = .(stkcd, yrgrp)]
rm(CJ)
# 与股价文件合并
div <- div[f.dret.stk[, .(stkcd, date, price)], on = .(stkcd, date), nomatch = 0
    ][, .(stkcd, date, div, price, dp = div/price)]
sv(div)

# 对所有股票逐日按照div排序（quintile）
divrank <- div[date >= as.Date("2010-01-01")
    ][dp == 0, ":="(divrank = 1)
    ][dp > 0, ":="(divrank = ntile(dp, 4) + 1), by = .(date)
    ][order(stkcd, date), .(stkcd, date, divrank)
    ] %>% unique(by = c("stkcd", "date"))
sv(divrank)
rm(div)

# PE/PB/PS Ratio（直接用CSMAR的数据）----
pe <- fread("C:/Users/rossz/OneDrive/SNT/03-PeterLung/firm.char/PE-PB-PS (CSMAR)/pe.txt", encoding = "UTF-8", header = T)
setnames(pe, names(pe), tolower(names(pe)))

# 对 DP, PE, PS, PB, liq（流动性，数字越大流动性越好） 排序
# dp = 0 说明没分红
perank <- pe[tradingdate >= as.Date("2013-01-01")
    ][, .(stkcd = sprintf("%06d", symbol), date = as.Date(tradingdate), pe = fill_na(pe), pb = fill_na(pb), ps = fill_na(ps), liq = fill_na(liquidility))
    ][, .(stkcd, perank = ntile(pe, 5), pbrank = ntile(pb, 5), psrank = ntile(ps, 5), liqrank = ntile(liq, 5)), keyby = date
    ][order(stkcd, date)]
sv(perank)

# Capital (Size) ----
ld(f.dret.stk)
sizerank <- f.dret.stk[date >= as.Date("2010-01-01"), .(stkcd, sizerank = ntile(cap, 5)), keyby = .(date)
    ][order(stkcd, date)
    ] %>% unique(by = c("stkcd", "date"))
sv(sizerank)

# Momentum/Contrarian ----
# 过去一年（180d）中最好/最差的股票
ld(f.dret.stk)
stk <- f.dret.stk[date >= as.Date("2013-01-01"), .(stkcd, date, adjprice, end.date = date)]
setkey(stk, stkcd, date, end.date)
itvls <- copy(stk)[, ":="(date = date - 180)]
olps <- foverlaps(itvls, stk, type = "any", which = T, nomatch = 0)
pastret <- olps[, .(pastret = stk[yid, (adjprice[.N] / adjprice[1] - 1) * 100]), keyby = xid] # 耗时10分钟

retrank <- copy(stk)[pastret$xid, ":="(pastret = pastret$pastret)
    ][, ":="(end.date = NULL, adjprice = NULL)
    ][date >= as.Date("2014-01-01") # 由于dret.stk数据从2013-01-01开始算，并且滚动期为360天，因此正式能用的数据从 2014-01-01 开始
    ][, .(stkcd, retrank = ntile(pastret, 5)), keyby = .(date)
    ][order(stkcd, date)
    ] %>% unique(by = c("stkcd", "date"))
sv(retrank)

# risk (250天滚动)----
# ivol: 自己用股票价格计算
ld(f.dret.stk)
ld(r.d3f)
# 设置 doParallel
cl <- makeCluster(8)
registerDoParallel(cl)

system.time({ 
ivol.120d <- f.dret.stk[r.d3f[, .(date, rm = winsorize(rm, probs = c(0.01, 0.99)))], on = .(date), nomatch = 0
    ][, ":="(dret = winsorize(dret, probs = c(0.01, 0.99)))
    ][order(stkcd, date)
    ][date >= as.Date("2015-01-01"),
    {
        n <- 120 # 120-day rolling
        if (.N >= n) {
            foreach(t = (n + 1):.N, .final = rbindlist, .packages = "PerformanceAnalytics") %dopar% {
                sub.dret <- dret[(t - n):t]
                sub.rm <- rm[(t - n):t]
                skew <- skewness(sub.dret)
                vol <- sum(sub.dret ^ 2)
                fit <- lm(sub.dret ~ sub.rm)
                beta <- coef(fit)[[2]]
                ivol <- sd(resid(fit))
                list(date = date[t], beta = beta, skew = skew, vol = vol, ivol = ivol)
            }
        } else if (.N %between% c(20, n)) {
            sub.dret <- dret
            sub.rm <- rm
            skew <- skewness(sub.dret)
            vol <- sum(sub.dret ^ 2)
            fit <- lm(sub.dret ~ sub.rm)
            beta <- coef(fit)[[2]]
            ivol <- sd(resid(fit))
            list(date = date, beta = beta, skew = skew, vol = vol, ivol = ivol)
        }
    }, keyby = .(stkcd)]
})
sv(ivol.120d)
ivolrank <- ivol.120d[, .(stkcd, betarank = ntile(beta, 5), skewrank = ntile(skew, 5), volrank = ntile(vol, 5), ivolrank = ntile(ivol, 5)), keyby = date]

# (Deprecated, 现在全部自己算) -- beta.vol数据集：包含 beta, volatility，来自GTA -- 
#beta.vol <- fread("C:/Users/rossz/OneDrive/SNT/03-PeterLung/firm.char/RISK-rolling-250-day/risk.txt", encoding = "UTF-8", header = T)
#setnames(beta.vol, names(beta.vol), tolower(names(beta.vol)))
#beta.volrank <- beta.vol[, .(stkcd = sprintf("%06d", symbol), date = as.Date(tradingdate), beta = beta1, vol = volatility)
    #][, .(stkcd, betarank = ntile(beta, 5), volrank = ntile(vol, 5)), keyby = date
    #][order(stkcd, date)]


# ed数据集：包含equity/debt
ed <- fread("C:/Users/rossz/OneDrive/SNT/03-PeterLung/firm.char/偿债能力/Equity-Debt.txt", encoding = "UTF-8", header = T)
setnames(ed, names(ed), tolower(names(ed)))
ed <- ed[typrep == "A", .(stkcd = sprintf("%06d", as.numeric(stkcd)), typrep, date = as.Date(accper), ed = f011801a)][order(stkcd, date)]
CJ <- ed[, .(date = seq(min(date), max(date), by = "day")), keyby = stkcd]
ed <- ed[CJ, on = .(stkcd, date), nomatch = NA
    ][order(stkcd, date)
    ][, .(date, ed = na.locf(ed, na.rm = F)), keyby = stkcd]
edrank <- ed[, .(stkcd, edrank = ntile(ed, 5)), keyby = date
    ][order(stkcd, date)]
# 把edrank合并到riskrank中
riskrank <- ivolrank[edrank, on = .(stkcd, date), nomatch = 0][order(stkcd, date)]
sv(riskrank)

# 盈利能力 ----
# profitrank包含：ROE / ROA / Profit margin
profit <- fread("C:/Users/rossz/OneDrive/SNT/03-PeterLung/firm.char/盈利能力/profitability.txt", encoding = "UTF-8", header = T)
setnames(profit, names(profit), tolower(names(profit)))
profit <- profit[typrep == "A", .(stkcd = sprintf("%06d", stkcd), date = as.Date(accper), typrep, roa = f050204c, roe = f050504c, profitmargin = f052301c)
    ][order(stkcd, date)
    ][, ":="(roa = na.locf(roa, na.rm = F), roe = na.locf(roe, na.rm = F), profitmargin = na.locf(profitmargin, na.rm = F)), keyby = stkcd
    ] %>% na.omit()

CJ <- profit[, .(date = seq(min(date), max(date), by = "day")), keyby = stkcd]
profitrank <- profit[CJ, on = .(stkcd, date), nomatch = NA
    ][order(stkcd, date)
    ][, ":="(roa = na.locf(roa, na.rm = F), roe = na.locf(roe, na.rm = F), profitmargin = na.locf(profitmargin, na.rm = F))
    ][, .(stkcd, roarank = ntile(roe, 5), roerank = ntile(roe, 5), profitmarginrank = ntile(profitmargin, 5)), keyby = date
    ][order(stkcd, date)]
sv(profitrank)

# Growth ----
# 都是同比增长, acc- 表示accelerated growth，即percentage growth
growth <- fread("C:/Users/rossz/OneDrive/SNT/03-PeterLung/firm.char/增长能力/growh.txt", encoding = "UTF-8", header = T)
setnames(growth, names(growth), tolower(names(growth)))
growth <- growth[typrep == "A", .(stkcd = sprintf("%06d", stkcd), date = as.Date(accper), salesgrowth = f081602c, earninggrowth = f081002b, assetgrowth = f080602a)
    ][order(stkcd, date)
    ][, ":="(acc.salesgrowth = c(NA, diff(salesgrowth)), acc.earninggrowth = c(NA, diff(earninggrowth)), acc.assetgrowth = c(NA, diff(assetgrowth))), keyby = stkcd] # 最后一步添加accelerated growth

CJ <- growth[, .(date = seq(min(date), max(date), by = "day")), keyby = stkcd]
growthrank <- growth[CJ, on = .(stkcd, date), nomatch = NA
    ][order(stkcd, date)
    ][, .(date, salesgrowth = na.locf(salesgrowth, na.rm = F),
    earninggrowth = na.locf(earninggrowth, na.rm = F),
    assetgrowth = na.locf(assetgrowth, na.rm = F),
    acc.salesgrowth = na.locf(acc.salesgrowth, na.rm = F),
    acc.earninggrowth = na.locf(acc.earninggrowth, na.rm = F),
    acc.assetgrowth = na.locf(acc.assetgrowth, na.rm = F)), keyby = stkcd
    ][, .(stkcd, salesgrowthrank = ntile(salesgrowth, 5),
    earninggrowthrank = ntile(earninggrowth, 5),
    assetgrowthrank = ntile(assetgrowth, 5),
    acc.salesgrowthrank = ntile(acc.salesgrowth, 5),
    acc.earninggrowthrank = ntile(acc.earninggrowth, 5),
    acc.assetgrowthrank = ntile(acc.assetgrowth, 5)), keyby = date
    ][order(stkcd, date)] %>% na.omit()
sv(growthrank)

# Earning surprise ----
# 在所有预测变量中，eps是缺失值最少的
# feps: 分析师eps预测的平均值
feps <- fread("C:/Users/rossz/OneDrive/SNT/03-PeterLung/firm.char/分析师预测/forcast.txt", encoding = "UTF-8", header = T)
setnames(feps, names(feps), tolower(names(feps)))
feps <- feps[, .(stkcd = sprintf("%06d", as.numeric(stkcd)), rptdt = as.Date(rptdt), fenddt = as.Date(fenddt), feps)
    ][, .(feps = median(feps, na.rm = T)), keyby = .(stkcd, year = year(fenddt))
    ][, ":="(feps = na.locf(feps, na.rm = F)), keyby = stkcd
    ] %>% na.omit()
# eps: 公司实际的eps
aeps <- fread("C:/Users/rossz/OneDrive/SNT/03-PeterLung/firm.char/分析师预测/actual.txt", encoding = "UTF-8", header = T)
setnames(aeps, names(aeps), tolower(names(aeps)))
aeps <- aeps[, .(stkcd = sprintf("%06d", as.numeric(stkcd)), date = ymd(ddate), aeps = meps)
    ][month(date) == 12 # 只选择对年度eps的预测
    ][order(stkcd, date)
    ][, ":="(year = year(date))
    ] %>% na.omit() # 太早的年份aeps缺失，故剔除
# SUE: surpring earning
sue <- feps[aeps, on = .(stkcd, year), nomatch = 0
    ][, .(stkcd, date, sue = aeps - feps)]
CJ <- sue[, .(date = seq(min(date), as.Date("2017-10-01"), by = "day")), keyby = stkcd]
suerank <- sue[CJ, on = .(stkcd, date), nomatch = NA
    ][order(stkcd, date)
    ][, .(date, sue = na.locf(sue, na.rm = F)), keyby = stkcd
    ][, .(stkcd, suerank = ntile(sue, 5)), keyby = date
    ][order(stkcd, date)]
sv(suerank)

# firmchar: 把所有的rank都合并 ----
ld(divrank)
ld(perank)
ld(sizerank)
ld(retrank)
ld(riskrank)
ld(profitrank)
ld(growthrank)
ld(suerank)

firmchar <- divrank[perank, on = .(stkcd, date), nomatch = 0
    ][sizerank, on = .(stkcd, date), nomatch = 0
    ][riskrank, on = .(stkcd, date), nomatch = 0
    ][growthrank, on = .(stkcd, date), nomatch = 0
    ][retrank, on = .(stkcd, date), nomatch = 0
    ][suerank, on = .(stkcd, date), nomatch = 0
    ][profitrank, on = .(stkcd, date), nomatch = 0
    ][order(stkcd, date)
    ] %>% unique()
sv(firmchar)

# 对于 firmchar 数据集的一些统计
#firmchar[, .N] # 1202113
#firmchar[, uniqueN(stkcd)] # 2793
#firmchar[, range(date)] # "2015-07-02" "2017-09-29"

# rb.char: 根据 firm.char，结合 cube.rb，得到个人的投资风格 -----
ld(firmchar)
ld(p.cube.rb)
ld(f.sp.owner)

p.rb.char <- p.cube.rb[f.sp.owner, .(user.id, cube.symbol, stkcd = str_sub(stock.symbol, 3, 8), date, year = year(date), week = week(date), amt), on = .(cube.symbol), nomatch = 0
    ][, .(cube.symbol, year, week, amt = sum(amt)), keyby = .(user.id, date, stkcd) # 同一个人在同一天可能交易同一个股票多次，进行合并
    ][firmchar, on = .(stkcd, date), nomatch = 0
    ][order(user.id, date, stkcd)
    ] %>% unique() %>% na.omit()
sv(p.rb.char)

# amt > 0 只考虑 buy trade；如果当周无交易，那么strategy则使用locf
p.rb.char.wk <- p.rb.char[amt > 0, lapply(.SD, weighted.mean, amt), keyby = .(user.id, cube.symbol, year, week), .SDcols = divrank:profitmarginrank
    ] %>% na.omit() 
sv(p.rb.char.wk)

