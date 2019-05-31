setwd("C:/Users/rossz/OneDrive/SNT/03-PeterLung")

# 导入股票的日度收益数据，计算每个股票每个月的vol和ivol，保存在dvol中 ----
ld(f.dret.stk)
ld(r.d3f)

# 把三因子（d3f）合并到日收益数据中
# dvol：每个股票的波动性特质
dvol <- na.omit(f.dret.stk[r.d3f, on = .(date), nomatch = 0
    ])[order(stkcd, date)
    ][date >= as.Date("2015-01-01"), {
        vol <- sd(dret * 100, na.rm = T);
        model <- lm(I(dret * 100) ~ I(rm * 100) + I(smb * 100) + I(hml * 100), data = .SD);
        ivol <- sd(residuals(model), na.rm = T);
        mret <-  (prod(1 + dret) - 1) * 100;
        alpha <- coef(model)[1];
        size <- mean(cap, na.rm = T);
        list(vol = vol, ivol = ivol, mret = mret, alpha = alpha, size = size)},
        keyby = .(stkcd, year = year(date), month = month(date))]

# 统计在雪球网上，每个股票每个月的平均交易次数(trade.count)
ld(p.cube.rb)
ld(p.nwk.size)

nwk.size <- p.nwk.size[, .(date = as.Date(str_c(year, week, 1, sep = "-"), "%Y-%U-%u"), cube.n, sp.n)
    ][, .(year = year(date), month = month(date), cube.n, sp.n)
    ][, .SD[.N], keyby = .(year, month)] %>% na.omit()
# trade.count：每个股票每个月的平均交易次数
trade.count <- p.cube.rb[, .(trade.n = .N, trade.buy.n = sum(amt > 0), trade.sell.n = sum(amt < 0)), keyby = .(stock.symbol, year = year(date), month = month(date))
    ][, ":="(trade.diff = trade.buy.n - trade.sell.n)
    ][nwk.size, on = .(year, month), nomatch = 0
    ][str_sub(stock.symbol, 1, 2) %in% c("SH", "SZ")
    ][, ":="(stkcd = str_sub(stock.symbol, 3, 8))]


# 把所有股票每个月按照vol/ivol分组 ----
rank.vol <- dvol[, ":="(rank.vol = ntile(vol, 20), rank.ivol = ntile(ivol, 20)), keyby = .(year, month)
    ][trade.count, on = .(stkcd, year, month), nomatch = 0
    ][, .(ret = mean(mret, na.rm = T), alpha = mean(alpha, na.rm = T),
    trade.n = mean(trade.n, na.rm = T), trade.buy.n = mean(trade.buy.n, na.rm = T), trade.sell.n = mean(trade.sell.n), trade.diff = mean(trade.diff, na.rm = T), size = mean(size, na.rm = T)), keyby = .(rank.vol)] %>% na.omit()

rank.ivol <- dvol[, ":="(rank.vol = ntile(vol, 20), rank.ivol = ntile(ivol, 20)), keyby = .(year, month)
    ][trade.count, on = .(stkcd, year, month), nomatch = 0
    ][, .(ret = mean(mret, na.rm = T), alpha = mean(alpha, na.rm = T),
    trade.n = mean(trade.n, na.rm = T), trade.buy.n = mean(trade.buy.n, na.rm = T), trade.sell.n = mean(trade.sell.n), trade.diff = mean(trade.diff, na.rm = T), size = mean(size, na.rm = T)), keyby = .(rank.ivol)] %>% na.omit()

# 按照月交易频率分组 ----
rank.trade <- dvol[trade.count, on = .(stkcd, year, month), nomatch = 0
    ][, ":="(rank.trade = ntile(trade.n, 20), rank.trade.diff = ntile(trade.diff, 20)), keyby = .(year, month)
    ][, .(ret = mean(mret, na.rm = T), alpha = mean(alpha, na.rm = T), size = mean(size, na.rm = T)), keyby = .(rank.trade.diff)] %>% na.omit()

# 绘图：波动率与成交量 ----
rank.ivol %>%
    ggplot(aes(x = rank.ivol, y = trade.diff), color = "gray") +
    geom_line() +
    geom_point() +
    theme_bw() +
    #scale_x_continuous(breaks = c(2, 4, 6, 8, 10)) +
    xlab("Idiosyncratic Volatility") +
    ylab("# Buy - # Sell")

# 绘图：波动率与收益率 ----
rank.ivol %>%
    ggplot(aes(x = rank.ivol, y = alpha), color = "gray") +
    geom_line() +
    geom_point() +
    theme_bw() +
    #scale_x_continuous(breaks = c(2, 4, 6, 8, 10)) +
    xlab("Idiosyncratic Volatility") +
    ylab("# Buy - # Sell")

# 绘图：需求与收益率 ----
rank.trade %>%
    ggplot(aes(x = rank.trade.diff, y = alpha), color = "gray") +
    geom_line() +
    geom_point() +
    theme_bw() +
    #scale_x_continuous(breaks = c(2, 4, 6, 8, 10)) +
    xlab("# Buy - # Sell") +
    ylab("Alpha")
