# correlated / non-correlated trade， risk时间序列 ----
ld(rb.sp)
ld(f.stk.risk)
risk <- rb.sp[, .(cube.symbol, stkcd = str_sub(stock.symbol, 3, 8), date, signal, amt)
    ][f.stk.risk[, .(stkcd, ivol, vol, skewness, kurtosis, ivol.10)], on = .(stkcd), nomatch = 0
    ][signal %in% c(1, 3), ":="(is.lead = 1)
    ][signal %in% c(4, 2), ":="(is.lead = 0)]
sv(risk)
## risk: 按amt加权
#risk[, .(risk = sum(ivol * (amt / sum(amt))), n = .N), keyby = .(signal)]
## risk: 等权
#risk[, .(risk = mean(ivol), n = .N), keyby = .(signal)]

# 这里的risk 指的是 idiosyncratic risk
ld(risk)
ld(f.yw)
risk.plot <- risk[, .(risk = sum((ivol.10 >= 9) * (amt / sum(amt)))), keyby = .(is.lead, year = year(date), week = week(date))
    ][f.yw, on = .(year, week), nomatch = 0]

risk.plot[date >= "2016-07-01" & is.lead %in% c(1, 0)] %>%
    ggplot(aes(x = date, y = risk)) +
    theme_bw() +
    geom_point(size = 2.25) +
    geom_line(size = 0.75) +
    geom_line(stat = "smooth", size = 0.65, linetype = "dashed", method = "lm", se = F, alpha = 0.6) +
    xlab("") +
    ylab("Risk") +
    #scale_color_discrete(name = "", labels = c("Non-correlated trade", "Correlated trade")) +
    #scale_x_date(date_breaks = "2 month", date_labels = "%b") +
    theme(legend.position = "bottom")

ggsave("risk_diff.jpg")
# Trading frequence plot ----
ld(rb.sp)
ld(f.yw)
ld(SDATE)
ld(EDATE)

freq.plot <- rb.sp[, .(freq = .N / uniqueN(cube.symbol), amt = sum(abs(amt) / uniqueN(cube.symbol))), keyby = .(year(date), week(date))
    ][f.yw, on = .(year, week), nomatch = 0]

freq.plot[between(date, SDATE, EDATE)] %>%
    ggplot(aes(x = date, y = freq)) +
    theme_bw() +
    geom_line(size = 0.75) +
    geom_smooth(data = freq.plot[between(date, SDATE, EDATE) & freq >= 2], method = "lm", se = F, linetype = "dashed", color = "grey") +
    geom_point(size = 2.25) +
    xlim(c(SDATE, as.Date("2017-09-01"))) +
    xlab("") +
    ylab("Trading Frequency (times / week)")

# 导入market volatility，考察market volatile的时候trading frequency如何变化
# risk.roll 是 HS300 的120日滚动 vol，来自Hedgefund-02
ld(risk.roll)
plot <- risk.roll[, .(skew = mean(skew), rv = mean(rv), gv = mean(gv)), keyby = .(year = year(date), week = week(date))
    ][freq.plot, on = .(year, week), nomatch = 0]

# 结果并不太好，因为我发现在样本期HS300的vol是下降的，而同期trade freq却上升
plot %>% 
    ggplot(aes(x = date, y = freq)) +
    geom_line()
plot %>% 
    ggplot(aes(x = date, y = skew)) +
    geom_line()


## 考察signal follower's return的波动率 ----
#ld(cube.ret)
## 每个cube执行30日滚动平均
#cube.risk <- cube.ret[order(cube.symbol, date)
    #][, ":="(dret = growth(value) * 100), keyby = .(cube.symbol)
    #][, {
    #n <- 30
    #if (.N > n) {
        #foreach(t = (n+1):.N, .final = rbindlist, .packages = "PerformanceAnalytics") %dopar% {
            #ret <- dret[(t - n):t]
            #skew <- skewness(ret)
            #rv <- sum(dret^2)
            #list(date = date[t], skew = skew, rv = rv)
        #}
    #}}, 
    #keyby = .(cube.symbol)]