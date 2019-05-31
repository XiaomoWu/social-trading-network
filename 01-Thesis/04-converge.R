# 先求出alpha (weekly) ----
if (!exists("f.cube.wret")) ld(f.cube.wret)
if (!exists("r.w5f")) ld(r.w5f)
wreg <- f.cube.wret[r.wcapm, on = .(year, week), nomatch = 0]
walpha <- wreg[, as.list(coef(lm(I(wret) ~ rm_rf + smb + hml))), keyby = cube.symbol]
setnames(walpha, names(walpha), c("cube.symbol", "alpha", "rm_rf", "smb", "hml"))
walpha <- na.omit(walpha)
walpha[, ":="(cube.type = str_sub(cube.symbol, 1, 2))]
sv(walpha)

# alpha描述性统计 ----
# summary(walpha)
summ <- function(x) {
    c(as.list(summary(x)), list(sd = sd(x, na.rm = T)))
}
walpha[, summ(alpha)]
walpha[, summ(alpha), by = cube.type]
# 直方图(需要自己更换cube.type)
ggplot(walpha[cube.type == 'ZH'], aes(x = alpha)) +
    geom_histogram(bins = 1000) +
    xlim(c(-2.5, 2.5)) +
    xlab("Alpha") +
    ylab("Count") +
    theme_bw()
# 


# return converge ----
if (!exists("f.iposp")) ld(f.iposp)
if (!exists("f.cube.wret")) ld(f.cube.wret)
if (!exists("f.yw")) ld(f.yw)
# 每周收益由高到低分成10组，考察收益最高和最低的decile之间的gap是否会随着时间推移而缩小
# 通过调整cube.type，可以选择计算ZH还是SP的gap
gap <- f.cube.wret[(cube.type == 'SP') & !(cube.symbol %in% f.iposp)][, ":="(rank = ntile(wret, 10)), keyby = .(year, week)][, .(avgret = mean(wret)), keyby = .(year, week, rank)][f.yw, on = .(year, week), nomatch = 0]
# 同时画出高低两组
# 这个图画出来效果不好，所以现在只画gap，如下图
#gap[rank %in% c(1, 5)][order(date)] %>%
#dcast(date ~ rank, value.var = "avgret") %>%
    #ggplot(aes(x = date, ymin = `1`, ymax = `5`)) +
    #geom_ribbon() +
    #scale_x_date(date_labels = "%Y-%m") +
    #theme_bw()
# 只画gap
gap[rank %in% c(1, 10)][, .(retgap = max(avgret) - min(avgret)), keyby = date] %>%
    ggplot(aes(x = date, y = retgap)) +
    geom_bar(stat = "identity", width = 5, col = "grey") +
    geom_smooth(method = "loess", color = 'black', se = F) +
    xlab("") +
    ylab("Return Gap") +
    scale_x_date(date_labels = "%Y-%m") +
    theme_bw() 

rm(gap, f.cube.wret, f.iposp, f.yw)

# return rank2 ----
if (!exists("f.cube.wret")) ld(f.cube.wret)

# 每周，为每个组合排序，track it's ranking
rank <- copy(f.cube.wret)[, ":="(rank = ntile(wret, 10)), keyby = .(cube.type, year, week)][order(cube.type, cube.symbol, year, week)][, ":="(dif.rank = c(NA, diff(rank))), keyby = .(cube.type, cube.symbol)]
# 找出在一周之内ntrank下降9档的组合(最极端的情况)
r <- rank[dif.rank <= -9]

# 将数据集user.stock.wcube填充至每个uid每周都有观测，生成数据集uyws (uid, year, week, stock) ----
if (!exists("f.cyw")) ld(f.cyw)
if (!exists("f.cu")) ld(f.cu)
# cyw + cu = uyw
uyw <- f.cyw[f.cu, .(user.id, year, week), on = .(cube.symbol)]
if (!exists("f.user.stock, wcube")) ld(f.user.stock.wcube)
# uyw + s = uyws
null2na <- function(x) {
    if (is.null(x)) NA
    else x
}
uyws <- f.user.stock.wcube[uyw, on = .(user.id, year, week)][order(user.id, year, week), .(user.id, year, week, follow.stk = lapply(follow.stk, null2na), follow.stk.cum = lapply(follow.stk.cum, null2na))]
rm(uyw)



z <- uyws[1:1000][, ":="(follow.stk.cum = na.locf(follow.stk.cum, na.rm = F)), keyby = user.id]


# 导入f.user.stock.wret,
