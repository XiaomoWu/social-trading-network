# 载入筛选后的样本 cid和uid ----
ld(cid)
ld(uid)

# cube.info的描述性统计----
# 总数
if (!exists('cube.info')) ld(cube.info)
nrow(cube.info)
cube.info[, table(cube.type)]

# 主理人数量
if (!exists('cube.info')) ld(cube.info)
cube.info[, uniqueN(owner.id)]
cube.info[, uniqueN(owner.id), by = cube.type]

# 最早最晚创建时间
if (!exists('cube.info')) ld(cube.info)
cube.info[, min(create.date), by = cube.type]

# 寿命
# 添加寿命（life）变量，并且剔除life<0的情况
if (!exists('cube.info')) ld(cube.info)
#sv(cube.info)
# life的描述性统计
if (!exists('cube.info')) ld(cube.info)
cube.info[, mean(life)]
cube.info[, mean(life), by = cube.type]

# 关注
if (!exists('cube.info')) ld(cube.info)
cube.info[, .(mean = mean(fans.count, na.rm = T), median = median(fans.count, na.rm = T))]
cube.info[, .(mean = mean(fans.count, na.rm = T), median = median(fans.count, na.rm = T)), by = cube.type]

# style
if (!exists('cube.info')) ld(cube.info)
cube.info[cube.type == 'ZH', table(style.name) / .N * 100]

# cube.rb的描述性统计----
summ <- function(x) {
    c(as.list(summary(x)), list(sd = sd(x, na.rm = T)))
}
narrow <- function(x, low = 1, high = 1) {
    x <- x[!is.na(x)]
    x[x %between% c(quantile(x, low / 100), quantile(x, (100 - high) / 100))]
}
# 总调仓次数
if (!exists("cube.rb")) ld(cube.rb)
cube.rb[cube.type == 'ZH', .(n = .N), keyby = cube.symbol][, summ(n)]
cube.rb[cube.type == 'SP', .(n = .N), keyby = cube.symbol][, summ(n)]
cube.rb[, .(n = .N), keyby = cube.symbol][, summ(n)]

# 平均调仓次数/间隔时间
if (!exists("cube.rb")) ld(cube.rb)
if (!exists("cube.info")) ld(cube.info)
nrb <- cube.rb[, .(n = .N), keyby = cube.symbol]
cinfo <- cube.info[nrb, on = "cube.symbol", nomatch = 0]
cinfo[, ":="(freq = n / life, dfreq = life / n)] # dfreq表示调仓间隔天数
cinfo[, summ(dfreq)]
cinfo[, summ(dfreq), keyby = cube.type]
rm(cinfo)

# 平均调仓幅度
if (!exists("cube.rb")) ld(cube.rb)
cube.rb[, narrow(abs(target.weight - prev.weight.adjusted)) %>% summ()]
cube.rb[, narrow(abs(target.weight - prev.weight.adjusted)) %>% summ(), keyby = cube.type]

# PLOT - 实盘调仓行为时间分布
cube.rb[cube.type == 'SP'] %>%
    ggplot(aes(x = as.POSIXct(as.ITime(datetime)))) +
    geom_histogram(bins = 50) +
    xlab("") +
    ylab("Count") +
    theme_bw()

# user.info的描述性统计----
if (!exists("r.user.info")) ld(r.user.info)
if (!exists("cube.info")) ld(cube.info)
user.info <- r.user.info[cube.info[order(owner.id, cube.type), .(cube.type = cube.type[1]), keyby = .(owner.id)], on = c(user.id = "owner.id"), nomatch = 0]
#sv(user.info)
# 人数与性别
user.info[, uniqueN(user.id)]
user.info[, uniqueN(user.id), keyby = gender]
user.info[, uniqueN(user.id), keyby = .(cube.type, gender)]
# 用户最多的省份
user.info[, .N, by = .(province)][order(-N)]
t <- user.info[, .N, keyby = .(cube.type, province)][order(cube.type, - N)]
rm(t)
# 认证用户数
user.info[verified | verified.realname, .N]
user.info[verified | verified.realname, .N, keyby = .(cube.type)]
# 平均关注/粉丝/自选股/组合/发帖数
col <- "status.count"
j <- parse(text = sprintf('.(mean = mean(%s, na.rm = T), median = median(%s, na.rm = T))', col, col))
user.info[, eval(j)]
user.info[, eval(j), keyby = cube.type]
rm(col, j)

# ret的描述性统计 ----
summ <- function(x) {
    c(as.list(summary(x)), list(sd = sd(x, na.rm = T)))
}
# 成立以来的总收益（from cube.info）
if (!exists("f.cube.info")) ld(f.cube.info)
f.cube.info[, summ((net.value - 1) * 100)]
f.cube.info[, summ((net.value - 1) * 100), by = cube.type]
# 年化收益（from cube.info）
if (!exists("cube.info")) ld(cube.info)
cube.info[, summ(annual.ret)]
# 至少有一人关注的组合的总收益（from cube.info）
if (!exists("cube.info")) ld(cube.info)
cube.info[fans.count >= 5, summ((net.value - 1) * 100)]
cube.info[fans.count >= 5, summ((net.value - 1) * 100), keyby = cube.type]
# 至少有一人关注的组合的年化收益（from cube.info）
if (!exists("cube.info")) ld(cube.info)
cube.info[fans.count >= 5, summ(annual.ret)]

# PLOT - 粉丝数与收益（累计分布）
if (!exists("cube.info")) ld(cube.info)
cum <- cube.info[cube.type == 'ZH'][order(annual.ret), .(annual.ret, fans.count, cumfans = cumsum(fans.count) / sum(fans.count) * 100)]
ggplot(cum, aes(x = annual.ret, y = cumfans)) +
    geom_line(size = 0.75) +
    xlim(c(-50, 400)) +
    xlab("Annualized Return in %") +
    ylab("Cumulative Distribution") +
    #theme_light() +
    theme_bw()
rm(cum)

# PLOT - 年化收益率的分布
if (!exists("cube.info")) ld(cube.info)
copy(cube.info)[cube.type == 'SP', annual.ret := ((net.value - 1) * 100)][,
    {   
        print(.BY)
        print(ggplot(.SD, aes(x = annual.ret)) +
            geom_histogram(bins = 300) +
            geom_density(kernel = 'gaussian') +
            xlim(c(-100, 100)) +
            ylab('Count') +
            xlab('Annulized Return in %') +
            theme_bw())
    },
    by = cube.type]

