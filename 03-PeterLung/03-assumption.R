# 这个脚本用来验证sending function和receiving function

SDATE <- as.Date("2016-07-01")
# 首先验证sending function
# 1. 用table ----
ld(p.cube.wret, T)
ld(r.user.cmt) # 用时 3.5 分钟
ld(f.cu)
# cmt.n: 发帖统计。msg.n: 全部发帖；msg.n.user：用户发帖（排除“我刚刚”）
cmt.n <- r.user.cmt[date >= SDATE, .(msg.n = .N, msg.n.user = .N - sum(str_sub(text, 1, 3) == "我刚刚")), keyby = .(user.id, year = year(date), week = week(date))]
sv(cmt.n)

# 把cmt.n和周收益合并，只计算 SP
send <- cmt.n[p.cube.wret[, .(cube.symbol, year, week, wret, date)][f.cu, on = .(cube.symbol), nomatch = 0],
    on = .(user.id, year, week)
    ][order(user.id, year, week, cube.symbol)
    ][, ":="(msg.n = fillna(msg.n), msg.n.user = fillna(msg.n.user))] # 有些week没有msg，用 0 填充

# 每周按照ret分quintle，看和msg.n的关系
ret.rank <- send[cube.type == "SP", .(msg.n = mean(msg.n), msg.n.user = mean(msg.n.user), wret = mean(wret), wret.max = max(wret)), keyby = .(user.id, year, week)
    ][, .(user.id, wret.rank = ntile(wret, 5), wret.max.rank = ntile(wret.max, 5), msg.n, msg.n.user), keyby = .(year, week) # 把同一个user.id的cube都集合到一块
    ]
# 按照msg.n的数量
ret.rank[, .(msg.n = sum(msg.n > 1, na.rm = T) / .N, msg.n.user = sum(msg.n.user > 1, na.rm = T) / .N), keyby = .(wret.max.rank)]
# 按照msg.n的概率
ret.rank[, .(msg.n = mean(msg.n, na.rm = T), msg.n.user = mean(msg.n.user, na.rm = T)), keyby = .(wret.max.rank)]

# 2. 用regression ----
ld(f.sp.owner)
ld(f.user.nwk)
ld(f.ywd)
# send2是上面send的加强版，除了自身的 ret 和 msg 以外，还包含了 nbr 的信息
# send2 只包含 SP 
send2 <- f.user.nwk[, .(nbr = unlist(nbr)), keyby = .(user.id, year, week)
    ][send[, .(cube.symbol, year, week, wret, msg.n, msg.n.user)], on = c(nbr = "cube.symbol", "year", "week"), nomatch = 0
    ][, .(wret.nbr = mean(wret), wret.max.nbr = max(wret), msg.n.nbr = mean(msg.n), msg.n.user.nbr = mean(msg.n.user)), keyby = .(user.id, year, week) # 计算 peer effect
    ][f.sp.owner, on = .(user.id), nomatch = 0
    ][send[, .(cube.symbol, year, week, wret, msg.n, msg.n.user)], on = .(cube.symbol, year, week), nomatch = 0
    ][, ":="(wret.gap.nbr = wret.nbr - wret)
    ][f.ywd, on = .(year, week), nomatch = 0]
sv(send2)

# run regression
file <- "C:/Users/rossz/OneDrive/SNT/03-PeterLung/reg.html"
stars <- c(0.01, 0.05, 0.1)

fit <- plm(msg.n.user ~ wret + I(wret^2) + wret.nbr + msg.n.user.nbr, data = send2, model = "within", effect = "twoways", index = c("cube.symbol", "date"))
summary(fit)
screenreg(fit)
htmlreg(fit, stars = stars, file = file, digits = 4)


# receiving function ----
ld(p.cube.rb)
ld(rb.char.wk)
ld(send2)
ld(p.cen)
# receive2 由 send2 改进而来，增加了 (1) trading freq(tf) ，(2) rb.char , (3) cen+degree
# receive2 只包含 SP
receive <- p.cube.rb[, .(tf = .N, tf.buy = sum(amt > 0)), keyby = .(cube.symbol, year = year(date), week = week(date))
    ][send2, on = .(cube.symbol, year, week)
    ][order(user.id, year, week)
    ][, ":="(tf = fillna(tf), tf.buy = fillna(tf.buy))
    ]
#receive2 <- rb.char.wk[receive, on = .(cube.symbol, user.id, year, week)
    #][, (5:26) := lapply(.SD, na.locf, F), keyby = .(user.id, cube.symbol), .SDcols = divrank:profitmarginrank # rb.char.wk不一定每周都有（因为有几周可能没有买入交易），因此用 locf 填充
    #][order(user.id, year, week)
    #][, ":="(user.id = as.character(user.id))
    #][p.cen, on = .(user.id), nomatch = 0
    #] %>% na.omit()
sv(receive2)
 

# run regression
file <- "C:/Users/rossz/OneDrive/SNT/03-PeterLung/reg.html"
stars <- c(0.01, 0.05, 0.1)

# 所有individual effects都用fixed effects capture
fit <- plm(tf.buy ~ I((wret.nbr - wret) ^ 2) + I(wret.nbr - wret) + I(as.factor(date)), data = receive2, index = c("cube.symbol", "date")) # 把time-effect用as.factor(date)表示，效果很好，R能达到0.03！

# 不用fixed effects，手动加入 cen和degree
fit <- lm(tf.buy ~ I((wret.nbr - wret) ^ 2) + I(wret.nbr - wret) + cen.scale + d.out + I(as.factor(date)), data = receive2) # 把time-effect用as.factor(date)表示，效果很好，R能达到0.03！

htmlreg(fit, stars = stars, file = file, digits = 4)
