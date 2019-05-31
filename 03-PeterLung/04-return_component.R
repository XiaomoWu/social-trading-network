# 这个脚本用来检验 A和P的return是否有不同（panelty，sigma等）
# 在这个脚本中，AP是用整个样本期来进行分类的
ld(ap.all)
ld(p.cube.wret)
ld(p.wret.mkt)

ret.comp <- p.cube.wret[, .(cube.symbol, year, week, wret)
    ][p.wret.mkt, on = .(year, week), nomatch = 0
    ][ap.all, on = .(cube.symbol), nomatch = 0]

fit_ret_comp <- function(data) {
    lm(wret ~ wret.mkt, data = data) # 这里不能用fixed，因为一旦fixed每个组合都会产生自己的alpha
}
fit.a <- fit_ret_comp(ret.comp[is.a == T])
fit.p <- fit_ret_comp(ret.comp[is.a == F])

# 结果做出来很好！和模型预期一样！
fit.a; sd(fit.a$residuals)
fit.p; sd(fit.p$residuals)