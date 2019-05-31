# 导入个股收益 ----
# 直接读入外部 trd.dalyr.Rdata 文件 - 2017-09-30（不再从MySQL中读取）
ld(trd.dalyr)
r.dstk <- trd.dalyr[trddt >= as.Date("2014-01-01"), .(stkcd = stkcd, date = ymd(trddt), vol = dnshrtrd, amt = dnvaltrd, mv = dsmvosd, dret = dretnd, adjprc = adjprcnd, mkttype = markettype)] # mv: market value, mkttype: 1=上海A，2=上海B，4=深圳A，8=深圳B,  16=创业板
setkey(r.dstk, stkcd, date)
sv(r.dstk)

# 导入三因子 ----
ld(sdi.thrfacday)
r.d3f <- sdi.thrfacday[markettypeid == "P9709", .(date = trddt, rm = riskpremium1, smb = smb1, hml = hml1)]
sv(r.d3f)