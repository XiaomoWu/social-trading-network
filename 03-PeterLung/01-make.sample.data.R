# generate sample data ----
# user.info
ld(f.user.info)
f.user.info[, .(user.id, screen.name, province, city, fans.count, follow.count, stock.count, cube.count, status.count, description, lastcrawl)][1:1000] %>% write.xlsx("USER.info.xlsx")

# user.stock
ld(f.user.stock)
f.user.stock[, .(user.id, stock.symbol, stock.exchange = cube.type, create.date, buy.price, sell.price)][1:1000] %>% write.xlsx("USER.stock.xlsx")

# user.fans
ld(r.user.fans)
r.user.fans[5000:6000] %>% write.xlsx("USER.fans.xls")

# user.follow
ld(r.user.follow)
r.user.follow[1:1000] %>% write.xlsx("USER.follow.xlsx")

# portfolio.info
ld(f.cube.info)
f.cube.info[, .(cube.symbol, cube.type, cube.name, owner.id, market, create.date, close.date, fans.count, rank.percent, style.name, tag, lastcrawl, description)
    ][cube.type == "SP", cube.type := "SF"
    ][cube.type == "ZH", cube.type := "SP"
    ][1:1000] %>% write.xlsx("PORTFOLIO.info.xlsx")

# portfolio.rb
ld(f.cube.rb)
f.cube.rb[comment != "", .(cube.symbol, cube.type, comment, stock.symbol, prev.weight.adjusted, target.weight, price, id, datetime)
    ][cube.type == "SP", cube.type := "SF"
    ][cube.type == "ZH", cube.type := "SP"
    ][1:1000] %>% write.xlsx("PORTFOLIO.rb.xlsx")

# portfolio.ret
ld(f.cube.ret)
f.cube.ret[, .(cube.symbol, cube.type, value, date)
    ][cube.type == "SP", cube.type := "SF"
    ][cube.type == "ZH", cube.type := "SP"
    ][1:1000] %>% write.xlsx("PORTFOLIO.ret.xlsx")


# 把p.cube.rb中出现的股票代码给Prof. Lung, 让他计算intraday volatility ----
stock.symbol <- p.cube.rb[str_sub(stock.symbol, 1, 2) %in% c("SZ", "SH"), .(stock.symbol = unique(stock.symbol))
    ][, .(exchange = str_sub(stock.symbol, 1, 2), stock.symbol = str_sub(stock.symbol, 3, -1))
    ][order(exchange, stock.symbol)
    ][exchange != "" & stock.symbol != ""]

fwrite(stock.symbol, "stock.symbol.csv")