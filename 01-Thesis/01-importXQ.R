library(mongolite)
# 本脚本用于将数据从MongoDB中导入。注意，本文件只用于导入最新一次抓取的数据。与旧数据的整合是在01-update中完成的

# 导入xq_user_info (37 s)----
system.time(
{
    r.user.info <- mongo(collection = 'xq_user_info_updt', db = 'xueqiutest')$find(field = '{"_id":0}') %>% setDT
    r.user.info <- r.user.info[, .(user.id = id, screen.name = screen_name, gender = gender, province = province, city = city, verified.type = verified_type, verified = verified, verified.realname = verified_realname, verified.description = verified_description, fans.count = followers_count, follow.count = friends_count, stock.count = stocks_count, cube.count = cube_count, status.count = status_count, donate.count = donate_count, st.color = st_color, step = step, status = status, allow.all.stock = allow_all_stock, domain = domain, type = type, url = url, description = description, last.status.id = last_status_id)]

    # 如果user.id重复，那么取status.id大的，即更新的
    r.user.info <- r.user.info[order(user.id, last.status.id)][, .SD[.n], keyby = .(user.id)]

    r.user.info <- r.user.info[, lapply(.SD, char2utf8)]
    setkey(r.user.info, user.id)
    sv(r.user.info)
})

# 导入xq_user_info_weibo (0.5 s)----
system.time(
{
    r.user.info.weibo <- mongo(collection = 'xq_user_info_weibo_updt', db = 'xueqiutest')$find(field = '{"_id":0}') %>% setDT
    r.user.info.weibo <- r.user.info.weibo[, .(user.id = user_id, weibo.id = weibo_id)]
    r.user.info.weibo[, uniqueN(user.id)] == nrow(r.user.info.weibo) # 验证每个user.id只占一行
    r.user.info.weibo <- r.user.info.weibo[, lapply(.SD, char2utf8)]
    sv(r.user.info.weibo)
})

# 导入xq_user_follow (29 s)----
system.time(
{
    r.user.follow <- mongo(collection = 'xq_user_follow_updt', db = 'xueqiutest')$find(field = '{"_id":0}') %>% setDT
    r.user.follow <- r.user.follow[, .(user.id = user_id, follow = follow)]
    r.user.follow <- unique(r.user.follow, by = "user.id")
    r.user.follow <- r.user.follow[, lapply(.SD, char2utf8)]
    setkey(r.user.follow, user.id)
    sv(r.user.follow)
})

# 导入xq_user_fans (126 s)----
system.time(
{
    r.user.fans <- mongo(collection = 'xq_user_fans_updt', db = 'xueqiutest')$find(field = '{"_id":0}') %>% setDT
    r.user.fans <- r.user.fans[, .(user.id = user_id, fans.count = count, anonymous.fans.count = anonymous_count, fans = fans, lastcrawl = lastcrawl)]
    r.user.fans <- unique(r.user.fans, by = c("user.id"))
    r.user.fans <- r.user.fans[, lapply(.SD, char2utf8)]
    setkey(r.user.fans, user.id)
    sv(r.user.fans)
})

# 导入xq_user_cmt ----
library(mongolite)
conn <- mongo(collection = 'xq_user_cmt_updt', db = 'xueqiutest', url = "mongodb://192.168.1.54:27017")
iter <- conn$iterate(query = '{}', field = '{"_id":0, "statuses.id":1, "statuses.user_id":1, "statuses.title":1, "statuses.created_at":1, "statuses.commentId":1, "statuses.retweet_status_id":1, "statuses.text":1}')
r.user.cmt <- data.table()
while (!is.null(res <- iter$batch(size = 1e5)))
{
    chunk <- lapply(res, `[[`, 1) %>% lapply(rbindlist, use.names = T, fill = T) %>% rbindlist(use.name = T, fill = T)
    r.user.cmt <- rbindlist(list(r.user.cmt, chunk), use.names = T, fill = T)
}

r.user.cmt <- r.user.cmt[, .(id, user.id = user_id, title, datetime = as.POSIXct(created_at / 1000, origin = "1970-01-01", tz = "GMT"), comment.id = commentId, retweet.status.id = retweet_status_id, text)][, date := as.Date(datetime)]
sv(r.user.cmt, T)

# 导入xq_user_stock (4 h)----
# 首先，在mongodb中运行以下程序，统计出每个uid对应有几个obs。这样做以便日后核对是否提取干净
# db.xq_user_stock_updt.aggregate([{$group:{_id:"$cube_symbol", "n":{$sum:1}}}, {$out:"uid.db"}])

# 建立向量symbol.db，里面包含unique(cube_symbol)
uid.db <- mongo(collection = 'uid.db', db = 'xueqiutest')$find(field = '{"_id":1, "n_db":1}') %>% setDT()
setnames(uid.db, names(uid.db), c("user_id", "n_db"))
uids <- uid.db[, unique(user_id)] %>% na.omit()

# 然后，为xq_user_stock建立index，db.xq_user_stock_updt.createIndex({user_id:1})

# 每一个uid读取称为1个data.table，满1e4个后合并成一个data.table，这样差不多有50个dt
conn <- mongo(collection = 'xq_user_stock_updt', db = 'xueqiutest')
system.time(
{
    n <- 1e2
    N <- as.integer(length(uids) / n + 1)
    #sink(file = "log.log")
    for (i in (1:1))
    print(sprintf('i:%s', i))
    {
        start <- (i - 1) * n + 1
        end <- min(c(i * n, length(uids)))
        l <- list()
        for (j in start:end)
        {
            # 在这里设置要读取的symbol
            uid <- uids[j]
            query <- sprintf('{"user_id": %s }', uid)
            field <- '{"_id":0}'
            l[[j]] <- conn$find(query = query, field = field) %>% setDT()
        }
        dt.name <- str_c("dt", i)
        assign(dt.name,
            value = rbindlist(l, use.names = TRUE, fill = TRUE))
        save(list = dt.name, file = str_c(dt.name, ".Rdata"))
    }
    sink()
    rm(i, j, l, n, N, query, field, start, end, uid, dt.name)
})

# 从磁盘读入50个dt进行合并
l <- list()
for (i in 1:50)
{
    print(paste(i, now()))
    #load(file = str_c("dt", i, ".Rdata")) # 如果working space中已经载入则不需要这条语句
    l[[i]] <- get(str_c("dt", i))
    rm(list = str_c("dt", i))
}
r.user.stock <- rbindlist(l, use.names = TRUE, fill = TRUE)
rm(l)

# 后期处理
r.user.stock <- r.user.stock[, .(user.id = user_id, count = count, is.public = isPublic, portfolios = lapply(portfolios, setDT), stocks = lapply(stocks, setDT))]
r.user.stock <- r.user.stock[, lapply(.SD, char2utf8)][, .SD[.N], keyby = .(user.id)]
# 把user.stock展平
r.user.stock[, ":="(rid = .I)]
s <- rbindlist(r.user.stock$stocks, use.name = T, fill = T, idcol = "rid")
r.user.stock <- s[r.user.stock[, ":="(stocks = NULL, portfolios = NULL)], on = .(rid), nomatch = 0][, ":="(rid = NULL)]
sv(r.user.stock)
rm(r)

sv(r.user.stock) # took 5.6 hs to save it!!!!!
setkey(r.user.stock, user.id)

# 导入xq_cube_info ----
conn <- mongo(collection = 'xq_cube_info_updt', db = 'xueqiutest')
# 先把除嵌套dict以外的所有变量都导入
r.cube.info <- conn$find(query = '{}', field = '{"_id":0, "last_rebalancing":0, "view_rebalancing":0, "owner":0, "last_success_rebalancing":0, "sell_rebalancing":0, "performance":0}') %>% setDT()
rm(conn)
# 然后逐一剔除没用的变量，选择需要的变量，生成r.cube.info
# .........（省略检验变量是否有用的代码若干行）
r.cube.info <- r.cube.info[, .(cube.type = cube_type,
    cube.symbol = symbol,
    cube.name = name,
    owner.id = owner_id,
    market = market,
    create.date = ymd(created_date_format),
    close.date = ymd(close_date),
    fans.count = follower_count,
    net.value = net_value,
    rank.percent = rank_percent,
    annual.ret = annualized_gain_rate,
    monthly.ret = monthly_gain,
    weekly.ret = weekly_gain,
    daily.ret = daily_gain,
    bb.rate = bb_rate,
    listed.flag = listed_flag,
    update.date = updated_at,
    style.name = style[[3]], style.degree = style[[4]],
    lastcrawl = lastcrawl,
    tag = tag, tid = tid, aid = aid,
    description = description)]
# 为cube.info设置key = cube_symbol
r.cube.info <- unique(r.cube.info, by = "cube.symbol")
r.cube.info <- r.cube.info[, lapply(.SD, char2utf8)]
setkey(r.cube.info, cube.symbol)
sv(r.cube.info)

# 导入cube.ret (~1 h)----
# 使用iterate/batch方式读入，用时仅为find方法的1/10不到
conn <- mongo(collection = 'xq_cube_ret_updt', db = 'xueqiutest', url = "mongodb://192.168.1.54:27017")
system.time({
    iter <- conn$iterate(query = '{}', field = '{"_id":0, "percent":0, "time":0}')
    r.cube.ret <- data.table()
    while (!is.null(res <- iter$batch(size = 1e7))) {
        chunk <- rbindlist(res, use.names = T, fill = T)
        r.cube.ret <- rbindlist(list(r.cube.ret, chunk), fill = T, use.names = T)
    }
    rm(iter, chunk)
})
r.cube.ret <- unique(r.cube.ret, by = c("cube_symbol", "date"))

# 对cube.ret进行后期处理（变量类型、set key、sv）
r.cube.ret[, ":="(date = ymd(date))]
setnames(r.cube.ret, names(r.cube.ret), str_replace(names(r.cube.ret), "_", "."))
r.cube.ret <- r.cube.ret[, lapply(.SD, char2utf8)]
setkey(r.cube.ret, cube.symbol, date)
sv(r.cube.ret) # 7 min

# 导入cube.rb ()----
# 首先，在mongodb中运行以下程序，统计出每个symbol对应有几个obs。这样做以便日后核对是否提取干净
#db.xq_cube_rb_updt.aggregate([{$group:{ _id:"$cube_symbol", "n":{$sum:1 }}}, {$out:"symbol.rb.db" }], { allowDiskUse:true })

# 建立向量symbol.db，里面包含unique(cube_symbol)
symbol.db <- mongo(collection = 'symbol.rb.db', db = 'xueqiutest', url = "mongodb://192.168.2.21:27017")$find(field = '{"_id":1, "n":1}') %>% setDT()
setnames(symbol.db, names(symbol.db), c("cube_symbol", "n_db"))
symbols <- symbol.db[, unique(cube_symbol)]

# 然后，为xq_cube_rb建立index，db.xq_cube_rb_updt.createIndex({cube_symbol : 1})

# 每一个symbol读取称为1个data.table，满1e4个后合并成一个data.table，这样差不多有113个dt
# 将这113个dt合并成r.cube.rb
conn <- mongo(collection = 'xq_cube_rb_updt', db = 'xueqiutest', url = "mongodb://192.168.2.21:27017")
system.time(
{
    print(paste("Start at:", now()))
    n <- 1e4
    N <- as.integer(length(symbols) / n + 1)
    for (i in (23:25))
    {
        start <- (i - 1) * n + 1
        end <- i * n
        print(sprintf('i:%s %s', i, now()))
        l <- list()
        for (j in start:end)
        {
            # 在这里设置要读取的symbol
            symbol <- symbols[j]
            #print(sprintf('i:%s j:%s symbol:%s', i, j, symbol))
            query <- sprintf('{"cube_symbol":"%s"}', symbol)
            field <- '{"_id":0, "holdings":0, "error_message":0, "error_status":0, "created_at":0, "updated_at":0, "prev_bebalancing_id":0, "new_buy_count":0, "diff":0, "exe_strategy":0, "rebalancing_histories.stock_label":0}'
            l[[j]] <- conn$find(query = query, field = field) %>% setDT()
        }
        dt.name <- str_c("dt.rb", i)
        assign(dt.name,
            value = rbindlist(l, use.names = TRUE, fill = TRUE))
        save(list = dt.name, file = str_c(dt.name, ".Rdata"), compress = F)
    }
    rm(i, j, l, n, N, query, field, start, end, symbol, symbols, conn, dt.name)
})
print(paste("Finished at:", now()))


# 从磁盘读入113个 dt.rb 进行合并
print(now())
r.cube.rb <- data.table()
    for (i in 1:113)
    {
        print(paste(i, now()))
        filename <- str_c("dt.rb", i)
        if (!exists(filename)) load(file = str_c(filename, ".Rdata")) # 如果working space中已经载入则不需要这条语句
        r.cube.rb <- rbindlist(list(r.cube.rb, get(filename)), use.names = T, fill = T)
        rm(list = filename)
    }
rm(i, filename)
print(now())

# 只保留status = "success" 以及 category == "user_rebalancing" 的观测
r.cube.rb <- r.cube.rb[status == 'success' & category == "user_rebalancing"] # 36886474 -> 33772116

# 只保留unique(id)的obs(ZH的情况)，以及保留所有SP
r.cube.rb <- rbindlist(list(r.cube.rb[cube_type == 'ZH', unique(.SD, by = "id")], r.cube.rb[cube_type == 'SP'])) # 33772116 -> 27281995

# r.cube.rb.without.history: 提取出除rebalancing_histories变量以外的所有变量
r.cube.rb[, ":="(rid = .I)]
r.cube.rb.without.history <- r.cube.rb[, .(cube_symbol, cube_type, cash_value, cash, comment, rid)]
save(list = "r.cube.rb.without.history", file = "r.cube.rb.without.history.Rdata", compress = F)
# r.cube.rb: 只包含rebalancing_histories
r.cube.rb[, ":="(category = NULL, status = NULL, cube_symbol = NULL, cube_type = NULL, id = NULL, cube_id = NULL, cash_value = NULL, cash = NULL, error_code = NULL, comment = NULL)]
# 把r.cube.rb重命名为r.cube.rb.only.history
r.cube.rb.only.history <- r.cube.rb
rm(r.cube.rb)

# 进行flatten操作 
system.time(
{
    r.cube.rb.only.history <- r.cube.rb.only.history[, rbindlist(rebalancing_histories, idcol = "rid", use.names = TRUE, fill = TRUE)]
})
r.cube.rb <- r.cube.rb.without.history[r.cube.rb.only.history, on = "rid"][, ":="(rid = NULL, stock_name = NULL, created_at = NULL, datetime = as.POSIXct(updated_at / 1000, origin = "1970-01-01"))][, ":="(updated_at = NULL)]
rm(r.cube.rb.only.history)
rm(r.cube.rb.without.history)

# 后期处理
setnames(r.cube.rb, names(r.cube.rb), str_replace_all(names(r.cube.rb), "_", "."))
r.cube.rb <- r.cube.rb[comment == '', comment := NA]
r.cube.rb <- r.cube.rb[, lapply(.SD, char2utf8)]
r.cube.rb[, ":="(target.weight = as.numeric(target.weight, prev.weight.adjusted = as.numeric(prev.weight.adjusted)))]
setkey(r.cube.rb, cube.symbol, datetime)
sv(r.cube.rb)


# 导入股票收益数据，用于3 factor等计算 ----
# 导入个股收益 ----
# 导入市场收益 （综合A股市场、沪深300）----
# 导入无风险收益 （deposit，shibor）----
# 导入三因子 ----