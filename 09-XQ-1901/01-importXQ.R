library(mongolite)
# 本脚本用于将数据从MongoDB中导入。注意，本文件只用于导入最新一次抓取的数据。与旧数据的整合是在01-update中完成的, 02-filter用于进行最低限度的清晰
# 以下所有时间估计都是在211-ADV中得到的

# 导入xq_user_info (22 s)----
system.time({
    r.user.info <- mongo(collection = 'xq_user_info', db = 'XQ-1901', url = "mongodb://localhost:27018")$find(field = '{"_id":0}') %>% setDT
    r.user.info <- r.user.info[, .(user.id = id, screen.name = screen_name, gender = gender, province = province, city = city, verified.type = verified_type, verified = verified, verified.realname = verified_realname, verified.description = verified_description, fans.count = followers_count, follow.count = friends_count, stock.count = stocks_count, cube.count = cube_count, status.count = status_count, donate.count = donate_count, st.color = st_color, step = step, status = status, allow.all.stock = allow_all_stock, domain = domain, type = type, url = url, description = description, last.status.id = last_status_id)]

    # 如果user.id重复，那么取status.id大的，即更新的
    r.user.info <- r.user.info[order(user.id, - last.status.id)][, .SD[1], keyby = .(user.id)]

    # 新建 lastcrawl 变量
    r.user.info[, ":="(lastcrawl = 1901)]

    r.user.info.1901 <- r.user.info
    setkey(r.user.info.1901, user.id)
    sv(r.user.info.1901)
})

# 导入xq_user_info_weibo (0.5 s)----
system.time({
    r.user.info.weibo <- mongo(collection = 'xq_user_info_weibo', db = 'XQ-1901', url = "mongodb://localhost:27018")$find(field = '{"_id":0}') %>% setDT
    r.user.info.weibo <- r.user.info.weibo[, .(user.id = user_id, weibo.id = weibo_id)]
    r.user.info.weibo[, uniqueN(user.id)] == nrow(r.user.info.weibo) # 验证每个user.id只占一行

    # 添加 lastcrawl
    r.user.info.weibo[, ":="(lastcrawl = 1901)]

    r.user.info.weibi.1901 <- r.user.info.weibo
    sv(r.user.info.weibo.1901)
})

# 导入xq_user_follow (20 s)----
library(mongolite)
system.time({
    r.user.follow <- mongo(collection = 'xq_user_follow', db = 'XQ-1901', url = "mongodb://localhost:27018")$find(field = '{"_id":0}') %>% setDT
    # 为 lastcrawl 设定值为
    r.user.follow <- r.user.follow[, .(user.id = user_id, follow = follow, lastcrawl = 1810)]
    # 由于同一个 user.id 可能有很多page，因而需要把一个user.id下面的所有follow合并
    r.user.follow. <- r.user.follow[, .(follow = list(unlist(follow)), lastcrawl = lastcrawl[1]), keyby = user.id]

    r.user.follow.1901 <- r.user.follow
    sv(r.user.follow.1901)
})

# 导入xq_user_fans (20 s)----
library(mongolite)
system.time({
    r.user.fans <- mongo(collection = 'xq_user_fans', db = 'XQ-1901', url = "mongodb://localhost:27018")$find(field = '{"_id":0}') %>% setDT
    r.user.fans <- r.user.fans[, .(user.id = user_id, fans.count = count, anonymous.fans.count = anonymous_count, fans = fans)]

    # 由于同一个 user.id 可能有很多page，因而需要把一个user.id下面的所有fans合并
    r.user.fans <- r.user.fans[, .(fans.count = fans.count[1], anonymous.fans.count = anonymous.fans.count[1], fans = list(unlist(fans))), keyby = user.id]

    # 增加lastcrawl = 1901
    r.user.fans[, lastcrawl := 1901]

    r.user.fans.1901 <- r.user.fans
    sv(r.user.fans.1901)
})

# 导入xq_user_cmt ----
library(mongolite)
conn <- mongo(collection = 'xq_user_cmt', db = 'XQ-1901', url = "mongodb://localhost:27018")
iter <- conn$iterate(query = '{}', field = '{"_id":0, "statuses.id":1, "statuses.user_id":1, "statuses.title":1, "statuses.created_at":1, "statuses.commentId":1, "statuses.retweet_count":1, "statuses.reply_count":1, "statuses.retweet_status_id":1, "statuses.text":1, "statuses.source":1}')

system.time({
r.user.cmt <- data.table()
iter.count <- 0
while (!is.null(res <- iter$batch(size = 1e6))) {
    chunk <- lapply(res, `[[`, 1) %>% lapply(rbindlist, use.names = T, fill = T) %>% rbindlist(use.name = T, fill = T)
    iter.count <- iter.count + 1
    cat(iter.count, '\n')
    r.user.cmt <- rbindlist(list(r.user.cmt, chunk), use.names = T, fill = T)
}
}) # 20 min @1e6

r.user.cmt <- r.user.cmt[, .(id, user.id = user_id, title, created.at = as.POSIXct(created_at / 1000, origin = "1970-01-01", tz = "GMT"), comment.id = commentId, retweet.status.id = retweet_status_id, text, source)]

# 添加 lastcrawl
r.user.cmt[, ":="(lastcrawl = 1901)]

r.user.cmt.1901 <- r.user.cmt
sv(r.user.cmt.1901) # 4.1 min

# 导入xq_user_stock ----
# 先在 mongodb 中运行以下代码，进行flatten操作，最终生成 r_user_stock
# ~= 5 min
#db.getCollection('xq_user_stock').aggregate([
#{"$project":{"count":1, "isPublic":1, "stocks":1, "user_id":1}},
#{"$unwind":"$stocks"},
#{"$project":{"_id":0, "user_id":"$user_id", "count":1, "isPublic":1, "code":"$stocks.code", "comment":{"$ifNull":["$stocks.comment", ""]}, 
#"sellPrice":"$stocks.sellPrice", "buyPrice":"$stocks.buyPrice", "portfolioIds":"$stocks.portfolioIds",
#"createAt":"$stocks.createAt", "targetPercent":"$stocks.targetPercent", "isNotice":"$stocks.isNotice",
#"stockName":{"$ifNull":["$stocks.stockName",""]}, 
#"exchange":{"$ifNull":["$stocks.exchange",""]},
#"stockType":{"$ifNull":["$stocks.stockType",""]}}},
#{"$out":"r_user_stock"}
#], {"allowDiskUse":true})

# 然后再把flatten后的数据集 (r_user_stock) 导入R
library(mongolite)
conn <- mongo(collection = 'r_user_stock', db = 'XQ-1901', url = "mongodb://localhost:27018")
iter <- conn$iterate(field = '{"_id":0}')
r.user.stock <- data.table()

system.time({
    iter.count <- 0
    while (!is.null(res <- iter$batch(size = 1e7))) {
        chunk <- rbindlist(res, use.names = T, fill = T)
        r.user.stock <- rbindlist(list(r.user.stock, chunk), use.names = T, fill = T)
        iter.count <- iter.count + 1
        cat(iter.count, '\n')
    }
}) # 4.6 min@1e7 

# 添加 lastcrawl，本次为 1901
r.user.stock[, ":="(lastcrawl = 1901)]
setnames(r.user.stock, c("isPublic", "user_id"), c("is.public", "user.id"))
setkey(r.user.stock, user.id)
r.user.stock.1901 <- r.user.stock
sv(r.user.stock.1901) # 37s

# 导入xq_cube_info ----
conn <- mongo(collection = 'xq_cube_info', db = 'XQ-1901', url = "mongodb://localhost:27018")
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
    tag = tag, tid = tid, aid = aid,
    description = description)]
# 为cube.info设置lastcrawl=1901
r.cube.info[, lastcrawl := 1901]
# 在同一次抓取冲，cube_symbol不能重复
r.cube.info <- unique(r.cube.info, by = "cube.symbol")
r.cube.info.1901 <- r.cube.info
sv(r.cube.info.1901)

# 导入xq_cube_ret ----
# 使用iterate/batch方式读入，用时仅为find方法的1/10不到
# 这步非常吃内存，强烈建议清空内存，重启mongodb service！
library(mongolite)
conn <- mongo(collection = 'xq_cube_ret', db = 'XQ-1901', url = "mongodb://localhost:27018")
system.time({
    iter <- conn$iterate(query = '{}', field = '{"_id":0, "percent":0, "time":0}')
    r.cube.ret <- data.table()
    iter.count <- 0
    while (!is.null(res <- iter$batch(size = 1e7))) {
        chunk <- rbindlist(res, use.names = T, fill = T)
        r.cube.ret <- rbindlist(list(r.cube.ret, chunk), fill = T, use.names = T)
        iter.count <- iter.count + 1
        cat(iter.count, '\n')
    }
    rm(iter, chunk, iter.count)
}) # 57 min @ size = 1e7
r.cube.ret <- unique(r.cube.ret, by = c("cube_symbol", "date"))

# 对cube.ret进行后期处理（变量类型、set key、sv）
# 设置 lastcrawl = 1901
r.cube.ret[, ":="(date = fast_strptime(date, "%Y-%m-%d", lt = F) %>% as.Date(), lastcrawl = 1901)]
setnames(r.cube.ret, names(r.cube.ret), str_replace(names(r.cube.ret), "_", "."))
r.cube.ret.1901 <- r.cube.ret
sv(r.cube.ret.1901) # 6.8 min

# 导入xq_cube_rb ----
# 使用MRO 3.4.4导入会出问题，使用MRO 3.5.0就没问题
# 首先在mongodb中运行以下程序，把所有的null field都剔除（recursively）
# ~= 4 h
#const remove = (data) => {
#for (let key in data) {
#const val = data[key];
#if (val == null) {
#delete data[key];
#} else if (Array.isArray(val)) {
#val.forEach((v) => {
#remove(v);
#});
#}
#}
#return data;
#}

#db.getCollection('xq_cube_rb').find({}).forEach((data) => {
#data = remove(data);
#db.xq_cube_rb.save(data);
#})

system.time({
    library(mongolite)
    conn <- mongo(collection = 'xq_cube_rb', db = 'XQ-1901', url = "mongodb://localhost:27018")
    # cube.rb 一共有三个id，分别为“id”（top level）， “id”（在 rebalancing_histories节点中），“rebalancing_id”（也在 rebalancing_histories 节点中）。其中，top level 的 id 和 reblancing_id 是一样的。所以我们在导入时选择 "id":0
    iter <- conn$iterate(field = '{"_id":0, "id":0, "holdings":0, "error_message":0, "error_status":0, "created_at":0, "updated_at":0, "prev_bebalancing_id":0, "new_buy_count":0, "diff":0, "exe_strategy":0, "rebalancing_histories.stock_label":0, "rebalancing_histories.created_at":0}')

    r.cube.rb <- data.table()
    iter.count <- 0
    while (!is.null(res <- iter$batch(size = 1e6))) {
        # res.nested 只包含“展开”后的reblancing_histories节点
        res.nested <- lapply(res, `[[`, "rebalancing_histories") %>% lapply(rbindlist, use.names = T, fill = T) %>% rbindlist(use.names = T, fill = T, idcol = "rid")
        # chunk 包含除了 rebalancing_histories 以外的所有非嵌套节点
        chunk <- lapply(res, function(ele) { ele[["rebalancing_histories"]] <- NULL; ele }) %>% rbindlist(use.names = T, fill = T, idcol = "rid")
        # 合并两者
        chunk <- res.nested[chunk, on = .(rid), nomatch = 0]
        # 用 chunk 更新 r.cube.rb
        r.cube.rb <- rbindlist(list(r.cube.rb, chunk), use.names = T, fill = T)
        iter.count <- iter.count + 1
        cat(iter.count, '\n')
    }
    rm(res.nested, chunk, res, iter, iter.count, conn)
}) # 85 min

# 后期处理
setnames(r.cube.rb, names(r.cube.rb), str_replace_all(names(r.cube.rb), "_", "."))
r.cube.rb <- r.cube.rb[comment == '', comment := NA]
r.cube.rb[, ":="(rid = NULL, target.weight = as.numeric(target.weight), prev.weight.adjusted = as.numeric(prev.weight.adjusted))
    ][, ":="(datetime = as.POSIXct(updated.at / 1000, origin = "1970-01-01"))
    ][, ":="(updated.at = NULL)] # 把 UNIX 时间戳转换为 POSIXct
setnames(r.cube.rb, "datetime", "created.at")

# 赋值：lastcrawl=1901
r.cube.rb[, ":="(lastcrawl = 1901)]

# 只保留status = "success" 以及 category == "user_rebalancing" 的观测
r.cube.rb <- r.cube.rb[status == 'success' & category == "user_rebalancing"] 
r.cube.rb[, ":="(status = NULL, category = NULL, cube.id = NULL, error.code = NULL)]

# setkey
setkey(r.cube.rb, cube.symbol, lastcrawl)

# save
r.cube.rb.1901 <- r.cube.rb
sv(r.cube.rb.1901)
rm(r.cube.rb)
