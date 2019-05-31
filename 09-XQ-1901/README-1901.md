### 更新日志
* user.cmt 只有SP user的发帖成功抓取，因为雪球封的太厉害，ZH用户没法抓

### 数据集 (.Rdata)
#### 命名规则（r-prefix 和 f-prefix 的区别）
* r-prefix表示“raw”，即原始数据集。

* r-preifx 除了剔除实在没有必要保留的duplicates，尽可能不做改动
* r-prefix 中，lastcrawl 变量表示抓取时间。例如“1803”表示18年3月抓取。
* r-prefix，不带mst，表示某次抓取得到的 transaction set，没有与master做合并
* r-prefix，带mst，表示从最初不断积累的master数据集。每次抓取后，我们都会用transaction去更新master。
* f-prefix (deprecated)。现在已经不再生成f-prefix，放到具体项目中生成。

#### 每个数据集具体介绍
1. r.cube.info.mst
* Variables (24): `cube.type`, `cube.symbol`, `cube.name`, `owner.id`, `market`, `create.date` 
 `close.date`, `fans.count`, `net.value`, `rank.percent`, `annual.ret`, `monthly.ret` 
 `weekly.ret`, `daily.ret`, `bb.rate`, `listed.flag`, `update.date`, `style.name` 
 `style.degree`, `tag`, `tid`, `aid`, `description`, `lastcrawl` 

* 去重规则：No duplicates on `(cube.symbol, lastcrawl)`; if there is any, select obs with largest (most recent) `update.date`
    ```R
    r.cube.info.mst.1810[order(cube.symbol, lastcrawl, - update.date)
        ][, .SD[1], keyby = .(cube.symbol, lastcrawl)]
    ```

* Key：`setorder(r.cube.info.mst.1810, cube.symbol, - lastcrawl)`


2. r.cube.ret.mst
* Variables (6): `date`, `value`, `cube.symbol`, `cube.type`, `lable`, `lastcrawl`

*  去重规则：每个`(cube.symbol, date, value)`只能有一个obs，如果重复，选择`lastcrawl`大（最新）的那个。
    ```R
    r.cube.ret.mst.1810[order(cube.symbol, date, value, - lastcrawl), verbose = T
        ][, head(.SD, 1), keyby = .(cube.symbol, date, value), verbose = T]
    ```

* Key：`setkey(r.cube.ret.mst.1810, cube.symbol, date)`

3. r.cube.rb.mst
* Variable (26): `cube.symbol`, `cube.type`, `comment`, `cash`, `cash.value` `stock.id` `target.weight`, `proactive`, `weight`, `prev.net.value`, `prev.target.volume`, `price`, `prev.weight.adjusted`, `stock.symbol`, `volume`, `prev.weight`, `prev.volume`, `net.value`, `prev.price`, `prev.target.weight`, `target.volume`, `id`, `rebalancing.id`, `created.at`, `lastcrawl`, `stock.name` 

* 去重规则：类似r.cube.ret，如果重复，保留`lastcrawl`大（最新）的那个
    ```R
    unique(f.cube.rb.mst.1810, by = c("id", "rebalancing.id", "cube.symbol", "stock.symbol", "price", "created.at", "target.weight", "prev.weight.adjusted", "lastcrawl")) 
    ```

* Key: `setkey(id, rebalancing.id, cube.symbol, stock.symbol, price, created.at, target.weight, prev.weight.adjusted)`

4. r.user.info
* Variable (25): `user.id`, `lastcrawl`, `screen.name`, `gender` 
`province`, `city`, `verified.type`, `verified`,
`verified.realname`, `verified.description`, `fans.count`, `follow.count` 
 `stock.count`, `cube.count`, `status.count`, `donate.count` 
 `st.color`, `step`, `status`, `allow.all.stock` 
 `domain`, `type`, `url`, `description` 
 `last.status.id` 

 * 去重规则：no duplicates on `(user.id, lastcrawl)`; if there is any, select obs with largest 'status.id'
    ```R
    r.user.info.mst.1810[order(user.id, lastcrawl, - last.status.id)
        ][, .SD[1], keyby = .(user.id, - lastcrawl)]
    ```
* Key：`(user.id, lastcrawl)`

5. r.user.info.weibo
* Variables (3): `user.id`, `weibo.id`, `lastcrawl`
* 去重标准：no duplicates on `(user.id, weibo.id)`; if there is any, select obs with largest `lastcrawl`
    ```R
    setkey(r.user.info.weibo.mst.1810, user.id)
    ```

6. r.user.stock
* __Warning 1__: 该数据集存在duplicate！对于同一笔交易，可能存在不同的`stockName`（比如`''`和`dow jones`），我选择保留全部，留到具体项目中进一步去重。

* __Warning 2__: 从1901开始，`createAt`都是0！因此从1903开始，应该使用以下被注销的语句（如果那时createAt仍旧为0的话）`r.user.stock.mst.1901 <- unique(r.user.stock.mst.1901)`

* Variable (15): `code`, `comment`, `sellPrice`, `buyPrice`, `portfolioIds`, `createAt`, `targetPercent`, `isNotice`, `stockName`, `exchange`, `user.id`, `count`(目前为止自选股总数),  `is.public`, `lastcrawl`, `stockType` 

* 去重规则：No duplicates on `(code, comment, sellPrice, buyPrice, portfolioIds, createAt, targetPercent, isNotice, stockName, exchange, user.id, is.public)`; if there is any, select obs with largest 'lastcrawl'
    ```R
    r.user.stock.mst.1810 <- r.user.stock.mst.1810[order(code, comment, sellPrice, buyPrice, portfolioIds, createAt, targetPercent, isNotice, exchange, user.id, is.public, stockName, - lastcrawl)
        ][, .SD[1], keyby = .(code, comment, sellPrice, buyPrice, portfolioIds, createAt, targetPercent, isNotice, exchange, user.id, is.public, stockName)]
    ```

* Key: `setkey(r.user.stock.mst.1810, user.id, createAt, code)`

7. r.user.cmt
* Variables (): `id`, `user.id`, `title`, `created.at`, `comment.id`, `retweet.status.id`, `text`, `lastcrawl`, `source` 

* 去重规则：No duplicates on `(id, user.id, created.at, title, text, source, comment.id, retweet.status.id)`; if there is any, select obs with largest `lastcrawl`
    ```R
    r.user.cmt.mst.1810[order(id, user.id, created.at, title, source, comment.id, retweet.status.id, - lastcrawl)
        ][, .SD[1], keyby = .(id, user.id, created.at, title, source, comment.id, retweet.status.id)]
    ```
* Key: `setkey(r.user.cmt.mst.1810, user.id, created.at)`

8. r.user.fans
* Variables (5): `user.id`, `fans.count`, `anonymous.fans.count`, `fans`, `lastcrawl` 

* 去重规则：No duplicates on `(user.id, lastcrawl)`，相当于每个`lastcrawl`都是对network的一个snapshot。
    ```R
    unique(r.user.fans.mst.1810, by = c('user.id', 'lastcrawl'))
    ```

* Key: `setkey(r.user.fans.mst.1810, user.id, lastcrawl)`


9. r.user.follow
* Variables: `user.id`, `follow`, `lastcrawl`

* 去重规则：No duplicates on `(user.id, lastcrawl)`，相当于每个`lastcrawl`都是对network的一个snapshot。
    ```R
    unique(r.user.follow.mst.1810, by = c('user.id', 'lastcrawl'))
    ```
***

### 代码文件 (.R）
#### 01-import / 02-update / 03-filter简介
1. 01-importXQ.R
* 用于将数据从MongoDB中导入。注意，本文件只用于导入最新一次抓取的数据。与旧数据的整合是在02-update中完成的, 03-filter则用于进行最低限度的清洗

2. 02-update.R
* 用于将新抓取的数据合并至旧数据中。
* 本次执行的任务是将1806合并至mst.1803
* 注意！除了cube.ret & cube.rb，本脚本执行后生成的数据集（r-prefix）具有重复的观测，将会在 03-filter中进行去重.

3. 03-filter.R
* 本脚本用于对 r-prefix.mst 进行最低限度的去重、清洗
* 由于清洗的标准因项目而异，因此建议把filter.R文件写到项目中，而不是在这里



