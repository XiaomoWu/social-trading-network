### 数据集 (.Rdata)
#### r-prefix 和 f-prefix 的区别
* 所有r-prefix数据集，都有lastcrawl变量表示抓取时间。例如“1803”表示1803抓取。

* r-prefix 前缀，不带mst，表示是1806抓取的原数据，没有与原来的数据做合并

* r-prefix前缀，带mst，是一直更新至1806的master数据集，除了r.cube.rb.mst 和 r.cube.ret.mst，别的都包含重复（每次lastcral都是全的）。

* r-prefix中，只有r.cube.mst.rb 和 r.cube.mst.ret 是去重过的，因为如果不去重，内存放不下。

* f-prefix是做过去重、清洗后的数据集。需要注意，f-prefix既保证了去重，又只对r-prefix做了最基本的清洗（限制条件最少），因此所有实证研究都应该使用 f-prefix。如果需要做特殊的清洗（例如存续期大于180天），则需要在研究中进一步清洗f-prefix。

#### 每个数据集具体介绍
1. r.cube.info.mst
* 去重规则：每个`(cube.symbol, lastcrawl)`只能有一个obs，如果重复，选择`update.date`最新的那个
    ```R
    r.cube.info.mst.1810[order(cube.symbol, lastcrawl, - update.date)
        ][, .SD[1], keyby = .(cube.symbol, lastcrawl)]
    ```

* setkey：`setorder(r.cube.info.mst.1810, cube.symbol, - lastcrawl)`

2. r.cube.ret.mst
* Variable: `date`, `value`, `cube.symbol`, `cube.type`, `lable`, `lastcrawl`
*  去重规则：每个`(cube.symbol, date, value)`只能有一个obs，如果重复，选择`lastcrawl`大（最新）的那个。
    ```R
    r.cube.ret.mst.1810[order(cube.symbol, date, value, - lastcrawl), verbose = T
        ][, head(.SD, 1), keyby = .(cube.symbol, date, value), verbose = T]
    ```

* key：`setkey(r.cube.ret.mst.1810, cube.symbol, date)`

3. r.cube.rb.mst
* Variable (26): `cube.symbol`, `cube.type`, `comment`, `cash`, `cash.value` `stock.id` `target.weight`, `proactive`, `weight`, `prev.net.value`, `prev.target.volume`, `price`, `prev.weight.adjusted`, `stock.symbol`, `volume`, `prev.weight`, `prev.volume`, `net.value`, `prev.price`, `prev.target.weight`, `target.volume`, `id`, `rebalancing.id`, `created.at`, `lastcrawl`, `stock.name` 

* 去重规则：类似r.cube.ret，如果重复，保留`lastcrawl`大（最新）的那个
    ```R
    unique(f.cube.rb.mst.1810, by = c("id", "rebalancing.id", "cube.symbol", "stock.symbol", "price", "created.at", "target.weight", "prev.weight.adjusted", "lastcrawl")) 
    ```

* key: `setkey(id, rebalancing.id, cube.symbol, stock.symbol, price, created.at, target.weight, prev.weight.adjusted)`
***

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
* key：`(user.id, lastcrawl)`

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

* key: `setkey(r.user.stock.mst.1810, user.id, createAt, code)`

7. r.user.cmt
* Variables (): `id`, `user.id`, `title`, `created.at`, `comment.id`, `retweet.status.id`, `text`, `lastcrawl`, `source` 

* 去重规则：No duplicates on `(id, user.id, created.at, title, text, source, comment.id, retweet.status.id)`; if there is any, select obs with largest `lastcrawl`
    ```R
    r.user.cmt.mst.1810[order(id, user.id, created.at, title, source, comment.id, retweet.status.id, - lastcrawl)
        ][, .SD[1], keyby = .(id, user.id, created.at, title, source, comment.id, retweet.status.id)]
    ```
* key: `setkey(r.user.cmt.mst.1810, user.id, created.at)`

8. r.user.fans
* Variables (5): `user.id`, `fans.count`, `anonymous.fans.count`, `fans`, `lastcrawl` 

* 去重规则：No duplicates on `(user.id, lastcrawl)`，相当于每个`lastcrawl`都是对network的一个snapshot。
    ```R
    unique(r.user.fans.mst.1810, by = c('user.id', 'lastcrawl'))
    ```

* key: `setkey(r.user.fans.mst.1810, user.id, lastcrawl)`


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



