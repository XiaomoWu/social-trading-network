### 数据集 (.Rdata)
#### r-prefix 和 f-prefix 的区别
* 所有r-prefix数据集，都有lastcrawl变量表示抓取时间。例如“1803”表示1803抓取。

* r-prefix 前缀，不带mst，表示是1806抓取的原数据，没有与原来的数据做合并

* r-prefix前缀，带mst，是一直更新至1806的master数据集，除了r.cube.rb.mst 和 r.cube.ret.mst，别的都包含重复（每次lastcral都是全的）。

* r-prefix中，只有r.cube.mst.rb 和 r.cube.mst.ret 是去重过的，因为如果不去重，内存放不下。

* f-prefix是做过去重、清洗后的数据集。需要注意，f-prefix既保证了去重，又只对r-prefix做了最基本的清洗（限制条件最少），因此所有实证研究都应该使用 f-prefix。如果需要做特殊的清洗（例如存续期大于180天），则需要在研究中进一步清洗f-prefix。

#### 每个数据集具体介绍
1. r.cube.mst.ret
* Variable: `date`, `value`, `cube.symbol`, `cube.type`, 'lable`, `lastcrawl`
* 

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
* 本脚本用于对 r-prefix.mst 进行去重、清洗（例如剔除存续期小于阈值的cube），最终生成的 f-prefix 
* 本脚本同时对 02-update 生成的 f.cube.rb.mst 和 f.cube.ret.mst 进行清洗



