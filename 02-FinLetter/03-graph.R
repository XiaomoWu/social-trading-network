# 从user.follow数据集建立graph ----
# 原则，除了centrality，尽量用data.table而不用igraph，否则效率非常低
# 要保证from和to都来自uid
library(igraph)
ld(uid)
if (!exists("f.user.follow")) ld(f.user.follow)
gdt <- copy(f.user.follow)[, ":="(follow = lapply(follow, as.character))][, .(to = unlist(follow)), keyby = .(from = user.id)][to %in% uid] %>% unique()
f.g <- graph_from_data_frame(gdt)
sv(f.g)
ecount(f.g) # 12157839
vcount(f.g) # 384353 有些人虽然有follow，但follow却是没有建立过组合的人，故vcount比uid的总数(390078)要少一些

# 计算centrality -----
system.time({
cen <- page_rank(g, directed = T)$vector
cen <- data.table(user.id = V(g)$name, cen = cen)

}) # 2 min

