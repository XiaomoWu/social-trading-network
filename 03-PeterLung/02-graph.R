# 首先生成 edges。我们会生成2组，分别为6、12个月----
putLocalFile("f.user.nwk.Rdata")
putLocalFile("f.cu.Rdata")

ld(f.user.nwk)
ld(f.cu)
# 第六个月时的edge list
system.time({
p.edges.6m <- f.user.nwk[year == 2017 & week == 1
    ][, .(source = user.id, target = nbr)
    ][!sapply(target, is.null)
    ][, .(target = unlist(target), period = "6m"), keyby = .(source)
    ][f.cu, on = .(target = cube.symbol), nomatch = 0
    ][, .(source = as.character(source), target = as.character(user.id), period)
    ] %>% unique()
}) #  2.51 / 3.57
# 第十二个时edge list
p.edges.12m <- f.user.nwk[year == 2017 & week == 30
    ][, .(source = user.id, target = nbr)
    ][!sapply(target, is.null)
    ][, .(target = unlist(target), period = "12m"), keyby = .(source)
    ][f.cu, on = .(target = cube.symbol), nomatch = 0
    ][, .(source = as.character(source), target = as.character(user.id), period)
    ] %>% unique()
# 包含全部可能的edge list
system.time({
p.edges.all <- f.user.nwk[order(user.id, year, week)
    ][, .SD[.N], keyby = .(user.id)
    ][!sapply(nbr, is.null)
    ][, .(to = unlist(nbr), period = "all"), keyby = .(from = user.id)
    ][f.cu, on = .(to = cube.symbol), nomatch = 0
    ][, .(source = as.character(from), target = as.character(user.id), period)
    ] %>% unique()
p.edges <- rbindlist(list(p.edges.6m, p.edges.12m, p.edges.all), use.names = T)
})# / 

sv(p.edges)
rm(p.edges.12m, p.edges.6m, p.edges.all)

# 接着生成nodes ----
ld(f.sp.owner)
p.nodes <- p.edges[, .(id = unique(c(source, target)))
    ][, ":="(is.sp = ifelse(id %in% f.sp.owner$user.id, T, F))]
sv(p.nodes)

# 输出nodes和edges ----
# 剩下的工作就交给 Gephi 了！
fwrite(p.nodes, "graph/nodes.csv")
fwrite(p.edges, "graph/edges.csv")

# 计算 degree 和 centrality，用于后面的回归（当然这些也可以在Gephi中完成，但是从Gephi中导入太麻烦了）
# 先计算centrality
system.time({
p.g.all <- graph_from_data_frame(p.edges[period == "all"], directed = T)
#sv(p.g.all)
cen.pr <- page.rank(p.g.all, directed = T)$vector
# cen.eigen <- eigen_centrality(p.g.all, directed = T) # eigen_centrality在我们这个情况下无法计算
})

# 再计算degree
system.time({
make_degree_tbl <- function(x, mode) {
    data.table(user.id = names(x), degree = x, degree.mode = mode)
}
degree.out <- degree(p.g.all, mode = "out")
degree.in <- degree(p.g.all, mode = "in")
degree.total <- degree(p.g.all, mode = "total")
# 把cen和degree合并到一个表中
degree <- rbindlist(list(
    make_degree_tbl(degree.out, "out"),
    make_degree_tbl(degree.in, "in"),
    make_degree_tbl(degree.total, "total")),
    use.names = T) %>% dcast(user.id ~ degree.mode, value.var = "degree")
p.cen <- data.table(user.id = names(cen.pr), cen= cen.pr)[degree, on = .(user.id)
    ][, .(user.id, cen, cen.scale = scale(cen), d.in = `in`, d.out = out, d.total = total)]
})

sv(p.cen)