ld(r.user.cmt.mst.1803)

r.user.cmt.mst.1803[, ':='(nid = .I)]
r.user.cmt.mst.1803[, ':='(part = ntile(nid, 10))]

for (i in 4:10) {
    name <- str_c('cmt.p', i)
    #assign(name, r.user.cmt.mst.1803[part == i])
    save(list = name, file = str_c(name, '.Rdata'))
}

ld(r.cube.info.mst.1803)
r.cube.info.mst.1803[cube.type == "SP", .(cube.num = as.numeric(str_sub(cube.symbol, start = 3)))
    ][, .(cube.num)
    ][order(cube.num)
    ][1:10]

