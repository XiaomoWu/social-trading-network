ld(r.cube.info.mst.1901)

dt = r.cube.info.mst.1901[, tag := NULL]
fwrite(dt, 'dt.csv')

sv(dt)

getwd()

ld(dt, force=T)

sv
