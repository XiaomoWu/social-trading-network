ld(r.cube.info)

sp <- r.cube.info[cube.type == "SP", .(cube.symbol = unique(cube.symbol))]

ld(f.cube.info)
sp <- f.cube.info[cube.type == "SP", .(cube.symbol = unique(cube.symbol))]
