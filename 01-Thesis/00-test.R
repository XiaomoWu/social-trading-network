setwd("C:/Users/rossz/OneDrive/SNT/01-Thesis")
ld(f.cube.rb)

f.cube.rb[, .(n = .N), keyby = cube.type]