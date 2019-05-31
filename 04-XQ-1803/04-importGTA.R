# 将文件编码修改为UTF-8-------------------
setwd('C:/Users/rossz/OneDrive/PR/Database-CSMar-Updating')

# 导入 trd.dalyr --------------------------------------------------
# 27个文件使用232s
system.time({
    dir <- "C:/Users/rossz/OneDrive/PR/Database-CSMar-Updating/trd.dalyr-2018-04-30/trd.daylr-2017-09-01-2018-04-30"
    file.paths <- list.files(path = dir,
                             pattern = "TRD_Dalyr\\(?[0123456789]*\\)?\\.txt$",
                             full.names = T)
    file.names <- list.files(path = dir,
                             pattern = "TRD_Dalyr\\(?[0123456789]*\\)?\\.txt$",
                             full.names = F)
    file.list <- list()
    for (i in seq_along(file.paths)) {
        t <- system.time({
            path <- file.paths[i]
            name <- file.names[i]
            cat(i, " -- ", name)
            file <- read.delim(path, header = T, fileEncoding = "UTF-8",
                               stringsAsFactors = F,
                               colClasses = c("character",
                                              "character",
                                              rep("numeric", 12),
                                              "character",
                                              "character",
                                              "character")) %>% setDT()
            file[, ":="(Trddt = as.Date(Trddt,format = "%Y-%m-%d"),
                        Capchgdt = as.Date(Capchgdt,format = "%Y-%m-%d"))]
            file.list[[name]] <- file
        })
        cat(" -- ", t[3], "\n")
    }
    trd.dalyr.append <- rbindlist(file.list, use.names = T) # rbindlist
    setnames(trd.dalyr.append, names(trd.dalyr.append), tolower(names(trd.dalyr.append))) # setname
    setorder(trd.dalyr.append, stkcd, trddt) # sort
    trd.dalyr.append <- unique(trd.dalyr.append) # delete duplicate
    # 把文件合并至 trd.dalyr.2017-09-30，生成 trd.dalyr.mst.1804
    ld(`trd.dalyr.2017-09-30`)
    trd.dalyr.mst.1804 <- rbindlist(list(trd.dalyr.append, trd.dalyr)) %>% unique(by = c('stkcd', 'trddt')) %>% setorder(stkcd, trddt)
    sv(trd.dalyr.mst.1804)
})

# 导入 trd.dalym --------------------------------------------------
dir <- "C:/Users/Root/OneDrive/PrivateResearch/Database-CSMar-Updating/trd.dalym-2015-10-10"
file.paths <- list.files(path = dir,
                         pattern = "^TRD_Dalym.txt$",
                         full.names = T)
file.names <- list.files(path = dir,
                         pattern = "^TRD_Dalym.txt$",
                         full.names = F)
file.list <- list()
for (i in seq_along(file.paths)) {
    path <- file.paths[i]
    name <- file.names[i]
    file <- read.delim(path, head = T, stringsAsFactors = F,
                       fileEncoding = "UTF-8",
                       colClasses = c("character",
                                      "character", # trddt
                                      rep("numeric", 9))) %>% setDT()
    file[, ":="(Trddt = as.Date(Trddt,format = "%Y-%m-%d"))]
    file.list[[name]] <- file
}
trd.dalym <- rbindlist(file.list, use.names = T)
setnames(trd.dalym, names(trd.dalym), tolower(names(trd.dalym)))

# 导入 trd.cndalym ------------------------------------------------
dir <- "C:/Users/rossz/OneDrive/PR/Database-CSMar-Updating/trd.cndalym-2017-03-31"
file.paths <- list.files(path = dir,
                         pattern = "^TRD_Cndalym.txt$",
                         full.names = T)
file.names <- list.files(path = dir,
                         pattern = "^TRD_Cndalym.txt$",
                         full.names = F)
file.list <- list()
for (i in seq_along(file.paths)) {
    path <- file.paths[i]
    name <- file.names[i]
    file <- read.delim(path, head = T, stringsAsFactors = F,
                       fileEncoding = "UTF-8",
                       colClasses = c("character",
                                      "character",
                                      rep("numeric", 9))) %>% setDT()
    file[, ":="(Trddt = as.Date(Trddt, format = "%Y-%m-%d"))]
    file.list[[name]] <- file
}
trd.cndalym <- rbindlist(file.list, use.names = T)
setnames(trd.cndalym, names(trd.cndalym), tolower(names(trd.cndalym)))

# 导入 trd.cnmont ------------------------------------------------
dir <- "C:/Users/rossz/OneDrive/PR/Database-CSMar-Updating/trd.cnmont-2016-09-30"
file.paths <- list.files(path = dir,
                         pattern = "^TRD_Cnmont.txt$",
                         full.names = T)
file.names <- list.files(path = dir,
                         pattern = "^TRD_Cnmont.txt$",
                         full.names = F)
file.list <- list()
for (i in seq_along(file.paths)) {
    path <- file.paths[i]
    name <- file.names[i]
    file <- read.delim(path, head = T, stringsAsFactors = F,
                       fileEncoding = "UTF-8",
                       colClasses = c("character",
                                      "character",
                                      rep("numeric", 9))) %>% setDT()
    file.list[[name]] <- file
}
trd.cnmont <- rbindlist(file.list, use.names = T)
setnames(trd.cnmont, names(trd.cnmont), tolower(names(trd.cnmont)))

# 导入 trd.weekcm ------------------------------------------------
dir <- "C:/Users/rossz/OneDrive/PR/Database-CSMar-Updating/trd.weekcm-2017-03-03"
file.paths <- list.files(path = dir,
                         pattern = "^TRD_Weekcm.txt$",
                         full.names = T)
file.names <- list.files(path = dir,
                         pattern = "^TRD_Weekcm.txt$",
                         full.names = F)
file.list <- list()
for (i in seq_along(file.paths)) {
    path <- file.paths[i]
    name <- file.names[i]
    file <- read.delim(path, head = T, stringsAsFactors = F,
                       fileEncoding = "UTF-8",
                       colClasses = c("character",
                                      "character",
                                      rep("numeric", 9))) %>% setDT()
    file.list[[name]] <- file
}
trd.weekcm <- rbindlist(file.list, use.names = T)
setnames(trd.weekcm, names(trd.weekcm), tolower(names(trd.weekcm)))

# 导入 trd.co --------------------------------------------------
file.paths <- list.files(path = "C:/Users/rossz/OneDrive/PrivateResearch/Database-CSMar-Updating/trd.co-2015-08-17",
                         pattern = "^TRD_Co.txt$",
                         full.names = T)
file.names <- list.files(path = "C:/Users/rossz/OneDrive/PrivateResearch/Database-CSMar-Updating/trd.co-2015-08-17",
                         pattern = "^TRD_Co.txt$",
                         full.names = F)
file.list <- list()
for (i in seq_along(file.paths)) {
    path <- file.paths[i]
    name <- file.names[i]
    file <- read.delim(path, head = T, stringsAsFactors = F,
                       fileEncoding = "UTF-8",
                       colClasses = c(rep("character", 15),
                                      "numeric",
                                      "numeric",
                                      "character", # 发行计量货币
                                      "numeric",
                                      rep("character", 2),
                                      "numeric",
                                      rep("character", 6))) %>% setDT()
    file[, ":="(Estbdt = as.Date(Estbdt,format = "%Y-%m-%d"),
                Listdt = as.Date(Listdt,format = "%Y-%m-%d"),
                Favaldt = as.Date(Favaldt,format = "%Y-%m-%d"),
                Ipodt = as.Date(Ipodt,format = "%Y-%m-%d"),
                Statdt = as.Date(Statdt,format = "%Y-%m-%d"))]
    file.list[[name]] <- file
}
trd.co <- rbindlist(file.list, use.names = T)
setnames(trd.co, names(trd.co), tolower(names(trd.co)))
trd.co[, lstupdt := Sys.Date()]

# 导入 trd.index ---------------------------------------------------
file.paths <- list.files(path = "C:/Users/Root/OneDrive/PR/Database-CSMar-Updating/trd.index-2015-08-17",
                         pattern = "^TRD_Index.txt$",
                         full.names = T)
file.names <- list.files(path = "C:/Users/Root/OneDrive/PrivateResearch/Database-CSMar-Updating/trd.index-2015-08-17",
                         pattern = "^TRD_Index.txt$",
                         full.names = F)
file.list <- list()
for (i in seq_along(file.paths)) {
    path <- file.paths[i]
    name <- file.names[i]
    file <- read.delim(path, head = T, stringsAsFactors = F,
                       fileEncoding = "UTF-8",
                       colClasses = c("character",
                                      "character",
                                      "numeric",
                                      rep("numeric", 5))) %>% setDT()
    file[, ":="(Trddt = as.Date(Trddt,format = "%Y-%m-%d"))]
    file.list[[name]] <- file
}
trd.index <- rbindlist(file.list, use.names = T)
setnames(trd.index, names(trd.index), tolower(names(trd.index)))


# 导入 idx.idxinfo ---------------------------------
file.paths <- list.files(path = "C:/Users/Root/OneDrive/PrivateResearch/Database-CSMar-Updating/idx.idxinfo-2015-10-10",
                         pattern = "IDX_Idxinfo.txt$",
                         full.names = T)
file.names <- list.files(path = "C:/Users/Root/OneDrive/PrivateResearch/Database-CSMar-Updating/idx.idxinfo-2015-10-10",
                         pattern = "IDX_Idxinfo.txt$",
                         full.names = F)
file.list <- list()
for (i in seq_along(file.paths)) {
    path <- file.paths[i]
    name <- file.names[i]
    file <- read.delim(path, head = T, stringsAsFactors = F,
                       fileEncoding = "UTF-8",
                       colClasses = c(rep("character", 3),
                                      "numeric",
                                      rep("character", 8))) %>% setDT()
    file[, ":="(Idxinfo02=as.Date(Idxinfo02,format="%Y-%m-%d"),
                Idxinfo11=as.Date(Idxinfo11,format="%Y-%m-%d"))]
    file.list[[name]] <- file
}
idx.idxinfo <- rbindlist(file.list, use.names = T)
setnames(idx.idxinfo, names(idx.idxinfo), tolower(names(idx.idxinfo)))
setnames(idx.idxinfo, "indexcd", "idxcd")
idx.idxinfo <- unique(idx.idxinfo, by = names(idx.idxinfo))[!is.na(idxinfo02)] # delete duplicate

# 导入 idx.idxtrd -----------------------------------
dir <- 'C:/Users/rossz/OneDrive/PR/Database-CSMar-Updating/idx.idxtrd-main-2017-03-31/2017-02-25 - 2017-03-31'
file.paths <- list.files(path = dir, pattern = "IDX_Idxtrd[\\(0123456789\\)]*\\.txt$", full.names = T)
file.names <- list.files(path = dir, pattern = "IDX_Idxtrd[\\(0123456789\\)]*\\.txt$", full.names = F)
file.list <- list()
for (i in seq_along(file.paths)) {
    path <- file.paths[i]
    name <- file.names[i]
    file <- read.delim(path, head = T, stringsAsFactors = F,
                       fileEncoding = "UTF-8",
                       colClasses = c(rep("character", 2),
                                      rep("numeric", 7))) %>% setDT()
    file[, ":="(trddt = as.Date(Idxtrd01,format="%Y-%m-%d"))][, Idxtrd01 := NULL]
    file.list[[name]] <- file
}
idx.idxtrd <- rbindlist(file.list, use.names = T)
setnames(idx.idxtrd, names(idx.idxtrd), tolower(names(idx.idxtrd)))
setnames(idx.idxtrd, "indexcd", "idxcd")
idx.idxtrd <- unique(idx.idxtrd, by = names(idx.idxtrd)) # delete duplicate


# 导入 ffr.idxdtrd ----------------------------------
file.paths <- list.files(path = "C:/Users/rossz/OneDrive/PrivateResearch/Database-CSMar-Updating/frr.idxdtrd-2015-09-02",
                         pattern = "FRR_Idxdtrd\\([0123456789]\\).txt$",
                         full.names = T)
file.names <- list.files(path = "C:/Users/rossz/OneDrive/PrivateResearch/Database-CSMar-Updating/frr.idxdtrd-2015-09-02",
                         pattern = "FRR_Idxdtrd\\([0123456789]\\).txt$",
                         full.names = F)
file.list <- list()
for (i in seq_along(file.paths)) {
    path <- file.paths[i]
    name <- file.names[i]
    file <- read.delim(path, head = T, stringsAsFactors = F,
                       fileEncoding = "UTF-8",
                       colClasses = c("character",
                                      "character",
                                      "numeric",
                                      "numeric")) %>% setDT()
    file[, ":="(Trdate = as.Date(Trdate, format = "%Y-%m-%d"))]
    file.list[[name]] <- file
}
frr.idxdtrd <- rbindlist(file.list, use.names = T)
setnames(frr.idxdtrd, names(frr.idxdtrd), tolower(names(frr.idxdtrd)))
setnames(frr.idxdtrd, "trdate", "trddt")
frr.idxdtrd <- unique(frr.idxdtrd, by = names(frr.idxdtrd)) # delete duplicate

#导入 ffr.idxdtrd -----------------------------
file.paths <- list.files(path = "C:/Users/rossz/OneDrive/PrivateResearch/Database-CSMar-Updating/frr.idxinfo-2015-09-02",
                         pattern = "FRR_Idxinfo.txt$",
                         full.names = T)
file.names <- list.files(path = "C:/Users/rossz/OneDrive/PrivateResearch/Database-CSMar-Updating/frr.idxinfo-2015-09-02",
                         pattern = "FRR_Idxinfo.txt$",
                         full.names = F)
file.list <- list()
for (i in seq_along(file.paths)) {
    path <- file.paths[i]
    name <- file.names[i]
    file <- read.delim(path, head = T, stringsAsFactors = F,
                       fileEncoding = "UTF-8",
                       colClasses = c(rep("character", 3),
                                      "numeric",
                                      rep("character", 8))) %>% setDT()
    file[, ":="(Bendate = as.Date(Bendate, format = "%Y-%m-%d"),
                Stdate = as.Date(Stdate, format = "%Y-%m-%d"))]
    file.list[[name]] <- file
}
frr.idxinfo <- rbindlist(file.list, use.names = T)
setnames(frr.idxinfo, names(frr.idxinfo), tolower(names(frr.idxinfo)))
setnames(frr.idxinfo, "maket", "market")
frr.idxinfo <- unique(frr.idxinfo, by = names(frr.idxinfo)) # delete duplicate

# 导入 sdi.thrfac day  ------------------------------
dir <- "C:/Users/rossz/OneDrive/PR/Database-CSMar-Updating/sdi.thrfacday-2018-04-30"
file.paths <- list.files(dir,
                         pattern = "STK_MKT_ThrfacDay.txt$",
                         full.names = T)
file.names <- list.files(dir,
                         pattern = "STK_MKT_ThrfacDay.txt$",
                         full.names = F)
file.list <- list()
for (i in seq_along(file.paths)) {
    path <- file.paths[i]
    name <- file.names[i]
    file <- read.delim(path, head = T, stringsAsFactors = F,
                       fileEncoding = "UTF-8",
                       colClasses = c(rep("character", 2),
                                      rep("numeric", 6))) %>% setDT()
    file[, ":="(trddt = as.IDate(TradingDate, format = "%Y-%m-%d"))][, TradingDate := NULL]
    file.list[[name]] <- file
}
sdi.thrfacday <- rbindlist(file.list, use.names = T)

setnames(sdi.thrfacday, names(sdi.thrfacday), tolower(names(sdi.thrfacday)))

sdi.thrfacday <- unique(sdi.thrfacday, by = names(sdi.thrfacday)) # delete duplicate
d3f.mst.1804 <- sdi.thrfacday
sv(d3f.mst.1804)

# 导入 sdi.thrfacweek ------------------------------
dir <- "C:/Users/rossz/OneDrive/PR/Database-CSMar-Updating/sdi.thrfacweek-2018-04-30"
file.paths <- list.files(dir,
                         pattern = "STK_MKT_ThrfacWeek.txt$",
                         full.names = T)
file.names <- list.files(dir,
                         pattern = "STK_MKT_ThrfacWeek.txt$",
                         full.names = F)
file.list <- list()
for (i in seq_along(file.paths)) {
    path <- file.paths[i]
    name <- file.names[i]
    file <- read.delim(path, head = T, stringsAsFactors = F,
                       fileEncoding = "UTF-8",
                       colClasses = c(rep("character", 2),
                                      rep("numeric", 6))) %>% setDT()
    file.list[[name]] <- file
}
sdi.thrfacweek <- rbindlist(file.list, use.names = T)
setnames(sdi.thrfacweek, names(sdi.thrfacweek), tolower(names(sdi.thrfacweek)))
sdi.thrfacweek <- unique(sdi.thrfacweek, by = names(sdi.thrfacweek)) # delete duplicate
w3f.mst.1804 <- sdi.thrfacweek
sv(w3f.mst.1804)

# 导入trd.nrrate -------------------------------------
dir <- "C:/Users/rossz/OneDrive/PR/Database-CSMar-Updating/trd.nrrate-2017-03-31"
file.paths <- list.files(path = dir,
                         pattern = "^TRD_Nrrate.txt$",
                         full.names = T)
file.names <- list.files(path = dir,
                         pattern = "^TRD_Nrrate.txt$",
                         full.names = F)
file.list <- list()
for (i in seq_along(file.paths)) {
    path <- file.paths[i]
    name <- file.names[i]
    file <- read.delim(path, head = T, stringsAsFactors = F,
                       fileEncoding = "UTF-8",
                       ) %>% setDT()
    file[, ":="(Clsdt = as.Date(Clsdt, format = "%Y-%m-%d"))]
    file.list[[name]] <- file
}
trd.nrrate <- rbindlist(file.list, use.names = T)
setnames(trd.nrrate, names(trd.nrrate), tolower(names(trd.nrrate)))

# 导入 margin.dsummary -----
dir <- "C:/Users/rossz/OneDrive/PR/Database-CSMar-Updating/margin.dsummary-2016-12-30"
file.paths <- list.files(path = dir,
                         pattern = "CHN_Stkmt_dsummary.txt",
                         full.names = T)
file.names <- list.files(path = dir,
                         pattern = "CHN_Stkmt_dsummary.txt",
                         full.names = F)
file.list <- list()
for (i in seq_along(file.paths)) {
    path <- file.paths[i]
    name <- file.names[i]
    file <- read.delim(path, header = T) %>% setDT()
    file[, ":="(trddt = as.Date(Mtdate, format = "%Y-%m-%d"))][, Mtdate := NULL]
    file.list[[name]] <- file
}
margin <- rbindlist(file.list, use.names = T)
setnames(margin, names(margin), tolower(names(margin)))

# 导入 margin.wsummary -----
dir <- "C:/Users/rossz/OneDrive/PR/Database-CSMar-Updating/margin.wsummary-2017-03-03"
file.paths <- list.files(path = dir,
                         pattern = "CHN_Stkmt_wsummary.txt",
                         full.names = T)
file.names <- list.files(path = dir,
                         pattern = "CHN_Stkmt_wsummary.txt",
                         full.names = F)
file.list <- list()
for (i in seq_along(file.paths)) {
    path <- file.paths[i]
    name <- file.names[i]
    file <- read.delim(path, header = T) %>% setDT()
    #file[, ":="(trddt = as.Date(Mtdate, format = "%Y-%m-%d"))][, Mtdate := NULL]
    file.list[[name]] <- file
}
margin <- rbindlist(file.list, use.names = T)
setnames(margin, names(margin), tolower(names(margin)))

# 导入 shibor.avg -----
dir <- "C:/Users/rossz/OneDrive/PR/Database-CSMar-Updating/shibor-2017-04-01"
file.paths <- list.files(path = dir,
                         pattern = "SHIBOR_LdAvgRate.txt",
                         full.names = T)
file.names <- list.files(path = dir,
                         pattern = "SHIBOR_LdAvgRate.txt",
                         full.names = F)
file.list <- list()
for (i in seq_along(file.paths)) {
    path <- file.paths[i]
    name <- file.names[i]
    file <- read.delim(path, header = T) %>% setDT()
    file[, ":="(trddt = as.Date(Tradingdate, format = "%Y-%m-%d"))][, Tradingdate := NULL]
    file.list[[name]] <- file
}
shibor <- rbindlist(file.list, use.names = T)
setnames(shibor, names(shibor), tolower(names(shibor)))

# 导入ffut.fdt -----
dir <- "C:/Users/rossz/OneDrive/PR/Database-CSMar-Updating/ffut.fdt-2016-12-30"
file.paths <- list.files(path = dir,
                         pattern = "FFUT_FDT.txt",
                         full.names = T)
file.names <- list.files(path = dir,
                         pattern = "FFUT_FDT.txt",
                         full.names = F)
file.list <- list()
for (i in seq_along(file.paths)) {
    path <- file.paths[i]
    name <- file.names[i]
    file <- read.delim(path, header = T, fileEncoding = "utf8") %>% setDT()
    file[, ":="(Trddt = as.Date(Trddt, format = "%Y-%m-%d"))]
    file.list[[name]] <- file
}
ffut <- rbindlist(file.list, use.names = T)
setnames(ffut, names(ffut), tolower(names(ffut)))

# 导入ffut.fwt -----
dir <- "C:/Users/rossz/OneDrive/PR/Database-CSMar-Updating/ffut.fwt-2017-03-03"
file.paths <- list.files(path = dir,
                         pattern = "FFUT_FWT.txt",
                         full.names = T)
file.names <- list.files(path = dir,
                         pattern = "FFUT_FWT.txt",
                         full.names = F)
file.list <- list()
for (i in seq_along(file.paths)) {
    path <- file.paths[i]
    name <- file.names[i]
    file <- read.delim(path, header = T, fileEncoding = "utf8") %>% setDT()
    #file[, ":="(Trddt = as.Date(Trddt, format = "%Y-%m-%d"))]
    file.list[[name]] <- file
}
ffut <- rbindlist(file.list, use.names = T)
setnames(ffut, names(ffut), tolower(names(ffut)))

# 将数据写入MySQL ------------------------------------------------------------
# 建立连接
conn.gta <- dbConnect(MySQL(),
                 user = "root", password = "19671006",
                 dbname = "gta", host = "localhost")
on.exit(dbDisconnect(conn.read.gta))
dbSendQuery(conn.gta, "set names GBK")

# MySQL 写入 trd.dalyr ------------------------------
# 全部读入使用了5min40s
system.time({
    dbWriteTable(conn.gta, value = setDF(trd.dalyr),
                 name = "trd_dalyr",
                 row.names = F,
                 field.types = list(stkcd = "varchar(20)",
                                    trddt = "date",
                                    opnprc = "double",
                                    hiprc = "double",
                                    loprc = "double",
                                    clsprc = "double",
                                    dnshrtrd = "double",
                                    dnvaltrd = "double",
                                    dsmvosd = "double",
                                    dsmvtll = "double",
                                    dretwd = "double",
                                    dretnd = "double",
                                    adjprcwd = "double",
                                    adjprcnd = "double",
                                    markettype = "varchar(10)",
                                    capchgdt = "date",
                                    trdsta = "varchar(10)"
                                    ),
                 overwrite = F,
                 append = T)
})
#system.time({
    #dbSendQuery(conn, "alter table trd_dalyr add primary key (stkcd, trddt);")
#})  # 设置主键 stkcd trddt, 使用90s

# MySQL 写入 trd.co ----------------------------------------
dbWriteTable(conn, name = "trd_co", value = setDF(trd.co),
             row.names = F,
             field.types = list(stkcd = "varchar(20)",
                                cuntrycd = "varchar(10)",
                                stknme = "varchar(20)", # 最长7字符
                                conme = "varchar(40)",  # 最长20字符
                                conme_en = "varchar(200)", # 最长88字符
                                indcd = "varchar(10)", # 4位行业代码
                                indnme = "varchar(10)", # 最长4字符
                                nindcd = "varchar(10)", # 最长5字符
                                nindnme = "varchar(30)", # max 17 char
                                nnindcd = "varchar(10)", # max 3 char
                                nnindnme = "varchar(40)", # max 20 char
                                estbdt = "date",
                                listdt = "date",
                                favaldt = "date",
                                curtrd = "varchar(10)", # max 3 char (CNY)
                                ipoprm = "double",
                                ipoprc = "double",
                                ipocur = "varchar(10)",
                                nshripo = "double",
                                parvcur = "varchar(10)",
                                ipodt = "date",
                                parval = "double",
                                sctcd = "varchar(10)",
                                statco = "varchar(10)",
                                crcd = "varchar(10)",
                                statdt = "date",
                                commnt = "varchar(10)",
                                markettype = "varchar(10)",
                                lstupdt = "date"
                                ),
             overwrite = F,
             append = T)
dbSendQuery(conn, "alter table trd_co add primary key (stkcd, lstupdt);")

############################ MySQL 写入 trd.index ###########################
dbWriteTable(conn, name = "trd_index", value = trd.index,
             row.names = F,
             field.types = list(indexcd = "varchar(20)",
                                trddt = "date",
                                daywk = "int(2)",
                                opnindex = "double",
                                hiindex = "double",
                                loindex = "double",
                                clsindex = "double",
                                retindex = "double"
             ),
             overwrite = F,
             append = T)
dbSendQuery(conn, "alter table trd_index add primary key (indexcd, trddt);")

############################ MySQL 写入 trd.dalym ###########################
dbWriteTable(conn, name = "trd_dalym", value = setDF(trd.dalym),
             row.names = F,
             field.types = list(markettype = "varchar(10)",
                                trddt = "date",
                                dnshrtrdtl = "double",
                                dnvaltrdtl = "double",
                                dretwdeq = "double",
                                dretmdeq = "double",
                                dretwdos = "double",
                                dretmdos = "double",
                                dretwdtl = "double",
                                dretmdtl = "double",
                                dnstkcal = "double"
             ),
             overwrite = F,
             append = T)
dbSendQuery(conn, "alter table trd_dalym add primary key (markettype, trddt);")

# MySQL 写入 trd.cndalym ---------------------------
dbWriteTable(conn.gta, name = "trd_cndalym", value = setDF(trd.cndalym),
             row.names = F,
             field.types = list(markettype = "varchar(10)",
                                trddt = "date",
                                cnshrtrdtl = "double",
                                cnvaltrdtl = "double",
                                cdretwdeq = "double",
                                cdretmdeq = "double",
                                cdretwdos = "double",
                                cdretmdos = "double",
                                cdretwdtl = "double",
                                cdretmdtl = "double",
                                cdnstkcal = "double"
             ),
             overwrite = F,
             append = T)
#dbSendQuery(conn, "alter table trd_cndalym add primary key (markettype, trddt);")

# MySQL 写入 trd.cnmont ---------------------------
dbWriteTable(conn.gta, name = "trd_cnmont", value = setDF(trd.cnmont),
             row.names = F,
             field.types = list(markettype = "varchar(10)",
                                trdmnt = "varchar(10)",
                                cmretwdeq = "double",
                                cmretmdeq = "double",
                                cmretwdos = "double",
                                cmretmdos = "double",
                                cmretwdtl = "double",
                                cmretmdtl = "double",
                                cmnstkcal = "double",
                                cmmvosd = "double",
                                cmmvttl = "double"
             ),
             overwrite = F,
             append = T)
#dbSendQuery(conn.gta, "alter table trd_cnmont add primary key (markettype, trdmnt);")

# MySQL 写入 trd.weekcm ---------------------------
dbWriteTable(conn.gta, name = "trd_weekcm", value = setDF(trd.weekcm),
             row.names = F,
             field.types = list(markettype = "varchar(10)",
                                trdwnt = "varchar(10)",
                                cwretwdeq = "double",
                                cwretmdeq = "double",
                                cwretwdos = "double",
                                cwretmdos = "double",
                                cwretwdtl = "double",
                                cwretmdtl = "double",
                                cwnstkcal = "double",
                                cwmvosd = "double",
                                cwmvttl = "double"
             ),
             overwrite = F,
             append = T)
#dbSendQuery(conn.gta, "alter table trd_weekcm add primary key (markettype, trdwnt);")

# MySQL 写入 idx.idxinfo ---------------------------------
dbWriteTable(conn, name = "idx_idxinfo", value = idx.idxinfo,
             row.names = F,
             field.types = list(idxcd = "varchar(20)",
                                idxinfo01 = "varchar(200)",
                                idxinfo02 = "varchar(200)",
                                idxinfo03 = "varchar(200)",
                                idxinfo04 = "varchar(200)",
                                idxinfo05 = "varchar(200)",
                                idxinfo06 = "varchar(200)",
                                idxinfo07 = "varchar(200)",
                                idxinfo08 = "varchar(200)",
                                idxinfo09 = "varchar(200)",
                                idxinfo10 = "varchar(1000)",
                                idxinfo11 = "date"
             ),
             overwrite = T,
             append = F)
dbSendQuery(conn, "alter table idx_idxinfo add primary key (idxcd);")

# MySQL 写入 idx.idxtrd ----------------------------
dbWriteTable(conn.gta, name = "idx_idxtrd", value = idx.idxtrd,
             row.names = F,
             field.types = list(idxcd = "varchar(20)",
                                idxtrd02 = "double",
                                idxtrd03 = "double",
                                idxtrd04 = "double",
                                idxtrd05 = "double",
                                idxtrd06 = "double",
                                idxtrd07 = "double",
                                idxtrd08 = "double",
                                trddt = "date"
             ),
             overwrite = F,
             append = T)
#dbSendQuery(conn.gta, "alter table idx_idxtrd add primary key (idxcd, trddt);")


# MySQL 写入 frr.idxdtrd --------------------------
dbWriteTable(conn, name = "frr_idxdtrd", value = frr.idxdtrd,
             row.names = F,
             field.types = list(indexcd = "varchar(20)",
                                trddt = "date",
                                clsdind = "double",
                                redind = "double"
             ),
             overwrite = T,
             append = F)
dbSendQuery(conn, "alter table frr_idxdtrd add primary key (indexcd, trddt);")

# MySQL 写入 frr.idxinfo --------------------
dbWriteTable(conn, name = "frr_idxinfo", value = frr.idxinfo,
             row.names = F,
             field.types = list(indexcd = "varchar(20)",
                                indnm = "varchar(40)",
                                bendate = "date",
                                bendata = "double",
                                samran = "varchar(500)",
                                weimeth = "varchar(500)",
                                issuorg = "varchar(500)",
                                indsort = "varchar(10)",
                                indtyp = "varchar(10)",
                                market = "varchar(10)",
                                calform = "varchar(500)",
                                stdate = "date"
             ),
             overwrite = F,
             append = T)
dbSendQuery(conn, "alter table frr_idxinfo add primary key (indexcd);")

# MySQL 写入 sdi.thrfacday ----------------------------------------
dbWriteTable(conn.read.gta, name = "sdi_thrfacday", value = sdi.thrfacday,
             row.names = F,
             field.types = list(markettypeid = "varchar(20)",
                                riskpremium1 = "double",
                                riskpremium2 = "double",
                                smb1 = "double",
                                smb2 = "double",
                                hml1 = "double",
                                hml2 = "double",
                                trddt = "date"
             ),
             overwrite = T,
             append = F)
dbSendQuery(conn.read.gta, "alter table sdi_thrfacday add primary key (markettypeid, trddt);")

# MySQL 写入 sdi.thrfacweek ----------------------------------------
dbWriteTable(conn.gta, name = "sdi_thrfacweek", value = sdi.thrfacweek,
             row.names = F,
             field.types = list(markettypeid = "varchar(20)",
                                tradingweek = "varchar(20)",
                                riskpremium1 = "double",
                                riskpremium2 = "double",
                                smb1 = "double",
                                smb2 = "double",
                                hml1 = "double",
                                hml2 = "double"
             ),
             overwrite = T,
             append = F)
#dbSendQuery(conn.gta, "alter table sdi_thrfacweek add primary key (markettypeid, tradingweek);")

dbWriteTable(conn.read.gta, name = "sdi_thrfacmonth", value = sdi.thrfacmonth,
             row.names = F,
             field.types = list(markettypeid = "varchar(20)",
                                tradingmonth = "varchar(20)",
                                riskpremium1 = "double",
                                riskpremium2 = "double",
                                smb1 = "double",
                                smb2 = "double",
                                hml1 = "double",
                                hml2 = "double"
             ),
             overwrite = T,
             append = F)
dbSendQuery(conn.read.gta, "alter table sdi_thrfacmonth add primary key (markettypeid, tradingmonth);")

# MySQL 写入 trd.nrrate ----------------------------------------
dbWriteTable(conn.gta, name = "trd_nrrate", value = setDF(trd.nrrate),
             row.names = F,
             field.types = list(nrr1 = "varchar(20)",
                                clsdt = "date",
                                nrrdata = "double",
                                nrrdaydt = "double",
                                nrrwkdt = "double",
                                nrrmtdt = "double"
                                ),
             overwrite = F,
             append = T)
dbSendQuery(conn.gta, "alter table trd_nrrate add primary key (nrr1, clsdt);")

# MySQL 写入 shibor.avg ----------------------------------------
dbWriteTable(conn.gta, name = "shibor_avg", value = setDF(shibor),
             row.names = F,
             field.types = list(market = "varchar(100)",
                                term = "varchar(100)",
                                currency = "varchar(100)",
                                interestrate = "double",
                                avg_5d = "double",
                                avg_10d = "double",
                                avg_20d = "double",
                                trddt = "date"
                                ),
             overwrite = F,
             append = T)
#dbSendQuery(conn.gta, "alter table shibor_avg add primary key (market, term, currency, trddt);")

# MySQL 写入 ffut.fdt ----------------------------------------
dbWriteTable(conn.gta, name = "ffut_fdt", value = setDF(ffut),
             row.names = F,
             field.types = list(trddt = "date",
                                agmtcd = "varchar(100)",
                                trdvar = "varchar(100)",
                                exhcd = "varchar(100)",
                                deldt = "date",
                                opnprc = "double",
                                hiprc = "double",
                                loprc = "double",
                                clsprc = "double",
                                stprc = "double",
                                ystprc = "double",
                                updown1 = "double",
                                updown2 = "double",
                                volume = "double",
                                opint = "double",
                                yopint = "double",
                                chopint = "double",
                                turnover = "double"
                                ),
             overwrite = F,
             append = T)
#dbSendQuery(conn.gta, "alter table ffut_fdt add primary key (trddt, agmtcd);")

# MySQL 写入 ffut.fwt ----------------------------------------
dbWriteTable(conn.gta, name = "ffut_fwt", value = setDF(ffut),
             row.names = F,
             field.types = list(trdwk = "varchar(100)",
                                opndt = "date",
                                clsdt = "date",
                                trdvar = "varchar(100)",
                                agmtcd = "varchar(100)",
                                exhcd = "varchar(100)",
                                wopnprc = "double",
                                wmaxprc = "double",
                                wminprc = "double",
                                wclsprc = "double",
                                wstprc = "double",
                                acupdown1 = "double",
                                acupdown2 = "double",
                                wopint = "double",
                                wchopint = "double",
                                wvolume = "double",
                                wturnover = "double",
                                ndaytrd = "double"
                                ),
             overwrite = F,
             append = T)
dbSendQuery(conn.gta, "alter table ffut_fwt add primary key (trdwk, agmtcd);")

# MySQL 写入 margin.dsummary ----------------------------------------
dbWriteTable(conn.gta, name = "margin_dsummary", value = setDF(margin),
             row.names = F,
             field.types = list(exchangecode = "varchar(100)",
                                financeamount = "double",
                                financerepay = "double",
                                financebalance = "double",
                                securityshort = "double",
                                securityrepay = "double",
                                secshortbalance = "double",
                                shortbalance = "double",
                                mtbalance = "double",
                                numstockfinance = "double",
                                numstockshort = "double",
                                trddt = "date"
                                ),
             overwrite = F,
             append = T)
#dbSendQuery(conn.gta, "alter table margin_dsummary add primary key (trddt, exchangecode);")

# MySQL 写入 margin.wsummary ----------------------------------------
dbWriteTable(conn.gta, name = "margin_wsummary", value = setDF(margin),
             row.names = F,
             field.types = list(exchangecode = "varchar(100)",
                                mtweek = "varchar(100)",
                                weekbegindate = "date",
                                weekenddate = "date",
                                financebalance = "double",
                                financebalancegrowth = "double",
                                secshortbalance = "double",
                                secshortbalancegrowth = "double",
                                shortbalance = "double",
                                shortbalancegrowth = "double",
                                mtbalance = "double",
                                mtbalcncegrowth = "double",
                                numstockfinance = "double",
                                numstockshort = "double"
                                ),
             overwrite = F,
             append = T)
#dbSendQuery(conn.gta, "alter table margin_wsummary add primary key (mtweek, exchangecode);")