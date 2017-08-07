context("HDFS joins, left source = xdf")

# skip everything until joins situation sorted out
# set the compute context manually

#detectHdfsConnection()

#local_exec({
    #df1 <- data.frame(a=letters[1:20], b=1:20, c=11:30, stringsAsFactors=FALSE)
    #df2 <- data.frame(a=letters[7:26], d=as.character(1:20), e=11:30, stringsAsFactors=FALSE)
    #xdf1 <- rxDataStep(df1, "xdf1.xdf", overwrite=TRUE)
    #xdf2 <- rxDataStep(df2, "xdf2.xdf", overwrite=TRUE)

    #df1f <- data.frame(a=factor(letters[1:20]), b=1:20, c=11:30, stringsAsFactors=FALSE)
    #df2f <- data.frame(a=factor(letters[7:26]), d=as.character(1:20), e=11:30, stringsAsFactors=FALSE)

    #xdf1f <- rxDataStep(df1f, "xdf1f.xdf", overwrite=TRUE)
    #xdf2f <- rxDataStep(df2f, "xdf2f.xdf", overwrite=TRUE)
#})

#xdf1 <- copy_to(hd, xdf1)
#xdf2 <- copy_to(hd, xdf2)

#xdf1f <- copy_to(hd, xdf1f)
#xdf2f <- copy_to(hd, xdf2f)


#verifyHdfsData <- function(xdf, expectedClass)
#{
    #isTRUE(xdf@createCompositeSet) && rxHadoopFileExists(xdf@file) && class(xdf) == expectedClass # test for exact class
#}


#test_that("xdf to xdf joining works",
#{
    #expect_true(verifyHdfsData(left_join(xdf1, xdf2), "tbl_xdf"))
    #expect_true(verifyHdfsData(right_join(xdf1, xdf2), "tbl_xdf"))
    #expect_true(verifyHdfsData(inner_join(xdf1, xdf2), "tbl_xdf"))
    #expect_true(verifyHdfsData(full_join(xdf1, xdf2), "tbl_xdf"))
    #expect_true(verifyHdfsData(semi_join(xdf1, xdf2), "tbl_xdf"))
    #expect_true(verifyHdfsData(anti_join(xdf1, xdf2), "tbl_xdf"))
    #expect_true(verifyHdfsData(union(xdf1, xdf1), "tbl_xdf"))
    #expect_true(verifyHdfsData(union_all(xdf1, xdf1), "tbl_xdf"))

    #expect_true(verifyHdfsData(inner_join(xdf1, xdf2, by=c("b"="d")), "tbl_xdf"))
    #expect_true(verifyHdfsData(inner_join(xdf1, xdf2, by=c("b"="e")), "tbl_xdf"))
    #expect_true(verifyHdfsData(inner_join(xdf1, xdf2, by=c("c"="d")), "tbl_xdf"))

    #expect_true(verifyHdfsData(inner_join(xdf1f, xdf2f), "tbl_xdf"))
    #expect_true(verifyHdfsData(inner_join(xdf1, xdf2f), "tbl_xdf"))
    #expect_true(verifyHdfsData(inner_join(xdf1f, xdf2), "tbl_xdf"))
#})


#test_that("xdf to xdf joining -> data frame works",
#{
    #expect_true(verifyHdfsData(left_join(xdf1, xdf2, .outFile=NULL), "data.frame"))
    #expect_true(verifyHdfsData(right_join(xdf1, xdf2, .outFile=NULL), "data.frame"))
    #expect_true(verifyHdfsData(inner_join(xdf1, xdf2, .outFile=NULL), "data.frame"))
    #expect_true(verifyHdfsData(full_join(xdf1, xdf2, .outFile=NULL), "data.frame"))
    #expect_true(verifyHdfsData(semi_join(xdf1, xdf2, .outFile=NULL), "data.frame"))
    #expect_true(verifyHdfsData(anti_join(xdf1, xdf2, .outFile=NULL), "data.frame"))

    #expect_true(verifyHdfsData(inner_join(xdf1, xdf2, by=c("b"="d"), .outFile=NULL), "data.frame"))
    #expect_true(verifyHdfsData(inner_join(xdf1, xdf2, by=c("b"="e"), .outFile=NULL), "data.frame"))
    #expect_true(verifyHdfsData(inner_join(xdf1, xdf2, by=c("c"="d"), .outFile=NULL), "data.frame"))

    #expect_true(verifyHdfsData(inner_join(xdf1f, xdf2f, .outFile=NULL), "data.frame"))
    #expect_true(verifyHdfsData(inner_join(xdf1, xdf2f, .outFile=NULL), "data.frame"))
    #expect_true(verifyHdfsData(inner_join(xdf1f, xdf2, .outFile=NULL), "data.frame"))

    #expect_true(verifyHdfsData(union(xdf1, xdf1, .outFile=NULL), "data.frame"))
    #expect_true(verifyHdfsData(union_all(xdf1, xdf1, .outFile=NULL), "data.frame"))
#})


#test_that("xdf to xdf joining -> xdf works",
#{
    #expect_true(verifyHdfsData(left_join(xdf1, xdf2, .outFile="test09.xdf"), "RxXdfData"))
    #expect_true(verifyHdfsData(right_join(xdf1, xdf2, .outFile="test09.xdf"), "RxXdfData"))
    #expect_true(verifyHdfsData(inner_join(xdf1, xdf2, .outFile="test09.xdf"), "RxXdfData"))
    #expect_true(verifyHdfsData(full_join(xdf1, xdf2, .outFile="test09.xdf"), "RxXdfData"))
    #expect_true(verifyHdfsData(semi_join(xdf1, xdf2, .outFile="test09.xdf"), "RxXdfData"))
    #expect_true(verifyHdfsData(anti_join(xdf1, xdf2, .outFile="test09.xdf"), "RxXdfData"))
    #expect_true(verifyHdfsData(union(xdf1, xdf1, .outFile="test09.xdf"), "RxXdfData"))
    #expect_true(verifyHdfsData(union_all(xdf1, xdf1, .outFile="test09.xdf"), "RxXdfData"))

    #expect_true(verifyHdfsData(inner_join(xdf1, xdf2, by=c("b"="d"), .outFile="test09.xdf"), "RxXdfData"))
    #expect_true(verifyHdfsData(inner_join(xdf1, xdf2, by=c("b"="e"), .outFile="test09.xdf"), "RxXdfData"))
    #expect_true(verifyHdfsData(inner_join(xdf1, xdf2, by=c("c"="d"), .outFile="test09.xdf"), "RxXdfData"))

    #expect_true(verifyHdfsData(inner_join(xdf1f, xdf2f, .outFile="test09.xdf"), "RxXdfData"))
    #expect_true(verifyHdfsData(inner_join(xdf1, xdf2f, .outFile="test09.xdf"), "RxXdfData"))
    #expect_true(verifyHdfsData(inner_join(xdf1f, xdf2, .outFile="test09.xdf"), "RxXdfData"))
#})

## clean up
#file.remove(dir(pattern="\\.(csv|xdf)$"))
#hdfs_dir_remove(c("xdf1", "xdf2", "xdf1f", "xdf2f"))


