context("HDFS joins, left source = xdf")

# set the compute context manually

detectHdfsConnection()

xdf1 <- data.frame(a=letters[1:20], b=1:20, c=11:30, stringsAsFactors=FALSE)
xdf2 <- data.frame(a=letters[7:26], d=as.character(1:20), e=11:30, stringsAsFactors=FALSE)

xdf1f <- data.frame(a=factor(letters[1:20]), b=1:20, c=11:30, stringsAsFactors=FALSE)
xdf2f <- data.frame(a=factor(letters[7:26]), d=as.character(1:20), e=11:30, stringsAsFactors=FALSE)

xdf1 <- copy_to_hdfs(xdf1, overwrite=TRUE)
xdf2 <- copy_to_hdfs(xdf2, overwrite=TRUE)

xdf1f <- copy_to_hdfs(xdf1f, overwrite=TRUE)
xdf2f <- copy_to_hdfs(xdf2f, overwrite=TRUE)


verifyHdfsData <- function(xdf, expectedClass)
{
    isTRUE(xdf@createCompositeSet) && rxHadoopFileExists(xdf@file) && class(xdf) == expectedClass # test for exact class
}


test_that("xdf to xdf joining works",
{
    expect_true(verifyHdfsData(left_join(xdf1, xdf2), "tbl_xdf"))
    expect_true(verifyHdfsData(right_join(xdf1, xdf2), "tbl_xdf"))
    expect_true(verifyHdfsData(inner_join(xdf1, xdf2), "tbl_xdf"))
    expect_true(verifyHdfsData(full_join(xdf1, xdf2), "tbl_xdf"))
    expect_error(semi_join(xdf1, xdf2))
    expect_error(anti_join(xdf1, xdf2))
    expect_error(union(xdf1, xdf1))
    expect_error(union_all(xdf1, xdf1))

    expect_true(verifyHdfsData(inner_join(xdf1, xdf2, by=c("b"="d")), "tbl_xdf"))
    expect_true(verifyHdfsData(inner_join(xdf1, xdf2, by=c("b"="e")), "tbl_xdf"))
    expect_true(verifyHdfsData(inner_join(xdf1, xdf2, by=c("c"="d")), "tbl_xdf"))

    #expect_true(verifyHdfsData(inner_join(xdf1f, xdf2f), "tbl_xdf"))
    #expect_true(verifyHdfsData(inner_join(xdf1, xdf2f), "tbl_xdf"))
    #expect_true(verifyHdfsData(inner_join(xdf1f, xdf2), "tbl_xdf"))
})


test_that("xdf to xdf joining -> data frame works",
{
    expect_true(inherits(left_join(xdf1, xdf2, .outFile=NULL), "data.frame"))
    expect_true(inherits(right_join(xdf1, xdf2, .outFile=NULL), "data.frame"))
    expect_true(inherits(inner_join(xdf1, xdf2, .outFile=NULL), "data.frame"))
    expect_true(inherits(full_join(xdf1, xdf2, .outFile=NULL), "data.frame"))

    expect_true(inherits(inner_join(xdf1, xdf2, by=c("b"="d"), .outFile=NULL), "data.frame"))
    expect_true(inherits(inner_join(xdf1, xdf2, by=c("b"="e"), .outFile=NULL), "data.frame"))
    expect_true(inherits(inner_join(xdf1, xdf2, by=c("c"="d"), .outFile=NULL), "data.frame"))

    #expect_true(inherits(inner_join(xdf1f, xdf2f, .outFile=NULL), "data.frame"))
    #expect_true(inherits(inner_join(xdf1, xdf2f, .outFile=NULL), "data.frame"))
    #expect_true(inherits(inner_join(xdf1f, xdf2, .outFile=NULL), "data.frame"))
})


test_that("xdf to xdf joining -> xdf works",
{
    expect_true(verifyHdfsData(left_join(xdf1, xdf2, .outFile="test09.xdf"), "RxXdfData"))
    expect_true(verifyHdfsData(right_join(xdf1, xdf2, .outFile="test09.xdf"), "RxXdfData"))
    expect_true(verifyHdfsData(inner_join(xdf1, xdf2, .outFile="test09.xdf"), "RxXdfData"))
    expect_true(verifyHdfsData(full_join(xdf1, xdf2, .outFile="test09.xdf"), "RxXdfData"))

    expect_true(verifyHdfsData(inner_join(xdf1, xdf2, by=c("b"="d"), .outFile="test09.xdf"), "RxXdfData"))
    expect_true(verifyHdfsData(inner_join(xdf1, xdf2, by=c("b"="e"), .outFile="test09.xdf"), "RxXdfData"))
    expect_true(verifyHdfsData(inner_join(xdf1, xdf2, by=c("c"="d"), .outFile="test09.xdf"), "RxXdfData"))

    #expect_true(verifyHdfsData(inner_join(xdf1f, xdf2f, .outFile="test09.xdf"), "RxXdfData"))
    #expect_true(verifyHdfsData(inner_join(xdf1, xdf2f, .outFile="test09.xdf"), "RxXdfData"))
    #expect_true(verifyHdfsData(inner_join(xdf1f, xdf2, .outFile="test09.xdf"), "RxXdfData"))
})

# clean up
hdfs_dir_remove(c("xdf1", "xdf2", "xdf1f", "xdf2f"))


