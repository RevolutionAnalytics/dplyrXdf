context("HDFS functionality")

# set the compute context manually

detectHdfsConnection()

verifyHdfsData <- function(xdf, expectedClass)
{
    isTRUE(xdf@createCompositeSet) && hdfs_dir_exists(xdf@file) && class(xdf) == expectedClass # test for exact class
}

if(hdfs_dir_exists("mtcars")) hdfs_dir_remove("mtcars")
mthc <- copy_to_hdfs(tibble::remove_rownames(mtcars), name="mtcars")

test_that("filter works",
{
    tbl <- mthc %>% filter(mpg > 15, cyl <= 6)
    expect_true(verifyHdfsData(tbl, "tbl_xdf"))
})

test_that("select works",
{
    tbl <- mthc %>% select(mpg, cyl, drat)
    expect_true(verifyHdfsData(tbl, "tbl_xdf"))
})

test_that("subset works",
{
    tbl <- mthc %>% subset(mpg > 15, c(mpg, cyl, drat))
    expect_true(verifyHdfsData(tbl, "tbl_xdf"))
})

test_that("mutate works",
{
    tbl <- mthc %>% mutate(mpg2=sin(mpg), wt2=sqrt(wt))
    expect_true(verifyHdfsData(tbl, "tbl_xdf"))
})

test_that("transmute works",
{
    tbl <- mthc %>% transmute(mpg2=sin(mpg), wt2=sqrt(wt))
    expect_true(verifyHdfsData(tbl, "tbl_xdf"))
})

test_that("ungrouped summarise works",
{
    tbl <- mthc %>% summarise(n=n(), mpg2=mean(mpg))
    expect_true(verifyHdfsData(tbl, "tbl_xdf"))
    expect_warning(tbl <- mthc %>% summarise(n=n(), mpg2=mean(mpg), .method=1))
    expect_true(verifyHdfsData(tbl, "tbl_xdf"))
    tbl <- mthc %>% summarise(n=n(), mpg2=mean(mpg), .method=2)
    expect_true(verifyHdfsData(tbl, "tbl_xdf"))
    expect_warning(tbl <- mthc %>% summarise(n=n(), mpg2=mean(mpg), .method=3))
    expect_true(verifyHdfsData(tbl, "tbl_xdf"))
    tbl <- mthc %>% summarise(n=n(), mpg2=mean(mpg), .method=4)
    expect_true(verifyHdfsData(tbl, "tbl_xdf"))
    tbl <- mthc %>% summarise(n=n(), mpg2=mean(mpg), .method=5)
    expect_true(verifyHdfsData(tbl, "tbl_xdf"))
})

test_that("ungrouped do works",
{
    tbl <- mthc %>% do(m=lm(mpg ~ cyl, data=.), w=lm(wt ~ cyl, data=.))
    expect_true(is.data.frame(tbl))
    tbl <- mthc %>% do(
    {
        .$mpg2 <- sin(.$mpg)
        .$cyl2 <- sqrt(.$cyl)
        .
    })
    expect_true(is.data.frame(tbl))
})

# only for local compute context
if(!inherits(rxGetComputeContext(), "RxHadoopMR"))
{
    test_that("distinct works",
    {
        tbl <- mthc %>% distinct()
        expect_true(verifyHdfsData(tbl, "tbl_xdf"))
        tbl <- mthc %>% distinct(cyl, gear)
        expect_true(verifyHdfsData(tbl, "tbl_xdf"))
    })
}

test_that("factorise works",
{
    tbl <- mthc %>% factorise(cyl, gear)
    expect_true(verifyHdfsData(tbl, "tbl_xdf") && varTypes(tbl)["cyl"] == "factor")
})

test_that("rename works",
{
    tbl <- mthc %>% rename(mpg2=mpg)
    expect_true(verifyHdfsData(tbl, "tbl_xdf") && names(tbl)[1] == "mpg2")
})

test_that("output to data frame works",
{
    tbl <- mthc %>% filter(mpg > 15, cyl <= 6, .outFile=NULL)
    expect_true(is.data.frame(tbl))
    tbl <- mthc %>% select(mpg, cyl, drat, .outFile=NULL)
    expect_true(is.data.frame(tbl))
    tbl <- mthc %>% subset(mpg > 15, c(mpg, cyl, drat), .outFile=NULL)
    expect_true(is.data.frame(tbl))
    tbl <- mthc %>% mutate(mpg2=sin(mpg), wt2=sqrt(wt), .outFile=NULL)
    expect_true(is.data.frame(tbl))
    tbl <- mthc %>% transmute(mpg2=sin(mpg), wt2=sqrt(wt), .outFile=NULL)
    expect_true(is.data.frame(tbl))

    tbl <- mthc %>% summarise(n=n(), mpg2=mean(mpg), .outFile=NULL)
    expect_true(is.data.frame(tbl))

    tbl <- mthc %>% factorise(cyl, gear, .outFile=NULL)
    expect_true(is.data.frame(tbl) && varTypes(tbl)["cyl"] == "factor")
})

test_that("output to xdf works",
{
    tbl <- mthc %>% filter(mpg > 15, cyl <= 6, .outFile="test51")
    expect_true(verifyHdfsData(tbl, "RxXdfData"))
    tbl <- mthc %>% select(mpg, cyl, drat, .outFile="test51")
    expect_true(verifyHdfsData(tbl, "RxXdfData"))
    tbl <- mthc %>% subset(mpg > 15, c(mpg, cyl, drat), .outFile="test51")
    expect_true(verifyHdfsData(tbl, "RxXdfData"))
    tbl <- mthc %>% mutate(mpg2=sin(mpg), wt2=sqrt(wt), .outFile="test51")
    expect_true(verifyHdfsData(tbl, "RxXdfData"))
    tbl <- mthc %>% transmute(mpg2=sin(mpg), wt2=sqrt(wt), .outFile="test51")
    expect_true(verifyHdfsData(tbl, "RxXdfData"))

    tbl <- mthc %>% summarise(n=n(), mpg2=mean(mpg), .outFile="test51")
    expect_true(verifyHdfsData(tbl, "RxXdfData"))

    tbl <- mthc %>% factorise(cyl, gear, .outFile="test51")
    expect_true(verifyHdfsData(tbl, "RxXdfData") && varTypes(tbl)["cyl"] == "factor")
})


hdfs_dir_remove(c("mtcars", "test51"))
