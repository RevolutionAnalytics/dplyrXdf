context("Grouped HDFS functionality")

# set the compute context manually

detectHdfsConnection()

mthc <- RxXdfData("/user/sshuser/mtcarsc", fileSystem=RxHdfsFileSystem(), createCompositeSet=TRUE)


verifyHdfsData <- function(xdf, expectedClass)
{
    isTRUE(xdf@createCompositeSet) && rxHadoopFileExists(xdf@file) && class(xdf) == expectedClass # test for exact class
}



# basic single-table verbs

test_that("filter works",
{
    tbl <- mthc %>% group_by(gear) %>% filter(mpg > 15, cyl <= 6)
    expect_true(verifyHdfsData(tbl, "grouped_tbl_xdf"))
})

test_that("select works",
{
    tbl <- mthc %>% group_by(gear) %>% select(mpg, cyl, drat)
    expect_true(verifyHdfsData(tbl, "grouped_tbl_xdf"))
})

test_that("subset works",
{
    tbl <- mthc %>% group_by(gear) %>% subset(mpg > 15, c(mpg, cyl, drat))
    expect_true(verifyHdfsData(tbl, "grouped_tbl_xdf"))
})

#test_that("mutate works",
#{
    #tbl <- mthc %>% group_by(gear) %>% mutate(mpg2=sin(mpg), wt2=sqrt(wt))
    #expect_true(verifyHdfsData(tbl, "grouped_tbl_xdf"))
#})

#test_that("transmute works",
#{
    #tbl <- mthc %>% group_by(gear) %>% transmute(mpg2=sin(mpg), wt2=sqrt(wt))
    #expect_true(verifyHdfsData(tbl, "grouped_tbl_xdf"))
#})


# summarise and do

test_that("grouped summarise works",
{
    tbl <- mthc %>% group_by(gear) %>% summarise(n=n(), mpg2=mean(mpg))
    expect_true(verifyHdfsData(tbl, "tbl_xdf"))
    tbl <- mthc %>% group_by(gear) %>% summarise(n=n(), mpg2=mean(mpg), .method=1)
    expect_true(verifyHdfsData(tbl, "tbl_xdf"))
    tbl <- mthc %>% group_by(gear) %>% summarise(n=n(), mpg2=mean(mpg), .method=2)
    expect_true(verifyHdfsData(tbl, "tbl_xdf"))
    tbl <- mthc %>% group_by(gear) %>% summarise(n=n(), mpg2=mean(mpg), .method=3)
    expect_true(verifyHdfsData(tbl, "tbl_xdf"))
    tbl <- mthc %>% group_by(gear) %>% summarise(n=n(), mpg2=mean(mpg), .method=4)
    expect_true(verifyHdfsData(tbl, "tbl_xdf"))
    tbl <- mthc %>% group_by(gear) %>% summarise(n=n(), mpg2=mean(mpg), .method=5)
    expect_true(verifyHdfsData(tbl, "tbl_xdf"))
})

test_that("grouped do works",
{
    tbl <- mthc %>% group_by(gear) %>% do(m=lm(mpg ~ cyl, data=.)) #, w=lm(wt ~ cyl, data=.))
    expect_true(is.data.frame(tbl))
    tbl <- mthc %>% group_by(gear) %>% do(
    {
        .$mpg2 <- sin(.$mpg)
        .$cyl2 <- sqrt(.$cyl)
        .
    })
    expect_true(is.data.frame(tbl))
})

# other single-table verbs

test_that("factorise xdf works",
{
    tbl <- mthc %>% group_by(gear) %>% factorise(cyl, gear)
    expect_true(verifyHdfsData(tbl, "grouped_tbl_xdf") && varTypes(tbl)["cyl"] == "factor")
})

#test_that("arrange xdf works",
#{
    #tbl <- mthc %>% group_by(gear) %>% arrange(cyl, gear)
    #expect_true(verifyHdfsData(tbl, "grouped_tbl_xdf"))
#})

#test_that("distinct works",
#{
    #tbl <- mthc %>% group_by(gear) %>% distinct()
    #expect_true(verifyHdfsData(tbl, "grouped_tbl_xdf"))
    #tbl <- mthc %>% group_by(gear) %>% distinct(cyl, gear)
    #expect_true(verifyHdfsData(tbl, "grouped_tbl_xdf"))
#})

#test_that("rename works",
#{
#tbl <- mthc %>% group_by(gear) %>% rename(mpg2=mpg)
#expect_true(verifyHdfsData(tbl, "grouped_tbl_xdf") && names(tbl)[1] == "mpg2")
#})


# misc functionality

test_that("output to data.frame works",
{
    tbl <- mthc %>% group_by(gear) %>% filter(mpg > 15, cyl <= 6, .outFile=NULL)
    expect_true(is.data.frame(tbl))
    tbl <- mthc %>% group_by(gear) %>% select(mpg, cyl, drat, .outFile=NULL)
    expect_true(is.data.frame(tbl))
    tbl <- mthc %>% group_by(gear) %>% subset(mpg > 15, c(mpg, cyl, drat), .outFile=NULL)
    expect_true(is.data.frame(tbl))
    #tbl <- mthc %>% group_by(gear) %>% mutate(mpg2=sin(mpg), wt2=sqrt(wt), .outFile=NULL)
    #expect_true(is.data.frame(tbl))
    #tbl <- mthc %>% group_by(gear) %>% transmute(mpg2=sin(mpg), wt2=sqrt(wt), .outFile=NULL)
    #expect_true(is.data.frame(tbl))

    tbl <- mthc %>% group_by(gear) %>% summarise(n=n(), mpg2=mean(mpg), .outFile=NULL)
    expect_true(is.data.frame(tbl))

    tbl <- mthc %>% group_by(gear) %>% factorise(cyl, gear, .outFile=NULL)
    expect_true(is.data.frame(tbl) && varTypes(tbl)["cyl"] == "factor")
    #tbl <- mthc %>% group_by(gear) %>% distinct(cyl, gear, .outFile=NULL)
    #expect_true(is.data.frame(tbl))
    #tbl <- mthc %>% group_by(gear) %>% rename(mpg2=mpg, .outFile=NULL)
    #expect_true(is.data.frame(tbl) && names(tbl)[1] == "mpg2")
})

test_that("output to xdf works",
{
    tbl <- mthc %>% group_by(gear) %>% filter(mpg > 15, cyl <= 6, .outFile="test16")
    expect_true(verifyHdfsData(tbl, "RxXdfData"))
    tbl <- mthc %>% group_by(gear) %>% select(mpg, cyl, drat, .outFile="test16")
    expect_true(verifyHdfsData(tbl, "RxXdfData"))
    tbl <- mthc %>% group_by(gear) %>% subset(mpg > 15, c(mpg, cyl, drat), .outFile="test16")
    expect_true(verifyHdfsData(tbl, "RxXdfData"))
    #tbl <- mthc %>% group_by(gear) %>% mutate(mpg2=sin(mpg), wt2=sqrt(wt), .outFile="test16")
    #expect_true(verifyHdfsData(tbl, "RxXdfData"))
    #tbl <- mthc %>% group_by(gear) %>% transmute(mpg2=sin(mpg), wt2=sqrt(wt), .outFile="test16")
    #expect_true(verifyHdfsData(tbl, "RxXdfData"))

    tbl <- mthc %>% group_by(gear) %>% summarise(n=n(), mpg2=mean(mpg), .outFile="test16")
    expect_true(verifyHdfsData(tbl, "RxXdfData"))

    tbl <- mthc %>% group_by(gear) %>% factorise(cyl, gear, .outFile="test16")
    expect_true(verifyHdfsData(tbl, "RxXdfData") && varTypes(tbl)["cyl"] == "factor")
    #tbl <- mthc %>% group_by(gear) %>% distinct(cyl, gear, .outFile="test16")
    #expect_true(verifyHdfsData(tbl, "RxXdfData"))
    #tbl <- mthc %>% group_by(gear) %>% rename(mpg2=mpg, .outFile="test16")
    #expect_true(verifyHdfsData(tbl, "RxXdfData") && names(tbl)[1] == "mpg2")
})


# cleanup
rxHadoopRemoveDir("test16")
