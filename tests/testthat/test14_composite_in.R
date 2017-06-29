context("Composite Xdf input")
mtc <- RxXdfData("mtc", createCompositeSet=TRUE)
rxDataStep(mtcars, mtc, overwrite=TRUE)

verifyCompositeData <- function(xdf, expectedClass)
{
    isTRUE(xdf@createCompositeSet) && is.data.frame(head(xdf)) && class(xdf) == expectedClass # test for exact class
}


test_that("filter works",
{
    tbl <- mtc %>% filter(mpg > 15, cyl <= 6)
    expect_true(verifyCompositeData(tbl, "tbl_xdf"))
})

test_that("select works",
{
    tbl <- mtc %>% select(mpg, cyl, drat)
    expect_true(verifyCompositeData(tbl, "tbl_xdf"))
})

test_that("subset works",
{
    tbl <- mtc %>% subset(mpg > 15, c(mpg, cyl, drat))
    expect_true(verifyCompositeData(tbl, "tbl_xdf"))
})

test_that("mutate works",
{
    tbl <- mtc %>% mutate(mpg2=sin(mpg), wt2=sqrt(wt))
    expect_true(verifyCompositeData(tbl, "tbl_xdf"))
})

test_that("transmute works",
{
    tbl <- mtc %>% transmute(mpg2=sin(mpg), wt2=sqrt(wt))
    expect_true(verifyCompositeData(tbl, "tbl_xdf"))
})

test_that("output to data.frame works",
{
    tbl <- mtc %>% filter(mpg > 15, cyl <= 6, .outFile=NULL)
    expect_true(is.data.frame(tbl))
    tbl <- mtc %>% select(mpg, cyl, drat, .outFile=NULL)
    expect_true(is.data.frame(tbl))
    tbl <- mtc %>% subset(mpg > 15, c(mpg, cyl, drat), .outFile=NULL)
    expect_true(is.data.frame(tbl))
    tbl <- mtc %>% mutate(mpg2=sin(mpg), wt2=sqrt(wt), .outFile=NULL)
    expect_true(is.data.frame(tbl))
    tbl <- mtc %>% transmute(mpg2=sin(mpg), wt2=sqrt(wt), .outFile=NULL)
    expect_true(is.data.frame(tbl))
})

test_that("output to xdf works",
{
    tbl <- mtc %>% filter(mpg > 15, cyl <= 6, .outFile="test14.xdf")
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
    tbl <- mtc %>% select(mpg, cyl, drat, .outFile="test14.xdf")
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
    tbl <- mtc %>% subset(mpg > 15, c(mpg, cyl, drat), .outFile="test14.xdf")
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
    tbl <- mtc %>% mutate(mpg2=sin(mpg), wt2=sqrt(wt), .outFile="test14.xdf")
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
    tbl <- mtc %>% transmute(mpg2=sin(mpg), wt2=sqrt(wt), .outFile="test14.xdf")
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
})


# cleanup
unlink(c("mtc", "test14"), recursive=TRUE)
