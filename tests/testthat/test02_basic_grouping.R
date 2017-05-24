context("Grouping functionality")
mtx <- rxDataStep(mtcars, "mtx.xdf", overwrite=TRUE)

verifyData <- function(xdf, expectedClass)
{
    is.data.frame(head(xdf)) && class(xdf) == expectedClass  # test for exact class
}

library(rlang)
x <- "mpg"
xs <- sym(x)

xnew <- "mpg2"

test_that("filter works", {
    tbl <- mtx %>% group_by(gear) %>% filter(mpg > 15, cyl <= 6)
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(gear) %>% filter((!!xs) > 15, cyl <= 6)
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(gear) %>% filter((!!sym(x)) > 15, cyl <= 6)
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
})

test_that("select works", {
    tbl <- mtx %>% group_by(gear) %>% select(mpg, cyl, drat)
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(gear) %>% select(!!xs, cyl, drat)
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(gear) %>% select(!!sym(x), cyl, drat)
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
})

test_that("subset works", {
    tbl <- mtx %>% group_by(gear) %>% subset(mpg > 15, c(mpg, cyl, drat))
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(gear) %>% subset((!!xs) > 15, c(!!xs, cyl, drat))
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(gear) %>% subset((!!sym(x)) > 15, c(!!sym(x), cyl, drat))
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
})

test_that("mutate works", {
    tbl <- mtx %>% group_by(gear) %>% mutate(mpg2=sin(mpg), wt2=sqrt(wt))
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(gear) %>% mutate(mpg2=sin(!!xs), wt2=sqrt(wt))
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(gear) %>% mutate(!!xnew := sin(!!xs), wt2=sqrt(wt))
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
})

test_that("transmute works", {
    tbl <- mtx %>% group_by(gear) %>% transmute(mpg2=sin(mpg), wt2=sqrt(wt))
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(gear) %>% transmute(mpg2=sin(!!xs), wt2=sqrt(wt))
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(gear) %>% transmute(!!xnew := sin(!!xs), wt2=sqrt(wt))
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
})

test_that("output to data.frame works", {
    tbl <- mtx %>% group_by(gear) %>% filter(mpg > 15, cyl <= 6, .outFile=NULL)
    expect_true(is.data.frame(tbl))
    tbl <- mtx %>% group_by(gear) %>% select(mpg, cyl, drat, .outFile=NULL)
    expect_true(is.data.frame(tbl))
    tbl <- mtx %>% group_by(gear) %>% subset(mpg > 15, c(mpg, cyl, drat), .outFile=NULL)
    expect_true(is.data.frame(tbl))
    tbl <- mtx %>% group_by(gear) %>% mutate(mpg2=sin(mpg), wt2=sqrt(wt), .outFile=NULL)
    expect_true(is.data.frame(tbl))
    tbl <- mtx %>% group_by(gear) %>% transmute(mpg2=sin(mpg), wt2=sqrt(wt), .outFile=NULL)
    expect_true(is.data.frame(tbl))
})

test_that("output to xdf works", {
    tbl <- mtx %>% group_by(gear) %>% filter(mpg > 15, cyl <= 6, .outFile="test01.xdf")
    expect_true(verifyData(tbl, "RxXdfData"))
    tbl <- mtx %>% group_by(gear) %>% select(mpg, cyl, drat, .outFile="test01.xdf")
    expect_true(verifyData(tbl, "RxXdfData"))
    tbl <- mtx %>% group_by(gear) %>% subset(mpg > 15, c(mpg, cyl, drat), .outFile="test01.xdf")
    expect_true(verifyData(tbl, "RxXdfData"))
    tbl <- mtx %>% group_by(gear) %>% mutate(mpg2=sin(mpg), wt2=sqrt(wt), .outFile="test01.xdf")
    expect_true(verifyData(tbl, "RxXdfData"))
    tbl <- mtx %>% group_by(gear) %>% transmute(mpg2=sin(mpg), wt2=sqrt(wt), .outFile="test01.xdf")
    expect_true(verifyData(tbl, "RxXdfData"))
})

test_that(".rxArgs works", {
    tbl <- mtx %>% group_by(gear) %>% filter(mpg > 15, cyl <= 6, .rxArgs=list(transformFunc=function(varlst) {
        varlst$mpg2 <- sin(varlst$mpg)
        varlst
    }))
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(gear) %>% select(mpg, cyl, drat, .rxArgs=list(transformFunc=function(varlst) {
        varlst$mpg2 <- sin(varlst$mpg)
        varlst
    }))
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(gear) %>% subset(mpg > 15, c(mpg, cyl, drat), .rxArgs=list(transformFunc=function(varlst) {
        varlst$mpg2 <- sin(varlst$mpg)
        varlst
    }))
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(gear) %>% mutate(wt2=sqrt(wt), .rxArgs=list(transformFunc=function(varlst) {
        varlst$mpg2 <- sin(varlst$mpg)
        varlst
    }))
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(gear) %>% transmute(.rxArgs=list(transformFunc=function(varlst) {
        varlst$mpg2 <- sin(varlst$mpg)
        varlst$wt2 <- sqrt(varlst$wt)
        varlst
    }))
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
})


# cleanup
file.remove("mtx.xdf", "test01.xdf")

