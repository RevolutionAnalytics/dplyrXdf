context("Summarise")
mtx <- rxDataStep(mtcars, "mtx.xdf", overwrite=TRUE)

verifyData <- function(xdf, expectedClass)
{
    is.data.frame(head(xdf)) && class(xdf) == expectedClass  # test for exact class
}

library(rlang)
x <- "mpg"
xs <- sym(x)

xnew <- "mpg2"

test_that("ungrouped summarise works", {
    tbl <- mtx %>% summarise(n=n(), mpg2=mean(mpg))
    expect_true(verifyData(tbl, "tbl_xdf"))
    expect_warning(tbl <- mtx %>% summarise(n=n(), mpg2=mean(mpg), .method=1))
    expect_true(verifyData(tbl, "tbl_xdf"))
    tbl <- mtx %>% summarise(n=n(), mpg2=mean(mpg), .method=2)
    expect_true(verifyData(tbl, "tbl_xdf"))
    expect_warning(tbl <- mtx %>% summarise(n=n(), mpg2=mean(mpg), .method=3))
    expect_true(verifyData(tbl, "tbl_xdf"))
    tbl <- mtx %>% summarise(n=n(), mpg2=mean(mpg), .method=4)
    expect_true(verifyData(tbl, "tbl_xdf"))
    tbl <- mtx %>% summarise(n=n(), mpg2=mean(mpg), .method=5)
    expect_true(verifyData(tbl, "tbl_xdf"))
})

test_that("ungrouped summarise works with quoting", {
    tbl <- mtx %>% summarise(n=n(), mpg2=mean(!!xs))
    expect_true(verifyData(tbl, "tbl_xdf"))
    tbl <- mtx %>% summarise(n=n(), mpg2=mean(!!sym(x)))
    expect_true(verifyData(tbl, "tbl_xdf"))
    tbl <- mtx %>% summarise(n=n(), !!xnew := mean(!!xs))
    expect_true(verifyData(tbl, "tbl_xdf"))
})

test_that("grouped summarise works", {
    tbl <- mtx %>% group_by(cyl, gear) %>% summarise(n=n(), mpg2=mean(mpg))
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(cyl, gear) %>% summarise(n=n(), mpg2=mean(mpg), .method=1)
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(cyl, gear) %>% summarise(n=n(), mpg2=mean(mpg), .method=2)
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(cyl, gear) %>% summarise(n=n(), mpg2=mean(mpg), .method=3)
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(cyl, gear) %>% summarise(n=n(), mpg2=mean(mpg), .method=4)
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(cyl, gear) %>% summarise(n=n(), mpg2=mean(mpg), .method=5)
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
})


