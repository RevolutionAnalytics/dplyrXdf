context("Summarise")
mtx <- rxDataStep(mtcars, "mtx.xdf", overwrite=TRUE)

verifyData <- function(xdf, expectedClass)
{
    is.data.frame(head(xdf)) && class(xdf) == expectedClass # test for exact class
}

x <- "mpg"
xs <- rlang::sym(x)

xnew <- "mpg2"

test_that("ungrouped summarise works",
{
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

test_that("ungrouped summarise works with quoting",
{
    tbl <- mtx %>% summarise(n=n(), mpg2=mean(!!xs))
    expect_true(verifyData(tbl, "tbl_xdf"))
    tbl <- mtx %>% summarise(n=n(), mpg2=mean(!!rlang::sym(x)))
    expect_true(verifyData(tbl, "tbl_xdf"))
    tbl <- mtx %>% summarise(n=n(), !!xnew := mean(!!xs))
    expect_true(verifyData(tbl, "tbl_xdf"))
})

test_that("grouped summarise works",
{
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

test_that("grouped summarise works with quoting",
{
    tbl <- mtx %>% group_by(cyl, gear) %>% summarise(n=n(), mpg2=mean(!!xs))
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(cyl, gear) %>% summarise(n=n(), mpg2=mean(!!rlang::sym(x)))
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(cyl, gear) %>% summarise(n=n(), !!xnew := mean(!!xs))
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
})

test_that("output to data frame works", {
    tbl <- mtx %>% summarise(n=n(), mpg2=mean(mpg), .outFile=NULL)
    expect_true(verifyData(tbl, "data.frame"))
    tbl <- mtx %>% group_by(cyl, gear) %>% summarise(n=n(), mpg2=mean(mpg), .outFile=NULL)
    expect_true(verifyData(tbl, "grouped_df"))
})

test_that("output to xdf works", {
    tbl <- mtx %>% summarise(n=n(), mpg2=mean(mpg), .outFile="test03.xdf")
    expect_true(verifyData(tbl, "RxXdfData"))
    tbl <- mtx %>% group_by(cyl, gear) %>% summarise(n=n(), mpg2=mean(mpg), .outFile="test03.xdf")
    expect_true(verifyData(tbl, "RxXdfData"))
})

test_that(".rxArgs works", {
    tbl <- mtx %>% summarise(n=n(), mpg2=mean(mpg2), .rxArgs=list(transformFunc=function(varlst) {
        varlst$mpg2 <- varlst$mpg * 2
        varlst
    }, transformVars="mpg"))
    expect_true(verifyData(tbl, "tbl_xdf"))
    tbl <- mtx %>% group_by(cyl, gear) %>% summarise(n=n(), mpg2=mean(mpg2), .rxArgs=list(transformFunc=function(varlst) {
        varlst$mpg2 <- varlst$mpg * 2
        varlst
    }, transformVars="mpg"))
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
})


file.remove("mtx.xdf", "test03.xdf")
