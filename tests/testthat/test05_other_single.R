context("Other single-table verbs")
mtx <- rxDataStep(mtcars, "mtx.xdf", overwrite = TRUE)
mtt <- rxXdfToText(mtx, "mtx.csv", overwrite=TRUE)


verifyData <- function(xdf, expectedClass)
{
    is.data.frame(head(xdf)) && class(xdf) == expectedClass # test for exact class
}

x <- "mpg"
xs <- rlang::sym(x)

xnew <- "mpg2"

test_that("factorise xdf works",
{
    tbl <- mtx %>% factorise(cyl, gear)
    expect_true(verifyData(tbl, "tbl_xdf"))
    tbl <- mtx %>% factorise(!!xs)
    expect_true(verifyData(tbl, "tbl_xdf"))
    tbl <- mtx %>% factorise(all_numeric())
    expect_true(verifyData(tbl, "tbl_xdf"))
})

test_that("factorise text works",
{
    tbl <- mtt %>% factorise(cyl, gear)
    expect_true(verifyData(tbl, "tbl_xdf"))
    tbl <- mtt %>% factorise(!!xs)
    expect_true(verifyData(tbl, "tbl_xdf"))
    tbl <- mtt %>% factorise(all_numeric())
    expect_true(verifyData(tbl, "tbl_xdf"))
})

test_that("arrange xdf works",
{
    tbl <- mtx %>% arrange(cyl, gear)
    expect_true(verifyData(tbl, "tbl_xdf"))
    tbl <- mtx %>% arrange(!!xs)
    expect_true(verifyData(tbl, "tbl_xdf"))
})

test_that("arrange text works",
{
    tbl <- mtt %>% arrange(cyl, gear)
    expect_true(verifyData(tbl, "tbl_xdf"))
    tbl <- mtt %>% arrange(!!xs)
    expect_true(verifyData(tbl, "tbl_xdf"))
})

test_that("distinct works",
{
    tbl <- mtx %>% distinct(cyl, gear)
    expect_true(verifyData(tbl, "tbl_xdf"))
    tbl <- mtx %>% distinct(!!xs)
    expect_true(verifyData(tbl, "tbl_xdf"))
})

test_that("rename works",
{
    tbl <- mtx %>% rename(mpg2=mpg)
    expect_true(verifyData(tbl, "tbl_xdf"))
    tbl <- mtx %>% rename(mpg2=!!xs)
    expect_true(verifyData(tbl, "tbl_xdf"))
    tbl <- mtx %>% rename(!!xnew := !!xs)
    expect_true(verifyData(tbl, "tbl_xdf"))
})

file.remove("mtx.xdf", "mtx.csv")
