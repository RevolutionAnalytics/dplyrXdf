context("Factorise")
ftest <- data.frame(c=letters[1:10], f=factor(letters[1:10]), n=1:10, x=as.double(1:10), b=rep(c(TRUE, FALSE), 5),
                    stringsAsFactors=FALSE)
ftest <- rxDataStep(ftest, "ftest.xdf", overwrite=TRUE)
ftestc <- rxXdfToText(ftest, "ftest.csv", overwrite=TRUE)

verifyData <- function(xdf, expectedClass)
{
    is.data.frame(head(xdf)) && class(xdf) == expectedClass  # test for exact class
}

nFactorCols <- function(xdf)
{
    types <- rxGetVarInfo(xdf)
    sum(sapply(types, "[[", "varType") == "factor")
}


test_that("column select works", {
    tbl <- ftest %>% factorise(c, f, n)
    expect_true(verifyData(tbl, "tbl_xdf") && ncol(tbl) == 5 && nFactorCols(tbl) == 3)
    tbl <- ftest %>% factorise(starts_with("d"))
    expect_true(verifyData(tbl, "tbl_xdf") && ncol(tbl) == 5 && nFactorCols(tbl) == 1)
    tbl <- ftest %>% factorise(all_numeric())
    expect_true(verifyData(tbl, "tbl_xdf") && ncol(tbl) == 5 && nFactorCols(tbl) == 4)
    tbl <- ftest %>% factorise(1,2,3,4)
    expect_true(verifyData(tbl, "tbl_xdf") && ncol(tbl) == 5 && nFactorCols(tbl) == 4)
})

test_that("named arguments work", {
    tbl <- ftest %>% factorise(c=c("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"))
    expect_true(verifyData(tbl, "tbl_xdf") && ncol(tbl) == 5 && nFactorCols(tbl) == 2)
    tbl <- ftest %>% factorise(c, n, b=c(0, 1))  # logicals are actually integer 0/1 in an xdf
    expect_true(verifyData(tbl, "tbl_xdf") && ncol(tbl) == 5 && nFactorCols(tbl) == 4)
})

test_that("column select works, csv input", {
    tbl <- ftestc %>% factorise(c, f, n)
    expect_true(verifyData(tbl, "tbl_xdf") && ncol(tbl) == 5 && nFactorCols(tbl) == 3)
    tbl <- ftestc %>% factorise(starts_with("d"))
    expect_true(verifyData(tbl, "tbl_xdf") && ncol(tbl) == 5 && nFactorCols(tbl) == 0)
    tbl <- ftestc %>% factorise(all_numeric())
    expect_true(verifyData(tbl, "tbl_xdf") && ncol(tbl) == 5 && nFactorCols(tbl) == 3)
    tbl <- ftestc %>% factorise(1,2,3,4)
    expect_true(verifyData(tbl, "tbl_xdf") && ncol(tbl) == 5 && nFactorCols(tbl) == 4)
})

test_that("named arguments work, csv input", {
    tbl <- ftestc %>% factorise(c=c("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"))
    expect_true(verifyData(tbl, "tbl_xdf") && ncol(tbl) == 5 && nFactorCols(tbl) == 1)
    tbl <- ftestc %>% factorise(c, n, b=c(0, 1))  # logicals are actually integer 0/1 in an xdf
    expect_true(verifyData(tbl, "tbl_xdf") && ncol(tbl) == 5 && nFactorCols(tbl) == 3)
})

