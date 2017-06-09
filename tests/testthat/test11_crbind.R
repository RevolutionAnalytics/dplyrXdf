context("Bind cols and rows")
mtx <- rxDataStep(mtcars, "mtx.xdf", overwrite=TRUE)
mtx2 <- rxDataStep(mtcars, "mtx2.xdf", overwrite=TRUE)
mtx3 <- rename_all(mtx2, function(x) paste0(x, ".1"))

verifyData <- function(xdf, expectedClass)
{
    is.data.frame(head(xdf)) && class(xdf) == expectedClass # test for exact class
}


test_that("cbind works",
{
    expect_warning(tbl <- cbind(mtx, mtx2))
    expect_true(verifyData(tbl, "tbl_xdf") && ncol(tbl) == ncol(mtx))
    tbl <- cbind(mtx, mtx3)
    expect_true(verifyData(tbl, "tbl_xdf") && ncol(tbl) == 2 * ncol(mtx))
})

test_that("rbind works",
{
    tbl <- rbind(mtx, mtx2)
    expect_true(verifyData(tbl, "tbl_xdf") && nrow(tbl) == 2 * nrow(mtx))
})

test_that("output to data frame works",
{
    tbl <- cbind(mtx, mtx3, .outFile=NULL)
    expect_true(verifyData(tbl, "data.frame") && ncol(tbl) == 2 * ncol(mtx))
    tbl <- rbind(mtx, mtx2, .outFile=NULL)
    expect_true(verifyData(tbl, "data.frame") && nrow(tbl) == 2 * nrow(mtx))
})

test_that("output to xdf works",
{
    tbl <- cbind(mtx, mtx3, .outFile="test11.xdf")
    expect_true(verifyData(tbl, "RxXdfData") && ncol(tbl) == 2 * ncol(mtx))
    tbl <- rbind(mtx, mtx2, .outFile="test11.xdf")
    expect_true(verifyData(tbl, "RxXdfData") && nrow(tbl) == 2 * nrow(mtx))
})

test_that(".rxArgs works",
{
    expect_error(cbind(mtx, mtx3, .rxArgs=list(rowsPerRead=1000)))
    tbl <- cbind.RxXdfData(mtx, mtx3, .rxArgs=list(rowsPerRead=1000))
    expect_true(verifyData(tbl, "tbl_xdf") && ncol(tbl) == 2 * ncol(mtx))
    expect_error(rbind(mtx, mtx2, .rxArgs=list(rowsPerRead=1000)))
    tbl <- rbind.RxXdfData(mtx, mtx2, .rxArgs=list(rowsPerRead=1000))
    expect_true(verifyData(tbl, "tbl_xdf") && nrow(tbl) == 2 * nrow(mtx))
})


# cleanup
file.remove("mtx.xdf", "mtx2.xdf", "test11.xdf")

