context("Column selections")
mtx <- rxDataStep(mtcars, "mtx.xdf", overwrite=TRUE)

verifyData <- function(xdf, expectedClass)
{
    is.data.frame(head(xdf)) && class(xdf) == expectedClass  # test for exact class
}


test_that("select works", {
    tbl <- mtx %>% select(starts_with("d"))
    expect_true(verifyData(tbl, "tbl_xdf") && ncol(tbl) == 2)
    tbl <- mtx %>% select(1,2,3,4)
    expect_true(verifyData(tbl, "tbl_xdf") && ncol(tbl) == 4)
    mtx %>% select(1:4)
    expect_true(verifyData(tbl, "tbl_xdf") && ncol(tbl) == 4)
    tbl <- mtx %>% select_("mpg", "cyl")
    expect_true(verifyData(tbl, "tbl_xdf") && ncol(tbl) == 2)
    tbl <- mtx %>% select_(.dots=c("mpg", "cyl"))
    expect_true(verifyData(tbl, "tbl_xdf") && ncol(tbl) == 2)
    expect_error(mtx %>% select(matches("xyz")))
    expect_error(mtx %>% select(starts_with("xyz")))
    expect_error(mtx %>% select(contains("xyz")))
})

test_that("subset works", {
    tbl <- mtx %>% subset(, starts_with("d"))
    expect_true(verifyData(tbl, "tbl_xdf") && ncol(tbl) == 2)
    tbl <- mtx %>% subset(, 1:4)
    expect_true(verifyData(tbl, "tbl_xdf") && ncol(tbl) == 4)
    tbl <- mtx %>% subset_(, c("mpg", "cyl"))
    expect_true(verifyData(tbl, "tbl_xdf") && ncol(tbl) == 2)
    expect_error(mtx %>% subset(, matches("xyz")))
    expect_error(mtx %>% subset(, starts_with("xyz")))
    expect_error(mtx %>% subset(, contains("xyz")))
})


# cleanup
file.remove("mtx.xdf")
