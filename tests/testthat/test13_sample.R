context("Sampling functionality")
mtx <- rxDataStep(mtcars, "mtx.xdf", overwrite=TRUE)

verifyData <- function(xdf, expectedClass)
{
    is.data.frame(head(xdf)) && class(xdf) == expectedClass # test for exact class
}


test_that("ungrouped sampling works", {
    tbl <- sample_n(mtx, 10)
    expect_true(verifyData(tbl, "tbl_xdf") && nrow(tbl) == 10)
    tbl <- sample_frac(mtx, 0.5)
    expect_true(verifyData(tbl, "tbl_xdf") && nrow(tbl) == 16)
})

test_that("grouped sampling works with rxExecBy",
{
    dplyrxdf_options(useExecBy=TRUE)
    tbl <- mtx %>% group_by(cyl) %>% sample_n(5)
    expect_true(verifyData(tbl, "grouped_tbl_xdf") && nrow(tbl) == 3 * 5)
    tbl <- mtx %>% group_by(cyl) %>% sample_frac(0.5)
    expect_true(verifyData(tbl, "grouped_tbl_xdf") && nrow(tbl) == 17)  # note effect of rounding
})

test_that("grouped sampling works with manual splitting",
{
    dplyrxdf_options(useExecBy=FALSE)
    tbl <- mtx %>% group_by(cyl) %>% sample_n(5)
    expect_true(verifyData(tbl, "grouped_tbl_xdf") && nrow(tbl) == 3 * 5)
    tbl <- mtx %>% group_by(cyl) %>% sample_frac(0.5)
    expect_true(verifyData(tbl, "grouped_tbl_xdf") && nrow(tbl) == 17) # note effect of rounding
})


file.remove("mtx.xdf")
