context("Do")
mtx <- rxDataStep(mtcars, "mtx.xdf", overwrite=TRUE)

verifyData <- function(xdf, expectedClass)
{
    is.data.frame(head(xdf)) && class(xdf) == expectedClass # test for exact class
}

x <- "mpg"
xs <- rlang::sym(x)

xnew <- "mpg2"

cc <- rxGetComputeContext()

test_that("set useExecBy works",
{
    dplyrxdf_options(useExecBy=FALSE)
    expect_false(dplyrxdf_options()$useExecBy)
})

test_that("ungrouped do works",
{
    tbl <- mtx %>% do(m=lm(mpg ~ cyl, data=.), w=lm(wt ~ cyl, data=.))
    expect_true(verifyData(tbl, "tbl_df"))
    tbl <- mtx %>% do(m=lm(!!rlang::sym(x) ~ cyl, data=.), w=lm(wt ~ cyl, data=.))
    expect_true(verifyData(tbl, "tbl_df"))
    tbl <- mtx %>% do(m=lm(!!xs ~ cyl, data=.), w=lm(wt ~ cyl, data=.))
    expect_true(verifyData(tbl, "tbl_df"))
    tbl <- mtx %>% do(
    {
        .$mpg2 <- sin(.$mpg)
        .$cyl2 <- sqrt(.$cyl)
        .
    })
    expect_true(verifyData(tbl, "data.frame"))
})

test_that("ungrouped do_xdf works",
{
    tbl <- mtx %>% do_xdf(m = rxLinMod(mpg ~ cyl, data = .), w = rxLinMod(wt ~ cyl, data = .))
    expect_true(verifyData(tbl, "tbl_df"))
    tbl <- mtx %>% do_xdf(m = rxLinMod(!!rlang::sym(x) ~ cyl, data = .), w = rxLinMod(wt ~ cyl, data = .))
    expect_true(verifyData(tbl, "tbl_df"))
    tbl <- mtx %>% do_xdf(m = rxLinMod(!!xs ~ cyl, data = .), w = rxLinMod(wt ~ cyl, data = .))
    expect_true(verifyData(tbl, "tbl_df"))
    tbl <- mtx %>% do_xdf(rxDataStep(., transformFunc=function(.)
    {
        .$mpg2 <- sin(.$mpg)
        .$cyl2 <- sqrt(.$cyl)
        .
    }))
    expect_true(verifyData(tbl, "tbl_df"))
})

test_that("grouped do works",
{
    tbl <- mtx %>% group_by(gear) %>% do(m = lm(mpg ~ cyl, data = .), w = lm(wt ~ cyl, data = .))
    expect_true(verifyData(tbl, "rowwise_df"))
    tbl <- mtx %>% group_by(gear) %>% do(m = lm(!!rlang::sym(x) ~ cyl, data = .), w = lm(wt ~ cyl, data = .))
    expect_true(verifyData(tbl, "rowwise_df"))
    tbl <- mtx %>% group_by(gear) %>% do(m = lm(!!xs ~ cyl, data = .), w = lm(wt ~ cyl, data = .))
    expect_true(verifyData(tbl, "rowwise_df"))
    tbl <- mtx %>% group_by(gear) %>% do(
    {
        .$mpg2 <- sin(.$mpg)
        .$cyl2 <- sqrt(.$cyl)
        .
    })
    expect_true(verifyData(tbl, "grouped_df"))
})

test_that("grouped do_xdf works",
{
    tbl <- mtx %>% group_by(gear) %>% do_xdf(m = rxLinMod(mpg ~ cyl, data = .), w = rxLinMod(wt ~ cyl, data = .))
    expect_true(verifyData(tbl, "rowwise_df"))
    tbl <- mtx %>% group_by(gear) %>% do_xdf(m = rxLinMod(!!rlang::sym(x) ~ cyl, data = .), w = rxLinMod(wt ~ cyl, data = .))
    expect_true(verifyData(tbl, "rowwise_df"))
    tbl <- mtx %>% group_by(gear) %>% do_xdf(m = rxLinMod(!!xs ~ cyl, data = .), w = rxLinMod(wt ~ cyl, data = .))
    expect_true(verifyData(tbl, "rowwise_df"))
    tbl <- mtx %>% group_by(gear) %>% do_xdf(rxDataStep(., transformFunc = function(.)
    {
        .$mpg2 <- sin(.$mpg)
        .$cyl2 <- sqrt(.$cyl)
        .
    }))
    expect_true(verifyData(tbl, "grouped_df"))
})

test_that("reset compute context works",
{
    expect_identical(rxGetComputeContext(), cc)
})


file.remove("mtx.xdf")
