context("Use rxExecBy")
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
    dplyrxdf_options(useExecBy=TRUE)
    expect_true(dplyrxdf_options()$useExecBy)
})

test_that("distinct works",
{
    tbl <- mtx %>% group_by(gear) %>% distinct()
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(gear) %>% distinct(cyl)
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(gear) %>% distinct(!!xs)
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(gear) %>% distinct(cyl, .keep_all=TRUE)
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(gear) %>% distinct(!!xs, .keep_all=TRUE)
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))

    tbl <- mtx %>% group_by(gear) %>% distinct(cyl, .outFile=NULL)
    expect_true(verifyData(tbl, "grouped_df"))
    tbl <- mtx %>% group_by(gear) %>% distinct(cyl, .outFile="test12.xdf")
    expect_true(verifyData(tbl, "RxXdfData"))

    tbl <- mtx %>% group_by(gear) %>% distinct(cyl, .rxArgs=list(rowsPerRead=1))
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
})

test_that("grouped do works",
{
    tbl <- mtx %>% group_by(gear) %>% do(m=lm(mpg ~ cyl, data=.), w=lm(wt ~ cyl, data=.))
    expect_true(verifyData(tbl, "rowwise_df"))
    tbl <- mtx %>% group_by(gear) %>% do(m=lm(!!rlang::sym(x) ~ cyl, data=.), w=lm(wt ~ cyl, data=.))
    expect_true(verifyData(tbl, "rowwise_df"))
    tbl <- mtx %>% group_by(gear) %>% do(m=lm(!!xs ~ cyl, data=.), w=lm(wt ~ cyl, data=.))
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
    tbl <- mtx %>% group_by(gear) %>% do_xdf(m=rxLinMod(mpg ~ cyl, data=.), w=rxLinMod(wt ~ cyl, data=.))
    expect_true(verifyData(tbl, "rowwise_df"))
    tbl <- mtx %>% group_by(gear) %>% do_xdf(m=rxLinMod(!!rlang::sym(x) ~ cyl, data=.), w=rxLinMod(wt ~ cyl, data=.))
    expect_true(verifyData(tbl, "rowwise_df"))
    tbl <- mtx %>% group_by(gear) %>% do_xdf(m=rxLinMod(!!xs ~ cyl, data=.), w=rxLinMod(wt ~ cyl, data=.))
    expect_true(verifyData(tbl, "rowwise_df"))
    tbl <- mtx %>% group_by(gear) %>% do_xdf(rxDataStep(., transformFunc=function(.)
    {
        .$mpg2 <- sin(.$mpg)
        .$cyl2 <- sqrt(.$cyl)
        .
    }))
    expect_true(verifyData(tbl, "grouped_df"))
})

test_that("mutate works",
{
    tbl <- mtx %>% group_by(gear) %>% mutate(mpg2=sin(mpg), wt2=sqrt(wt))
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(gear) %>% mutate(mpg2=sin(!!xs), wt2=sqrt(wt))
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(gear) %>% mutate(!!xnew := sin(!!xs), wt2=sqrt(wt))
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))

    tbl <- mtx %>% group_by(gear) %>% mutate(mpg2=sin(mpg), wt2=sqrt(wt), .outFile=NULL)
    expect_true(verifyData(tbl, "grouped_df"))
    tbl <- mtx %>% group_by(gear) %>% mutate(mpg2=sin(mpg), wt2=sqrt(wt), .outFile="test12.xdf")
    expect_true(verifyData(tbl, "RxXdfData"))
    tbl <- mtx %>% group_by(gear) %>% mutate(wt2=sqrt(wt), .rxArgs=list(transformFunc=function(varlst)
    {
        varlst$mpg2 <- sin(varlst$mpg)
        varlst
    }))
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
})

test_that("transmute works",
{
    tbl <- mtx %>% group_by(gear) %>% transmute(mpg2=sin(mpg), wt2=sqrt(wt))
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(gear) %>% transmute(mpg2=sin(!!xs), wt2=sqrt(wt))
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(gear) %>% transmute(!!xnew := sin(!!xs), wt2=sqrt(wt))
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))

    tbl <- mtx %>% group_by(gear) %>% transmute(mpg2=sin(mpg), wt2=sqrt(wt), .outFile=NULL)
    expect_true(verifyData(tbl, "grouped_df"))
    tbl <- mtx %>% group_by(gear) %>% transmute(mpg2=sin(mpg), wt2=sqrt(wt), .outFile="test12.xdf")
    expect_true(verifyData(tbl, "RxXdfData"))
    tbl <- mtx %>% group_by(gear) %>% transmute(.rxArgs=list(transformFunc=function(varlst)
    {
        varlst$mpg2 <- sin(varlst$mpg)
        varlst$wt2 <- sqrt(varlst$wt)
        varlst
    }))
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
})

test_that("grouped summarise works",
{
    tbl <- mtx %>% group_by(cyl, gear) %>% summarise(n=n(), mpg2=mean(mpg), .method=4)
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(cyl, gear) %>% summarise(n=n(), mpg2=mean(mpg), .method=5)
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
})

test_that("grouped summarise works with implicit factoring",
{
    tbl <- mtx %>% mutate(x=as.character(sample(4, .rxNumRows, TRUE))) %>% group_by(x) %>% summarise(n=n(), .method=4)
    expect_true(verifyData(tbl, "tbl_xdf"))
    tbl <- mtx %>% mutate(x=as.character(sample(4, .rxNumRows, TRUE))) %>% group_by(x) %>% summarise(n=n(), .method=5)
    expect_true(verifyData(tbl, "tbl_xdf"))
})

test_that("reset compute context works",
{
    expect_identical(rxGetComputeContext(), cc)
})

# cleanup
file.remove("mtx.xdf", "test12.xdf")
