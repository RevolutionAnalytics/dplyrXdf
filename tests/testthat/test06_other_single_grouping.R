context("Other single-table grouping")
mtx <- rxDataStep(mtcars, "mtx.xdf", overwrite=TRUE)
mtt <- rxXdfToText(mtx, "mtx.csv", overwrite=TRUE)


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

test_that("factorise xdf works",
{
    tbl <- mtx %>% group_by(gear) %>% factorise(cyl)
    expect_true(verifyData(tbl, "grouped_tbl_xdf") && varTypes(tbl)["cyl"] == "factor")
    tbl <- mtx %>% group_by(gear) %>% factorise(!!xs)
    expect_true(verifyData(tbl, "grouped_tbl_xdf") && varTypes(tbl)["mpg"] == "factor")
    tbl <- mtx %>% group_by(gear) %>% factorise(all_numeric())
    expect_true(verifyData(tbl, "grouped_tbl_xdf") && all(varTypes(tbl) == "factor"))
})

test_that("factorise data frame works",
{
    tbl <- mtcars %>% group_by(gear) %>% factorise(cyl)
    expect_true(verifyData(tbl, "grouped_df") && varTypes(tbl)["cyl"] == "factor")
    tbl <- mtcars %>% group_by(gear) %>% factorise(!!xs)
    expect_true(verifyData(tbl, "grouped_df") && varTypes(tbl)["mpg"] == "factor")
    tbl <- mtcars %>% group_by(gear) %>% factorise(all_numeric())
    expect_true(verifyData(tbl, "grouped_df") && all(varTypes(tbl) == "factor"))
})

test_that("factorise text works",
{
    tbl <- mtt %>% group_by(gear) %>% factorise(cyl)
    expect_true(verifyData(tbl, "grouped_tbl_xdf") && varTypes(tbl)["cyl"] == "factor")
    tbl <- mtt %>% group_by(gear) %>% factorise(!!xs)
    expect_true(verifyData(tbl, "grouped_tbl_xdf") && varTypes(tbl)["mpg"] == "factor")
    tbl <- mtt %>% group_by(gear) %>% factorise(all_numeric())
    expect_true(verifyData(tbl, "grouped_tbl_xdf") && all(varTypes(tbl) == "factor"))
})

test_that("arrange xdf works",
{
    tbl <- mtx %>% group_by(gear) %>% arrange(cyl)
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(gear) %>% arrange(cyl, .by_group=TRUE)
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(gear) %>% arrange(!!xs)
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
})

test_that("arrange text works",
{
    tbl <- mtt %>% group_by(gear) %>% arrange(cyl)
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtt %>% group_by(gear) %>% arrange(cyl, .by_group=TRUE)
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtt %>% group_by(gear) %>% arrange(!!xs)
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
})

test_that("distinct works",
{
    tbl <- mtx %>% group_by(gear) %>% distinct(cyl)
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(gear) %>% distinct(!!xs)
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(gear) %>% distinct(cyl, .keep_all=TRUE)
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(gear) %>% distinct(!!xs, .keep_all=TRUE)
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
})

test_that("rename works",
{
    tbl <- mtx %>% group_by(gear) %>% rename(mpg2=mpg)
    expect_true(verifyData(tbl, "grouped_tbl_xdf") && names(tbl)[1] == "mpg2")
    tbl <- mtx %>% group_by(gear) %>% rename(mpg2=!!xs)
    expect_true(verifyData(tbl, "grouped_tbl_xdf") && names(tbl)[1] == "mpg2")
    tbl <- mtx %>% group_by(gear) %>% rename(!!xnew := !!xs)
    expect_true(verifyData(tbl, "grouped_tbl_xdf") && names(tbl)[1] == "mpg2")
})

test_that("output to data frame works",
{
    tbl <- mtx %>% group_by(gear) %>% factorise(cyl, .outFile=NULL)
    expect_true(verifyData(tbl, "grouped_df") && varTypes(tbl)["cyl"] == "factor")
    expect_warning(tbl <- mtcars %>% group_by(gear) %>% factorise(cyl, .outFile=NULL))
    expect_true(verifyData(tbl, "grouped_df") && varTypes(tbl)["cyl"] == "factor")
    tbl <- mtt %>% group_by(gear) %>% factorise(cyl, .outFile=NULL)
    expect_true(verifyData(tbl, "grouped_df") && varTypes(tbl)["cyl"] == "factor")
    tbl <- mtt %>% group_by(gear) %>% arrange(cyl, .outFile=NULL)
    expect_true(verifyData(tbl, "grouped_df"))
    tbl <- mtx %>% group_by(gear) %>% distinct(cyl, .outFile=NULL)
    expect_true(verifyData(tbl, "grouped_df"))
    tbl <- mtx %>% group_by(gear) %>% rename(mpg2=mpg, .outFile=NULL)
    expect_true(verifyData(tbl, "grouped_df") && names(tbl)[1] == "mpg2")

})

test_that("output to xdf works",
{
    tbl <- mtx %>% group_by(gear) %>% factorise(cyl, .outFile="test06.xdf")
    expect_true(verifyData(tbl, "RxXdfData") && varTypes(tbl)["cyl"] == "factor")
    expect_warning(tbl <- mtcars %>% group_by(gear) %>% factorise(cyl, .outFile="test06.xdf"))
    expect_true(verifyData(tbl, "grouped_df") && varTypes(tbl)["cyl"] == "factor")
    tbl <- mtt %>% group_by(gear) %>% factorise(cyl, .outFile="test06.xdf")
    expect_true(verifyData(tbl, "RxXdfData") && varTypes(tbl)["cyl"] == "factor")
    tbl <- mtt %>% group_by(gear) %>% arrange(cyl, .outFile="test06.xdf")
    expect_true(verifyData(tbl, "RxXdfData"))
    tbl <- mtx %>% group_by(gear) %>% distinct(cyl, .outFile="test06.xdf")
    expect_true(verifyData(tbl, "RxXdfData"))
    tbl <- mtx %>% group_by(gear) %>% rename(mpg2=mpg, .outFile="test06.xdf")
    expect_true(verifyData(tbl, "RxXdfData") && names(tbl)[1] == "mpg2")
})

test_that(".rxArgs works",
{
    tbl <- mtx %>% group_by(gear) %>% factorise(cyl, .rxArgs=list(sortLevels=TRUE))
    expect_true(verifyData(tbl, "grouped_tbl_xdf") && varTypes(tbl)["cyl"] == "factor")
    expect_warning(tbl <- mtcars %>% group_by(gear) %>% factorise(cyl, .rxArgs=list(overwrite=TRUE)))
    expect_true(verifyData(tbl, "grouped_df") && varTypes(tbl)["cyl"] == "factor")
    tbl <- mtt %>% group_by(gear) %>% factorise(cyl, .rxArgs=list(rowsPerRead=1))
    expect_true(verifyData(tbl, "grouped_tbl_xdf") && varTypes(tbl)["cyl"] == "factor")
    tbl <- mtt %>% group_by(gear) %>% arrange(cyl, .rxArgs=list(type="mergeSort"))
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(gear) %>% distinct(cyl, .rxArgs=list(rowsPerRead=1))
    expect_true(verifyData(tbl, "grouped_tbl_xdf"))
    tbl <- mtx %>% group_by(gear) %>% rename(mpg2=mpg, .rxArgs=list(rowsPerRead=1))
    expect_true(verifyData(tbl, "grouped_tbl_xdf") && names(tbl)[1] == "mpg2")
})

test_that("reset compute context works",
{
    expect_identical(rxGetComputeContext(), cc)
})


file.remove("mtx.xdf", "mtx.csv", "test06.xdf")
