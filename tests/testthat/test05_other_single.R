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
    expect_true(verifyData(tbl, "tbl_xdf") && varTypes(tbl)["cyl"] == "factor")
    tbl <- mtx %>% factorise(!!xs)
    expect_true(verifyData(tbl, "tbl_xdf") && varTypes(tbl)["mpg"] == "factor")
    tbl <- mtx %>% factorise(all_numeric())
    expect_true(verifyData(tbl, "tbl_xdf") && all(varTypes(tbl) == "factor"))
})

test_that("factorise data frame works",
{
    tbl <- mtcars %>% factorise(cyl, gear)
    expect_true(verifyData(tbl, "data.frame") && varTypes(tbl)["cyl"] == "factor")
    tbl <- mtcars %>% factorise(!!xs)
    expect_true(verifyData(tbl, "data.frame") && varTypes(tbl)["mpg"] == "factor")
    tbl <- mtcars %>% factorise(all_numeric())
    expect_true(verifyData(tbl, "data.frame") && all(varTypes(tbl) == "factor"))
})

test_that("factorise text works",
{
    tbl <- mtt %>% factorise(cyl, gear)
    expect_true(verifyData(tbl, "tbl_xdf") && varTypes(tbl)["cyl"] == "factor")
    tbl <- mtt %>% factorise(!!xs)
    expect_true(verifyData(tbl, "tbl_xdf") && varTypes(tbl)["mpg"] == "factor")
    tbl <- mtt %>% factorise(all_numeric())
    expect_true(verifyData(tbl, "tbl_xdf") && all(varTypes(tbl) == "factor"))
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
    tbl <- mtx %>% distinct()
    expect_true(verifyData(tbl, "tbl_xdf"))
    tbl <- mtx %>% distinct(cyl, gear)
    expect_true(verifyData(tbl, "tbl_xdf"))
    tbl <- mtx %>% distinct(!!xs)
    expect_true(verifyData(tbl, "tbl_xdf"))
    tbl <- mtx %>% distinct(cyl, gear, .keep_all=TRUE)
    expect_true(verifyData(tbl, "tbl_xdf"))
    tbl <- mtx %>% distinct(!!xs, .keep_all=TRUE)
    expect_true(verifyData(tbl, "tbl_xdf"))
})

test_that("rename works",
{
    tbl <- mtx %>% rename(mpg2=mpg)
    expect_true(verifyData(tbl, "tbl_xdf") && names(tbl)[1] == "mpg2")
    tbl <- mtx %>% rename(mpg2=!!xs)
    expect_true(verifyData(tbl, "tbl_xdf") && names(tbl)[1] == "mpg2")
    tbl <- mtx %>% rename(!!xnew := !!xs)
    expect_true(verifyData(tbl, "tbl_xdf") && names(tbl)[1] == "mpg2")
})

test_that("output to data frame works",
{
    tbl <- mtx %>% factorise(cyl, gear, .outFile=NULL)
    expect_true(verifyData(tbl, "data.frame") && varTypes(tbl)["cyl"] == "factor")
    expect_warning(tbl <- mtcars %>% factorise(cyl, gear, .outFile=NULL))
    expect_true(verifyData(tbl, "data.frame") && varTypes(tbl)["cyl"] == "factor")
    tbl <- mtt %>% factorise(cyl, gear, .outFile=NULL)
    expect_true(verifyData(tbl, "data.frame") && varTypes(tbl)["cyl"] == "factor")
    tbl <- mtt %>% arrange(cyl, gear, .outFile=NULL)
    expect_true(verifyData(tbl, "data.frame"))
    tbl <- mtx %>% distinct(cyl, gear, .outFile=NULL)
    expect_true(verifyData(tbl, "data.frame"))
    tbl <- mtx %>% rename(mpg2=mpg, .outFile=NULL)
    expect_true(verifyData(tbl, "data.frame") && names(tbl)[1] == "mpg2")
})

test_that("output to xdf works",
{
    tbl <- mtx %>% factorise(cyl, gear, .outFile="test05.xdf")
    expect_true(verifyData(tbl, "RxXdfData") && varTypes(tbl)["cyl"] == "factor")
    expect_warning(tbl <- mtcars %>% factorise(cyl, gear, .outFile="test05.xdf"))
    expect_true(verifyData(tbl, "data.frame") && varTypes(tbl)["cyl"] == "factor")
    tbl <- mtt %>% factorise(cyl, gear, .outFile="test05.xdf")
    expect_true(verifyData(tbl, "RxXdfData") && varTypes(tbl)["cyl"] == "factor")
    tbl <- mtt %>% arrange(cyl, gear, .outFile="test05.xdf")
    expect_true(verifyData(tbl, "RxXdfData"))
    tbl <- mtx %>% distinct(cyl, gear, .outFile="test05.xdf")
    expect_true(verifyData(tbl, "RxXdfData"))
    tbl <- mtx %>% rename(mpg2=mpg, .outFile="test05.xdf")
    expect_true(verifyData(tbl, "RxXdfData") && names(tbl)[1] == "mpg2")
})

test_that(".rxArgs works",
{
    tbl <- mtx %>% factorise(cyl, gear, .rxArgs=list(sortLevels=TRUE))
    expect_true(verifyData(tbl, "tbl_xdf") && varTypes(tbl)["cyl"] == "factor")
    expect_warning(tbl <- mtcars %>% factorise(cyl, gear, .rxArgs=list(overwrite=TRUE)))
    expect_true(verifyData(tbl, "data.frame") && varTypes(tbl)["cyl"] == "factor")
    tbl <- mtt %>% factorise(cyl, gear, .rxArgs=list(rowsPerRead=1))
    expect_true(verifyData(tbl, "tbl_xdf") && varTypes(tbl)["cyl"] == "factor")
    tbl <- mtt %>% arrange(cyl, gear, .rxArgs=list(type="mergeSort"))
    expect_true(verifyData(tbl, "tbl_xdf"))
    tbl <- mtx %>% distinct(cyl, gear, .rxArgs=list(rowsPerRead=1))
    expect_true(verifyData(tbl, "tbl_xdf"))
    tbl <- mtx %>% rename(mpg2=mpg, .rxArgs=list(rowsPerRead=1))
    expect_true(verifyData(tbl, "tbl_xdf") && names(tbl)[1] == "mpg2")
})


file.remove("mtx.xdf", "mtx.csv", "test05.xdf")
