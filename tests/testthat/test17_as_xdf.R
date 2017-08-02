context("Coerce to Xdf")

mtx <- rxDataStep(mtcars, "mtx.xdf", overwrite=TRUE)
mtc <- RxXdfData("mtc", createCompositeSet=TRUE)
rxDataStep(mtcars, mtc, overwrite=TRUE)

write.csv(mtcars, "mttext.csv", row.names=FALSE)
mtt <- RxTextData("mttext.csv")

verifyData <- function(xdf, expectedClass)
{
    is.data.frame(head(xdf)) && class(xdf) == expectedClass # test for exact class
}

verifyCompositeData <- function(xdf, expectedClass)
{
    isTRUE(xdf@createCompositeSet) && is.data.frame(head(xdf)) && class(xdf) == expectedClass # test for exact class
}


test_that("as_xdf works",
{
    tbl <- as_xdf(mtx)
    expect_true(verifyData(tbl, "RxXdfData"))
    expect_identical(mtx@file, tbl@file)

    tbl <- as_xdf(mtx, file="test17.xdf")
    expect_true(verifyData(tbl, "RxXdfData"))

    tbl <- as_xdf(mtx, file="test17", composite=TRUE)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
})

test_that("as_standard_xdf works",
{
    tbl <- as_standard_xdf(mtx)
    expect_true(verifyData(tbl, "RxXdfData"))
    expect_identical(mtx@file, tbl@file)

    tbl <- as_standard_xdf(mtx, file="test17.xdf")
    expect_true(verifyData(tbl, "RxXdfData"))
})

test_that("as_composite_xdf works",
{
    tbl <- as_composite_xdf(mtx)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))

    tbl <- as_composite_xdf(mtx, file="test17")
    expect_true(verifyData(tbl, "RxXdfData"))
})

test_that("as_xdf works, composite input",
{
    tbl <- as_xdf(mtc)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
    expect_identical(mtc@file, tbl@file)

    tbl <- as_xdf(mtc, file="test17")
    expect_true(verifyCompositeData(tbl, "RxXdfData"))

    tbl <- as_xdf(mtc, file="test17.xdf", composite=FALSE)
    expect_true(verifyData(tbl, "RxXdfData"))
})

test_that("as_standard_xdf works, composite input",
{
    tbl <- as_standard_xdf(mtc)
    expect_true(verifyData(tbl, "RxXdfData"))

    tbl <- as_standard_xdf(mtc, file="test17.xdf")
    expect_true(verifyData(tbl, "RxXdfData"))
})

test_that("as_composite_xdf works, composite input",
{
    tbl <- as_composite_xdf(mtc)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
    expect_identical(mtc@file, tbl@file)

    tbl <- as_composite_xdf(mtc, file="test17")
    expect_true(verifyData(tbl, "RxXdfData"))
})

test_that("as_xdf works, tbl input",
{
    tbl0 <- mutate(mtx)
    tbl <- as_xdf(tbl0)
    expect_true(verifyData(tbl, "RxXdfData"))
    expect_identical(tbl0@file, tbl@file)

    tbl <- as_xdf(tbl0, "test17.xdf")
    expect_true(verifyData(tbl, "RxXdfData"))

    tbl <- as_xdf(tbl0, "test17", composite=TRUE)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
})

test_that("as_standard_xdf works, tbl input",
{
    tbl0 <- mutate(mtx)
    tbl <- as_standard_xdf(tbl0)
    expect_true(verifyData(tbl, "RxXdfData"))
    expect_identical(tbl0@file, tbl@file)

    tbl <- as_standard_xdf(tbl0, "test17.xdf")
    expect_true(verifyData(tbl, "RxXdfData"))
})

test_that("as_composite_xdf works, tbl input",
{
    tbl0 <- mutate(mtx)
    tbl <- as_composite_xdf(tbl0)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))

    tbl <- as_composite_xdf(tbl0, "test17")
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
})

test_that("as_xdf works, text input",
{
    tbl <- as_xdf(mtt)
    expect_true(verifyData(tbl, "RxXdfData"))

    tbl <- as_xdf(mtt, "test17.xdf")
    expect_true(verifyData(tbl, "RxXdfData"))

    tbl <- as_xdf(mtt, "test17", composite=TRUE)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
})

test_that("as_standard_xdf works, text input",
{
    tbl <- as_standard_xdf(mtt)
    expect_true(verifyData(tbl, "RxXdfData"))
})

test_that("as_composite_xdf works, text input",
{
    tbl <- as_composite_xdf(mtt)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
})

test_that("as_xdf works, data frame input",
{
    tbl <- as_xdf(mtcars)
    expect_true(verifyData(tbl, "RxXdfData"))

    tbl <- as_xdf(mtcars, "test17.xdf")
    expect_true(verifyData(tbl, "RxXdfData"))

    tbl <- as_xdf(mtcars, "test17", composite=TRUE)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
})

test_that("as_standard_xdf works, data frame input",
{
    tbl <- as_standard_xdf(mtcars)
    expect_true(verifyData(tbl, "RxXdfData"))
})

test_that("as_composite_xdf works, data frame input",
{
    tbl <- as_composite_xdf(mtcars)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
})


testFiles <- dir(pattern="^file")
unlink(c("mtx.xdf", "mtc", "mtx", "mtc.xdf", "mttext.csv", "mttext", "mttext.xdf", "mtcars.xdf", "mtcars",
         "test17.xdf", "test17", testFiles),
    recursive=TRUE)

