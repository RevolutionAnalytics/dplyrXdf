context("Xdf file utilities")

mtx <- rxDataStep(mtcars, "mtx.xdf", overwrite=TRUE)
mtc <- RxXdfData("mtc", createCompositeSet=TRUE)
rxDataStep(mtcars, mtc, overwrite=TRUE)

verifyData <- function(xdf, expectedClass)
{
    is.data.frame(head(xdf)) && class(xdf) == expectedClass # test for exact class
}

verifyCompositeData <- function(xdf, expectedClass)
{
    isTRUE(xdf@createCompositeSet) && is.data.frame(head(xdf)) && class(xdf) == expectedClass # test for exact class
}

.path <- function(path)
{
    normalizePath(path, mustWork=FALSE)
}

test2 <- tempfile()
dir.create(test2)

test_that("rename works",
{
    tbl <- rename_xdf(mtx, "mtx2.xdf")
    expect_true(verifyData(tbl, "RxXdfData"))
    expect_false(file.exists(mtx@file))

    rename_xdf(tbl, "mtx.xdf")
    expect_true(verifyData(mtx, "RxXdfData"))
    expect_false(file.exists(tbl@file))

    expect_error(rename(mtx, file.path(test2, "foo.xdf")))
})

test_that("rename works for composite",
{
    tbl <- rename_xdf(mtc, "mtc2")
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
    expect_false(dir.exists(mtc@file))

    rename_xdf(tbl, "mtc")
    expect_true(verifyCompositeData(mtc, "RxXdfData"))
    expect_false(file.exists(tbl@file))

    expect_error(rename(mtc, file.path(test2, "foo")))
})

test_that("copy and move work",
{
    # copy to same dir = working dir
    tbl <- copy_xdf(mtx, "test16.xdf")
    expect_true(verifyData(tbl, "RxXdfData"))
    expect_identical(.path(tbl@file), .path(file.path(getwd(), "test16.xdf")))

    # move to same dir = working dir (rename)
    tbl2 <- move_xdf(tbl, "test16a.xdf")
    expect_true(verifyData(tbl2, "RxXdfData"))
    expect_false(file.exists(tbl@file))
    expect_identical(.path(tbl2@file), .path("test16a.xdf"))

    # copy to different dir
    tbl <- copy_xdf(mtx, test2)
    expect_true(verifyData(tbl, "RxXdfData"))
    expect_identical(.path(tbl@file), .path(file.path(test2, "mtx.xdf")))

    # move to different dir
    tbl2 <- move_xdf(tbl2, test2)
    expect_true(verifyData(tbl2, "RxXdfData"))
    expect_identical(.path(tbl2@file), .path(file.path(test2, "test16a.xdf")))

    # copy to same explicit dir
    dest <- .path(file.path(getwd(), "test16.xdf"))
    unlink(dest)
    tbl <- copy_xdf(mtx, dest)
    expect_true(verifyData(tbl, "RxXdfData"))
    expect_identical(.path(tbl@file), dest)

    # move to same explicit dir
    dest2 <- .path(file.path(getwd(), "test16a.xdf"))
    unlink(dest2)
    tbl2 <- move_xdf(tbl, dest2)
    expect_true(verifyData(tbl2, "RxXdfData"))
    expect_false(file.exists(tbl@file))
    expect_identical(.path(tbl2@file), dest2)

    # copy to different dir + rename
    dest <- .path(file.path(test2, "test16.xdf"))
    unlink(dest)
    tbl <- copy_xdf(mtx, dest)
    expect_true(verifyData(tbl, "RxXdfData"))
    expect_identical(.path(tbl@file), dest)

    # move to different dir + rename
    dest2 <- .path(file.path(test2, "test16a.xdf"))
    unlink(dest2)
    tbl2 <- move_xdf(mtx, dest2)
    expect_true(verifyData(tbl2, "RxXdfData"))
    expect_false(file.exists(mtx@file))
    expect_identical(.path(tbl2@file), dest2)

    # recreate original file
    mtx <- rxDataStep(mtcars, "mtx.xdf", overwrite=TRUE)
})

test_that("copy and move work for composite",
{
    # copy to same dir = working dir
    tbl <- copy_xdf(mtc, "test16")
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
    expect_identical(.path(tbl@file), .path(file.path(getwd(), "test16")))

    # move to same dir = working dir (rename)
    tbl2 <- move_xdf(tbl, "test16a")
    expect_true(verifyCompositeData(tbl2, "RxXdfData"))
    expect_false(file.exists(tbl@file))
    expect_identical(.path(tbl2@file), .path("test16a"))

    # copy to different dir
    tbl <- copy_xdf(mtc, test2)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
    expect_identical(.path(tbl@file), .path(file.path(test2, "mtc")))

    # move to different dir
    tbl2 <- move_xdf(tbl2, test2)
    expect_true(verifyCompositeData(tbl2, "RxXdfData"))
    expect_identical(.path(tbl2@file), .path(file.path(test2, "test16a")))

    # copy to same explicit dir
    dest <- .path(file.path(getwd(), "test16"))
    unlink(dest, recursive=TRUE)
    tbl <- copy_xdf(mtc, dest)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
    expect_identical(.path(tbl@file), dest)

    # move to same explicit dir
    dest2 <- .path(file.path(getwd(), "test16a"))
    unlink(dest2, recursive=TRUE)
    tbl2 <- move_xdf(tbl, dest2)
    expect_true(verifyCompositeData(tbl2, "RxXdfData"))
    expect_false(file.exists(tbl@file))
    expect_identical(.path(tbl2@file), dest2)

    # copy to different dir + rename
    dest <- .path(file.path(test2, "test16"))
    unlink(dest, recursive=TRUE)
    tbl <- copy_xdf(mtc, dest)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
    expect_identical(.path(tbl@file), dest)

    # move to different dir + rename
    dest2 <- .path(file.path(test2, "test16a"))
    unlink(dest2, recursive=TRUE)
    tbl2 <- move_xdf(mtc, dest2)
    expect_true(verifyCompositeData(tbl2, "RxXdfData"))
    expect_false(file.exists(mtc@file))
    expect_identical(.path(tbl2@file), dest2)

    # recreate original file
    rxDataStep(mtcars, mtc, overwrite=TRUE)
})

test_that("persist works",
{
    expect_warning(persist(mtx, "test16.xdf"))
    tbl <- as(mtx, "tbl_xdf") %>% persist("test16.xdf", move=FALSE)
    expect_true(verifyData(tbl, "RxXdfData"))
    tbl2 <- as(tbl, "tbl_xdf") %>% persist("test16a.xdf", move=TRUE)
    expect_true(verifyData(tbl2, "RxXdfData"))
    expect_false(file.exists(tbl@file))
})

test_that("persist works for composite",
{
    expect_warning(persist(mtc, "test16"))
    tbl <- as(mtc, "tbl_xdf") %>% persist("test16", move=FALSE)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
    tbl2 <- as(tbl, "tbl_xdf") %>% persist("test16a", move=TRUE)
    expect_true(verifyCompositeData(tbl2, "RxXdfData"))
    expect_false(file.exists(tbl@file))

    tbl <- as(mtx, "tbl_xdf") %>% persist("test16", composite=TRUE, move=FALSE)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
    tbl <- as(mtc, "tbl_xdf") %>% persist("test16.xdf", composite=FALSE, move=FALSE)
    expect_true(verifyData(tbl, "RxXdfData"))
})

test_that("collect and compute work",
{
    tbl <- collect(mtc)
    expect_true(is.data.frame(tbl))
    tbl <- compute(mtc)
    expect_is(tbl, "data.frame")
    tbl <- compute(mtc, as_data_frame=FALSE)
    expect_true(verifyData(tbl, "RxXdfData"))
})


unlink(c("mtx.xdf", "mtc", "test16.xdf", "test16a.xdf", "test16", "test16a"), recursive=TRUE)
