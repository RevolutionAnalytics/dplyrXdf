context("Coerce to Xdf, HDFS")

# set the compute context manually

detectHdfsConnection()

mthc <- RxXdfData("mtcars", fileSystem=RxHdfsFileSystem(), createCompositeSet=TRUE)
mtt <- RxTextData("mttext.csv", fileSystem=RxHdfsFileSystem())

write.csv(mtcars, "mttext.csv", row.names=FALSE)
hdfs_upload("mttext.csv", ".", overwrite=TRUE)


# check actual data -- slow but necessary to check that non-Revo file ops succeeded
verifyCompositeData <- function(xdf, expectedClass)
{
    isTRUE(xdf@createCompositeSet) && is.data.frame(as.data.frame(xdf)) && class(xdf) == expectedClass # test for exact class
}


test_that("copy_to works",
{
    if(hdfs_dir_exists("mtcars"))
        hdfs_dir_remove("mtcars", skipTrash=TRUE)
    copy_to_hdfs(mtcars)
    verifyCompositeData(mthc, "RxXdfData")
})

test_that("as_xdf works",
{
    tbl <- as_xdf(mthc, overwrite=TRUE)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
    expect_identical(mthc@file, tbl@file)

    tbl <- as_xdf(mthc, file="test54", composite=TRUE, overwrite=TRUE)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))

    expect_error(tbl <- as_xdf(mthc, composite=FALSE, overwrite=TRUE))
})

test_that("as_standard_xdf works",
{
    expect_error(tbl <- as_standard_xdf(mthc, overwrite=TRUE))
})

test_that("as_composite_xdf works",
{
    tbl <- as_composite_xdf(mthc, overwrite=TRUE)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))

    tbl <- as_composite_xdf(mthc, file="test54", overwrite=TRUE)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
})


test_that("as_xdf works, tbl input",
{
    tbl0 <- as(mthc, "tbl_xdf")
    tbl <- as_xdf(tbl0, overwrite=TRUE)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
    expect_identical(tbl0@file, tbl@file)

    tbl <- as_xdf(tbl0, "test54", overwrite=TRUE)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))

    expect_error(tbl <- as_xdf(tbl0, "test54.xdf", composite=FALSE, overwrite=TRUE))
})

test_that("as_standard_xdf works, tbl input",
{
    tbl0 <- as(mthc, "tbl_xdf")
    expect_error(tbl <- as_standard_xdf(tbl0, overwrite=TRUE))
})

test_that("as_composite_xdf works, tbl input",
{
    tbl0 <- as(mthc, "tbl_xdf")
    tbl <- as_composite_xdf(tbl0, overwrite=TRUE)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))

    tbl <- as_composite_xdf(tbl0, "test54", overwrite=TRUE)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
})

test_that("as_xdf works, text input",
{
    tbl <- as_xdf(mtt, overwrite=TRUE)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))

    tbl <- as_xdf(mtt, "test54", composite=TRUE, overwrite=TRUE)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))

    expect_error(tbl <- as_xdf(mtt, "test54.xdf", composite=FALSE, overwrite=TRUE))
})

test_that("as_standard_xdf works, text input",
{
    expect_error(tbl <- as_standard_xdf(mtt, overwrite=TRUE))
})

test_that("as_composite_xdf works, text input",
{
    tbl <- as_composite_xdf(mtt, overwrite=TRUE)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
})


hdfs_dir_remove(c("mtcars", "mttext", "test54"), skipTrash=TRUE)
hdfs_file_remove("mttext.csv", skipTrash=TRUE)

unlink(c("mttext.csv", "test54"), recursive=TRUE)


