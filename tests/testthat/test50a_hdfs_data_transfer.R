context("Data transfer for HDFS")

# set the compute context manually

detectHdfsConnection()


mtx <- RxXdfData("mtcars.xdf", fileSystem=RxNativeFileSystem(), createCompositeSet=FALSE)
mtc <- RxXdfData("mtcars", fileSystem=RxNativeFileSystem(), createCompositeSet=TRUE)
mtt <- RxTextData("mtcars.csv", fileSystem=RxNativeFileSystem())

# check actual data -- slow but necessary to check that non-Revo file ops succeeded
verifyCompositeData <- function(xdf, expectedClass)
{
    isTRUE(xdf@createCompositeSet) && is.data.frame(as.data.frame(xdf)) && class(xdf) == expectedClass # test for exact class
}


test_that("local_exec works",
{
    local_exec(
    {
        rxDataStep(mtcars, mtx, overwrite=TRUE)
        rxDataStep(mtcars, mtc, overwrite=TRUE)
        rxDataStep(mtcars, mtt, overwrite=TRUE)
    })
    expect_true(file.exists("mtcars.xdf"))
    expect_true(dir.exists("mtcars"))
    expect_true(file.exists("mtcars.csv"))
})

test_that("copy_to works, data frame input",
{
    if(hdfs_dir_exists("mtcars"))
        hdfs_dir_remove("mtcars")
    if(hdfs_dir_exists("mtc"))
        hdfs_dir_remove("mtc")

    out <- copy_to_hdfs(mtcars)
    expect_true(hdfs_dir_exists("mtcars"))
    expect_true(verifyCompositeData(out, "RxXdfData"))
    hdfs_dir_remove("mtcars")

    out <- copy_to_hdfs(mtcars, "mtc")
    expect_true(hdfs_dir_exists("mtc"))
    expect_true(verifyCompositeData(out, "RxXdfData"))
    hdfs_dir_remove("mtc")

    if(hdfs_dir_exists("testdir"))
        hdfs_dir_remove("testdir")
    hdfs_dir_create("testdir")

    out <- copy_to_hdfs(mtcars, "testdir")
    expect_true(hdfs_dir_exists("testdir/mtcars"))
    expect_true(verifyCompositeData(out, "RxXdfData"))
    hdfs_dir_remove("testdir/mtcars")

    out <- copy_to_hdfs(mtcars, "testdir/mtc")
    expect_true(hdfs_dir_exists("testdir/mtc"))
    expect_true(verifyCompositeData(out, "RxXdfData"))

    hdfs_dir_remove("testdir")
})

test_that("copy_to works, standard xdf input",
{
    if(hdfs_dir_exists("mtcars"))
        hdfs_dir_remove("mtcars")
    if(hdfs_dir_exists("mtc"))
        hdfs_dir_remove("mtc")

    out <- copy_to_hdfs(mtx)
    expect_true(hdfs_dir_exists("mtcars"))
    expect_true(verifyCompositeData(out, "RxXdfData"))
    hdfs_dir_remove("mtcars")

    out <- copy_to_hdfs(mtx, "mtc")
    expect_true(hdfs_dir_exists("mtc"))
    expect_true(verifyCompositeData(out, "RxXdfData"))
    hdfs_dir_remove("mtc")

    if(hdfs_dir_exists("testdir"))
        hdfs_dir_remove("testdir")
    hdfs_dir_create("testdir")

    out <- copy_to_hdfs(mtx, "testdir")
    expect_true(hdfs_dir_exists("testdir/mtcars"))
    expect_true(verifyCompositeData(out, "RxXdfData"))
    hdfs_dir_remove("testdir/mtcars")

    out <- copy_to_hdfs(mtx, "testdir/mtc")
    expect_true(hdfs_dir_exists("testdir/mtc"))
    expect_true(verifyCompositeData(out, "RxXdfData"))

    hdfs_dir_remove("testdir")
})

test_that("copy_to works, composite xdf input",
{
    if(hdfs_dir_exists("mtcars"))
        hdfs_dir_remove("mtcars")
    if(hdfs_dir_exists("mtc"))
        hdfs_dir_remove("mtc")

    out <- copy_to_hdfs(mtc)
    expect_true(hdfs_dir_exists("mtcars"))
    expect_true(verifyCompositeData(out, "RxXdfData"))
    hdfs_dir_remove("mtcars")

    out <- copy_to_hdfs(mtc, "mtc")
    expect_true(hdfs_dir_exists("mtc"))
    expect_true(verifyCompositeData(out, "RxXdfData"))
    hdfs_dir_remove("mtc")

    if(hdfs_dir_exists("testdir"))
        hdfs_dir_remove("testdir")
    hdfs_dir_create("testdir")

    out <- copy_to_hdfs(mtc, "testdir")
    expect_true(hdfs_dir_exists("testdir/mtcars"))
    expect_true(verifyCompositeData(out, "RxXdfData"))
    hdfs_dir_remove("testdir/mtcars")

    out <- copy_to_hdfs(mtc, "testdir/mtc")
    expect_true(hdfs_dir_exists("testdir/mtc"))
    expect_true(verifyCompositeData(out, "RxXdfData"))

    hdfs_dir_remove("testdir")
})

test_that("copy_to works, text input",
{
    if(hdfs_dir_exists("mtcars"))
        hdfs_dir_remove("mtcars")
    if(hdfs_dir_exists("mtc"))
        hdfs_dir_remove("mtc")

    out <- copy_to_hdfs(mtt)
    expect_true(hdfs_dir_exists("mtcars"))
    expect_true(verifyCompositeData(out, "RxXdfData"))
    hdfs_dir_remove("mtcars")

    out <- copy_to_hdfs(mtt, "mtc")
    expect_true(hdfs_dir_exists("mtc"))
    expect_true(verifyCompositeData(out, "RxXdfData"))
    hdfs_dir_remove("mtc")

    if(hdfs_dir_exists("testdir"))
        hdfs_dir_remove("testdir")
    hdfs_dir_create("testdir")

    out <- copy_to_hdfs(mtt, "testdir")
    expect_true(hdfs_dir_exists("testdir/mtcars"))
    expect_true(verifyCompositeData(out, "RxXdfData"))
    hdfs_dir_remove("testdir/mtcars")

    out <- copy_to_hdfs(mtt, "testdir/mtc")
    expect_true(hdfs_dir_exists("testdir/mtc"))
    expect_true(verifyCompositeData(out, "RxXdfData"))

    hdfs_dir_remove("testdir")
})

test_that("collect and compute work",
{
    mthc <- copy_to_hdfs(mtcars)
    tbl <- collect(mthc)
    expect_true(is.data.frame(tbl))
    tbl <- compute(mthc)
    expect_true(local_exec(verifyCompositeData(tbl, "tbl_xdf")))
    tbl <- compute(mthc, as_data_frame=TRUE)
    expect_true(is.data.frame(tbl))
})

unlink(c("mtcars.xdf", "mtcars", "mtcars.csv"), recursive=TRUE)
