context("Xdf file utilities in HDFS")

# set the compute context manually

detectHdfsConnection()

mthc <- RxXdfData("/user/sshuser/mtcarsc", fileSystem=hd, createCompositeSet=TRUE)
mtc <- RxXdfData("mtcarsc", fileSystem=RxNativeFileSystem(), createCompositeSet=TRUE)


verifyHdfsData <- function(xdf, expectedClass)
{
    isTRUE(xdf@createCompositeSet) && rxHadoopFileExists(xdf@file) && class(xdf) == expectedClass # test for exact class
}

# check actual data -- slow but necessary to check that non-Revo file ops succeeded
verifyCompositeData <- function(xdf, expectedClass)
{
    isTRUE(xdf@createCompositeSet) && is.data.frame(as.data.frame(xdf)) && class(xdf) == expectedClass # test for exact class
}

.path <- function(path)
{
    normalizeHdfsPath(path)
}

test2 <- tempfile(tmpdir="/tmp")
hdfs_dir_create(test2)


test_that("local_exec works",
{
    local_exec(rxDataStep(mtcars, mtc, overwrite=TRUE))
    local_exec(verifyCompositeData(mtc, "RxXdfData"))
})

test_that("copy_to works",
{
    if(hdfs_dir_exists("/user/sshuser/mtcarsc"))
        rxHadoopRemoveDir("/user/sshuser/mtcarsc", skipTrash=TRUE)
    copy_to(RxHdfsFileSystem(), mtc, overwrite=TRUE)
    verifyCompositeData(mthc, "RxXdfData")
})

test_that("rename works",
{
    tbl <- rename_xdf(mthc, "mthc2")
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
    expect_false(hdfs_dir_exists(mthc@file))

    rename_xdf(tbl, "mtcarsc")
    expect_true(verifyCompositeData(mthc, "RxXdfData"))
    expect_false(hdfs_dir_exists(tbl@file))

    expect_error(rename(mthc, file.path(test2, "foo")))
})

test_that("copy and move work",
{
    # copy to same dir = working dir
    tbl <- copy_xdf(mthc, "test52")
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
    expect_identical(.path(tbl@file), .path("test52"))

    # move to same dir = working dir (rename)
    tbl2 <- move_xdf(tbl, "test52a")
    expect_true(verifyCompositeData(tbl2, "RxXdfData"))
    expect_false(hdfs_dir_exists(tbl@file))
    expect_identical(.path(tbl2@file), .path("test52a"))

    # copy to different dir
    tbl <- copy_xdf(mthc, test2)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
    expect_identical(.path(tbl@file), .path(file.path(test2, "mtcarsc")))

    # move to different dir
    tbl2 <- move_xdf(tbl2, test2)
    expect_true(verifyCompositeData(tbl2, "RxXdfData"))
    expect_identical(.path(tbl2@file), .path(file.path(test2, "test52a")))

    # copy to same explicit dir
    dest <- .path("test52")
    if(hdfs_dir_exists(dest))
        hdfs_dir_remove(dest)
    tbl <- copy_xdf(mthc, dest)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
    expect_identical(.path(tbl@file), dest)

    # move to same explicit dir
    dest2 <- .path("test52a")
    if(hdfs_dir_exists(dest2))
        hdfs_dir_remove(dest2)
    tbl2 <- move_xdf(tbl, dest2)
    expect_true(verifyCompositeData(tbl2, "RxXdfData"))
    expect_false(hdfs_dir_exists(tbl@file))
    expect_identical(.path(tbl2@file), dest2)

    # copy to different dir + rename
    dest <- .path(file.path(test2, "test52"))
    if(hdfs_dir_exists(dest))
        hdfs_dir_remove(dest)
    tbl <- copy_xdf(mthc, dest)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
    expect_identical(.path(tbl@file), dest)

    # move to different dir + rename
    dest2 <- .path(file.path(test2, "test52a"))
    if(hdfs_dir_exists(dest2))
        hdfs_dir_remove(dest2)
    tbl2 <- move_xdf(mthc, dest2)
    expect_true(verifyCompositeData(tbl2, "RxXdfData"))
    expect_false(hdfs_dir_exists(mthc@file))
    expect_identical(.path(tbl2@file), dest2)

    # recreate original file
    copy_to(RxHdfsFileSystem(), mtc, overwrite=TRUE)
})

test_that("persist works",
{
    expect_warning(persist(mthc, "test52"))
    tbl <- as(mthc, "tbl_xdf") %>% persist("test52", move=FALSE)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
    tbl2 <- as(tbl, "tbl_xdf") %>% persist("test52a", move=TRUE)
    expect_true(verifyCompositeData(tbl2, "RxXdfData"))
    expect_false(hdfs_dir_exists(tbl@file))

    expect_warning(tbl <- as(mthc, "tbl_xdf") %>% persist("test52.xdf", composite=FALSE, move=FALSE))
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
})

test_that("collect and compute work",
{
    tbl <- collect(mthc)
    expect_true(is.data.frame(tbl))
    tbl <- compute(mthc)
    expect_true(local_exec(verifyCompositeData(tbl, "tbl_xdf")))
    tbl <- compute(mthc, as_data_frame=TRUE)
    expect_true(is.data.frame(tbl))
})


lapply(c("test52", "test52a", test2), hdfs_dir_remove, skipTrash=TRUE)
unlink("mtcarsc", recursive=TRUE)

