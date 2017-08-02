context("Coerce to Xdf, HDFS")

# set the compute context manually

skip_if_not(!is.na(isRemoteHdfsClient(FALSE)), message="not in distributed compute context")

mthc <- RxXdfData("/user/sshuser/mtcarsc", fileSystem=hd, createCompositeSet=TRUE)
mtt <- RxTextData("/user/sshuser/mttext.csv", fileSystem=hd)
mtc <- RxXdfData("mtcarsc", createCompositeSet=TRUE)

write.csv(mtcars, "mttext.csv", row.names=FALSE)
if(isRemoteHdfsClient()) rxHadoopCopyFromClient("mttext.csv", hdfsDest=".") else rxHadoopCopyFromLocal("mttext.csv", ".")


# check actual data -- slow but necessary to check that non-Revo file ops succeeded
verifyCompositeData <- function(xdf, expectedClass)
{
    isTRUE(xdf@createCompositeSet) && is.data.frame(head(xdf)) && class(xdf) == expectedClass # test for exact class
}


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

test_that("as_xdf works",
{
    tbl <- as_xdf(mthc)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
    expect_identical(mthc@file, tbl@file)

    tbl <- as_xdf(mthc, file="test53", composite=TRUE)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))

    expect_error(tbl <- as_xdf(mthc, composite=FALSE))
})

test_that("as_standard_xdf works",
{
    expect_error(tbl <- as_standard_xdf(mthc))
})

test_that("as_composite_xdf works",
{
    tbl <- as_composite_xdf(mthc)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))

    tbl <- as_composite_xdf(mthc, file="test53")
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
})


test_that("as_xdf works, tbl input",
{
    tbl0 <- mutate(mthc)
    tbl <- as_xdf(tbl0)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
    expect_identical(tbl0@file, tbl@file)

    tbl <- as_xdf(tbl0, "test53")
    expect_true(verifyCompositeData(tbl, "RxXdfData"))

    expect_error(tbl <- as_xdf(tbl0, "test53.xdf", composite=FALSE))
})

test_that("as_standard_xdf works, tbl input",
{
    tbl0 <- mutate(mthc)
    expect_error(tbl <- as_standard_xdf(tbl0))
})

test_that("as_composite_xdf works, tbl input",
{
    tbl0 <- mutate(mthc)
    tbl <- as_composite_xdf(tbl0)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))

    tbl <- as_composite_xdf(tbl0, "test53")
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
})

test_that("as_xdf works, text input",
{
    tbl <- as_xdf(mtt)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))

    tbl <- as_xdf(mtt, "test53", composite=TRUE)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))

    expect_error(tbl <- as_xdf(mtt, "test53.xdf", composite=FALSE))
})

test_that("as_standard_xdf works, text input",
{
    expect_error(tbl <- as_standard_xdf(mtt))
})

test_that("as_composite_xdf works, text input",
{
    tbl <- as_composite_xdf(mtt)
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
})


testFiles <- hdfs_dir(pattern="^file")
lapply(c("mtcarsc", "mttext", "test53"), hdfs_dir_remove, skipTrash=TRUE)
hdfs_file_remove("mttext.csv", skipTrash=TRUE)

unlink(c("mtcarsc", "mttext.csv"), recursive=TRUE)


