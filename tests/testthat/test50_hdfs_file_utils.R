context("Basic file utilities in HDFS")

# set the compute context manually

detectHdfsConnection()


test_that("local_exec works",
{
    mtc <- RxXdfData("mtcarsc", fileSystem=RxNativeFileSystem(), createCompositeSet=TRUE)
    local_exec(mtc <- rxDataStep(mtcars, mtc, overwrite=TRUE))
    write.csv(mtcars, "mtcars.csv", row.names=FALSE)
    expect_true(dir.exists("mtcarsc"))
    expect_true(file.exists("mtcars.csv"))
})

test_that("file upload works",
{
    if(hdfs_dir_exists("mtcarsc"))
        hdfs_dir_remove("mtcarsc")

    expect_false(hdfs_dir_exists("mtcarsc"))
    expect_true(hdfs_upload("mtcarsc", ".", overwrite=TRUE))
    expect_true(hdfs_dir_exists("mtcarsc"))
    expect_error(hdfs_upload("mtcarsc", ".", overwrite=FALSE))
    expect_true(hdfs_upload("mtcarsc", ".", overwrite=TRUE))
    expect_true(hdfs_dir_exists("mtcarsc"))

    if(hdfs_file_exists("mtcars.csv"))
        hdfs_file_remove("mtcars.csv")

    expect_false(hdfs_file_exists("mtcars.csv"))
    expect_true(hdfs_upload("mtcars.csv", ".", overwrite=TRUE))
    expect_true(hdfs_file_exists("mtcars.csv"))
    expect_error(hdfs_upload("mtcars.csv", ".", overwrite=FALSE))
    expect_true(hdfs_upload("mtcars.csv", ".", overwrite=TRUE))
    expect_true(hdfs_file_exists("mtcars.csv"))
})

test_that("file download works",
{
    file.remove("mtcars.csv")
    unlink("mtcarsc", recursive=TRUE)

    expect_true(hdfs_download("mtcarsc", ".", overwrite=TRUE))
    expect_true(dir.exists("mtcarsc"))
    expect_error(hdfs_download("mtcarsc", ".", overwrite=FALSE))
    expect_true(hdfs_download("mtcarsc", ".", overwrite=TRUE))
    expect_true(dir.exists("mtcarsc"))

    expect_true(hdfs_download("mtcars.csv", ".", overwrite=TRUE))
    expect_true(file.exists("mtcars.csv"))
    expect_error(hdfs_download("mtcars.csv", ".", overwrite=FALSE))
    expect_true(hdfs_download("mtcars.csv", ".", overwrite=TRUE))
    expect_true(file.exists("mtcars.csv"))
})

test_that("dir create works",
{
    if(hdfs_dir_exists("temp"))
        hdfs_dir_remove("temp")

    expect_true(hdfs_dir_create("temp"))
})

test_that("file list/move/copy/rename works",
{
    expect_true(hdfs_file_copy("mtcarsc", "temp"))
    expect_true(hdfs_dir_exists("temp/mtcarsc"))

    expect_true(hdfs_file_copy("mtcarsc", "temp/mtcarsc2"))
    expect_true(hdfs_dir_exists("temp/mtcarsc2"))

    expect_true(hdfs_file_move("temp/mtcarsc2", "temp/mtcarsc3"))
    expect_true(hdfs_dir_exists("temp/mtcarsc3"))

    expect_true(hdfs_file_copy("mtcars.csv", "temp"))
    expect_true(hdfs_file_exists("temp/mtcars.csv"))

    expect_true(hdfs_file_copy("temp/mtcars.csv", "temp/mtcars2.csv"))
    expect_true(hdfs_file_exists("temp/mtcars2.csv"))

    expect_true(hdfs_file_move("temp/mtcars2.csv", "temp/mtcars3.csv"))
    expect_true(hdfs_file_exists("temp/mtcars3.csv"))
})

test_that("file/dir remove works",
{
    expect_true(hdfs_file_remove("temp/mtcars3.csv"))
    expect_true(hdfs_dir_remove("temp"))
    expect_true(hdfs_file_remove("mtcars.csv"))
    expect_true(hdfs_dir_remove("mtcarsc"))
})

unlink(c("mtcars.csv", "mtcarsc"), recursive=TRUE)




