context("Summarise checks")
mtx <- rxDataStep(mtcars, "mtx.xdf", overwrite=TRUE)


test_that("grouping vars treated properly", {
    smrydf <- mtx %>% mutate(g4=gear == 4) %>% group_by(g4) %>% summarise(m=mean(mpg), .outFile=NULL)
    expect(!any(is.na(smrydf$g4)), "logical grouping var error")

    expect_error(mtx %>% group_by(wt) %>% summarise(m=mean(mpg)))
    expect_s4_class(mtx %>% factorise(wt) %>% group_by(wt) %>% summarise(m=mean(mpg)), "tbl_xdf")
})


test_that("input tbls removed", {
    tblDir <- getXdfTblDir()

    deleteXdfTbls()
    tbl1 <- mtx %>% mutate(w2=wt*2)
    expect_warning(tbl2 <- tbl1 %>% summarise(m=mean(w2), .method=1))
    expect(length(dir(tblDir, "\\.xdf$")) == 1, "input tbls not deleted for ungrouped .method=1")
    expect_s3_class(as.data.frame(tbl2), "data.frame")

    deleteXdfTbls()
    tbl1 <- mtx %>% mutate(w2=wt*2)
    tbl2 <- tbl1 %>% group_by(gear) %>% summarise(m=mean(w2), .method=1)
    expect(length(dir(tblDir, "\\.xdf$")) == 1, "input tbls not deleted for .method=1")
    expect_s3_class(as.data.frame(tbl2), "data.frame")

    deleteXdfTbls()
    tbl1 <- mtx %>% mutate(w2=wt*2)
    tbl2 <- tbl1 %>% summarise(m=mean(w2), .method=2)
    expect(length(dir(tblDir, "\\.xdf$")) == 1, "input tbls not deleted for ungrouped .method=2")
    expect_s3_class(as.data.frame(tbl2), "data.frame")

    deleteXdfTbls()
    tbl1 <- mtx %>% mutate(w2=wt*2)
    tbl2 <- tbl1 %>% group_by(gear) %>% summarise(m=mean(w2), .method=2)
    expect(length(dir(tblDir, "\\.xdf$")) == 1, "input tbls not deleted for .method=2")
    expect_s3_class(as.data.frame(tbl2), "data.frame")

    deleteXdfTbls()
    tbl1 <- mtx %>% mutate(w2=wt*2)
    expect_warning(tbl2 <- tbl1 %>% summarise(m=mean(w2), .method=3))
    expect(length(dir(tblDir, "\\.xdf$")) == 1, "input tbls not deleted for ungrouped .method=3")
    expect_s3_class(as.data.frame(tbl2), "data.frame")

    deleteXdfTbls()
    tbl1 <- mtx %>% mutate(w2=wt*2)
    tbl2 <- tbl1 %>% group_by(gear) %>% summarise(m=mean(w2), .method=3)
    expect(length(dir(tblDir, "\\.xdf$")) == 1, "input tbls not deleted for .method=3")
    expect_s3_class(as.data.frame(tbl2), "data.frame")

    deleteXdfTbls()
    tbl1 <- mtx %>% mutate(w2=wt*2)
    tbl2 <- tbl1 %>% summarise(m=mean(w2), .method=4)
    expect(length(dir(tblDir, "\\.xdf$")) == 1, "input tbls not deleted for ungrouped .method=4")
    expect_s3_class(as.data.frame(tbl2), "data.frame")

    deleteXdfTbls()
    tbl1 <- mtx %>% mutate(w2=wt*2)
    tbl2 <- tbl1 %>% group_by(gear) %>% summarise(m=mean(w2), .method=4)
    expect(length(dir(tblDir, "\\.xdf$")) == 1, "input tbls not deleted for .method=4")
    expect_s3_class(as.data.frame(tbl2), "data.frame")

    deleteXdfTbls()
    tbl1 <- mtx %>% mutate(w2=wt*2)
    tbl2 <- tbl1 %>% summarise(m=mean(w2), .method=5)
    expect(length(dir(tblDir, "\\.xdf$")) == 1, "input tbls not deleted for ungrouped .method=5")
    expect_s3_class(as.data.frame(tbl2), "data.frame")

    deleteXdfTbls()
    tbl1 <- mtx %>% mutate(w2=wt*2)
    tbl2 <- tbl1 %>% group_by(gear) %>% summarise(m=mean(w2), .method=5)
    expect(length(dir(tblDir, "\\.xdf$")) == 1, "input tbls not deleted for .method=5")
    expect_s3_class(as.data.frame(tbl2), "data.frame")
})


# cleanup
file.remove("mtx.xdf")
deleteXdfTbls()

