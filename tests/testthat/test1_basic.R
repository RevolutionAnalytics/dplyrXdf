context("Basic functionality")
mtx <- rxDataStep(mtcars, "mtx.xdf", overwrite=TRUE)

setXdfTblDir(".")

verifyData <- function(xdf, expectedClass)
{
    is.data.frame(head(xdf)) && class(xdf) == expectedClass  # test for exact class
}


test_that("arrange works", {
    tbl <- mtx %>% arrange(mpg, disp)
    expect_true(verifyData(tbl, "tbl_xdf"))
})

test_that("distinct works", {
    tbl <- mtx %>% distinct(cyl, gear)
    expect_true(verifyData(tbl, "tbl_xdf"))

    # .keep_all will have no effect when dplyr < 0.5 installed
    skip_if_not(packageVersion("dplyr") >= package_version("0.5"))
    tbl <- mtx %>% distinct(cyl, gear, .keep_all=TRUE)
    expect_true(verifyData(tbl, "tbl_xdf"))
    tbl <- mtx %>% distinct(cyl, gear, .keep_all=FALSE)
    expect_true(verifyData(tbl, "tbl_xdf"))
})

test_that("do works", {
    expect_true(verifyData(mtx %>% do(m=lm(mpg ~ disp, .)), "tbl_df"))
    expect_true(verifyData(mtx %>% doXdf(m=rxLinMod(mpg ~ disp, .)), "tbl_df"))
})

test_that("factorise works", {
    tbl <- mtx %>% factorise(mpg, cyl)
    expect_true(verifyData(tbl, "tbl_xdf"))
    expect(all(sapply(rxGetVarInfo(tbl)[c("mpg", "cyl")], "[[", "varType") == "factor"),
           "factor conversion failed")
})

test_that("filter works", {
    tbl <- mtx %>% filter(mpg > 16, cyl == 8)
    expect_true(verifyData(tbl, "tbl_xdf"))
})

test_that("mutate works", {
    tbl <- mtx %>% mutate(m2=2*mpg, sw=sqrt(wt))
    expect_true(verifyData(tbl, "tbl_xdf"))
})

test_that("rename works", {
    tbl <- mtx %>% rename(cc=cyl, mm=mpg)
    expect_true(verifyData(tbl, "tbl_xdf"))
    expect_true(all(c("cc", "mm") %in% names(tbl)))
})

test_that("select works", {
    tbl <- mtx %>% select(mpg, cyl, drat)
    expect_true(verifyData(tbl, "tbl_xdf"))
})

test_that("subset works", {
    tbl <- mtx %>% subset(cyl==6, c(2,3,4))
    expect_true(verifyData(tbl, "tbl_xdf"))
})

test_that("summarise works", {
    expect_warning(tbl <- mtx %>% summarise(m=mean(mpg), .method=1))
    expect_true(verifyData(tbl, "tbl_xdf"))
    tbl <- mtx %>% summarise(m=mean(mpg), .method=2)
    expect_true(verifyData(tbl, "tbl_xdf"))
    expect_warning(tbl <- mtx %>% summarise(m=mean(mpg), .method=3))
    expect_true(verifyData(tbl, "tbl_xdf"))
    tbl <- mtx %>% summarise(m=mean(mpg), .method=4)
    expect_true(verifyData(tbl, "tbl_xdf"))
    tbl <- mtx %>% summarise(m=mean(mpg), .method=5)
    expect_true(verifyData(tbl, "tbl_xdf"))
})

test_that("transmute works", {
    tbl <- mtx %>% transmute(m2=2*mpg, sw=sqrt(wt))
    expect_true(verifyData(tbl, "tbl_xdf"))
})

test_that("setting tbl dir works", {
    expect(getXdfTblDir() != tempdir(), "changing tbl dir failed")
    expect_gt(length(dir(getXdfTblDir())), 0, "no files in tbl dir")
})


# cleanup
file.remove("mtx.xdf")
