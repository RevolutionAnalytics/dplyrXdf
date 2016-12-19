context("Text input")
write.csv(mtcars, "mtcars.csv", row.names=FALSE)
mtt <- RxTextData("mtcars.csv")

verifyData <- function(xdf, expectedClass)
{
    is.data.frame(head(xdf)) && class(xdf) == expectedClass  # test for exact class
}


test_that("arrange works", {
    tbl <- mtt %>% arrange(mpg, disp)
    expect_true(verifyData(tbl, "tbl_xdf"))
})

test_that("distinct works", {
    tbl <- mtt %>% distinct(cyl, gear)
    expect_true(verifyData(tbl, "tbl_xdf"))

    # .keep_all will have no effect when dplyr < 0.5 installed
    skip_if_not(packageVersion("dplyr") >= package_version("0.5"))
    tbl <- mtt %>% distinct(cyl, gear, .keep_all=TRUE)
    expect_true(verifyData(tbl, "tbl_xdf"))
    tbl <- mtt %>% distinct(cyl, gear, .keep_all=FALSE)
    expect_true(verifyData(tbl, "tbl_xdf"))
})

test_that("do works", {
    expect_true(verifyData(mtt %>% do(m=lm(mpg ~ disp, .)), "tbl_df"))
    expect_true(verifyData(mtt %>% doXdf(m=rxLinMod(mpg ~ disp, .)), "tbl_df"))
})

test_that("factorise works", {
    tbl <- mtt %>% factorise(mpg, cyl)
    expect_true(verifyData(tbl, "tbl_xdf"))
    expect(all(sapply(rxGetVarInfo(tbl)[c("mpg", "cyl")], "[[", "varType") == "factor"),
           "factor conversion failed")
})

test_that("filter works", {
    tbl <- mtt %>% filter(mpg > 16, cyl == 8)
    expect_true(verifyData(tbl, "tbl_xdf"))
})

test_that("mutate works", {
    tbl <- mtt %>% mutate(m2=2*mpg, sw=sqrt(wt))
    expect_true(verifyData(tbl, "tbl_xdf"))
})

test_that("rename works", {
    tbl <- mtt %>% rename(cc=cyl, mm=mpg)
    expect_true(verifyData(tbl, "tbl_xdf"))
    expect_true(all(c("cc", "mm") %in% names(tbl)))
})

test_that("select works", {
    tbl <- mtt %>% select(mpg, cyl, drat)
    expect_true(verifyData(tbl, "tbl_xdf"))
})

test_that("subset works", {
    tbl <- mtt %>% subset(cyl==6, c(2,3,4))
    expect_true(verifyData(tbl, "tbl_xdf"))
})

test_that("summarise works", {
    expect_warning(tbl <- mtt %>% summarise(m=mean(mpg), .method=1))
    expect_true(verifyData(tbl, "tbl_xdf"))
    tbl <- mtt %>% summarise(m=mean(mpg), .method=2)
    expect_true(verifyData(tbl, "tbl_xdf"))
    expect_warning(tbl <- mtt %>% summarise(m=mean(mpg), .method=3))
    expect_true(verifyData(tbl, "tbl_xdf"))
    tbl <- mtt %>% summarise(m=mean(mpg), .method=4)
    expect_true(verifyData(tbl, "tbl_xdf"))
    tbl <- mtt %>% summarise(m=mean(mpg), .method=5)
    expect_true(verifyData(tbl, "tbl_xdf"))
})

test_that("transmute works", {
    tbl <- mtt %>% transmute(m2=2*mpg, sw=sqrt(wt))
    expect_true(verifyData(tbl, "tbl_xdf"))
})

# cleanup
file.remove("mtcars.csv")
