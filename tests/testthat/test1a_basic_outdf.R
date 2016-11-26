context("Output to data frame")
mtx <- rxDataStep(mtcars, "mtx.xdf", overwrite=TRUE)

verifyData <- function(xdf, expectedClass)
{
    is.data.frame(head(xdf)) && class(xdf) == expectedClass  # test for exact class
}


test_that("arrange works", {
    tbl <- mtx %>% arrange(mpg, disp, .outFile=NULL)
    expect_true(verifyData(tbl, "data.frame"))
})

test_that("distinct works", {
    tbl <- mtx %>% distinct(cyl, gear, .outFile=NULL)
    expect_true(verifyData(tbl, "tbl_df"))  # distinct outputs a tibble

    # .keep_all will have no effect when dplyr < 0.5 installed
    skip_if_not(packageVersion("dplyr") >= package_version("0.5"))
    tbl <- mtx %>% distinct(cyl, gear, .keep_all=TRUE, .outFile=NULL)
    expect_true(verifyData(tbl, "data.frame"))
    tbl <- mtx %>% distinct(cyl, gear, .keep_all=FALSE, .outFile=NULL)
    expect_true(verifyData(tbl, "data.frame"))
})

test_that("do works", {
    expect_true(verifyData(mtx %>% do(m=lm(mpg ~ disp, .), .outFile=NULL), "tbl_df"))
    expect_true(verifyData(mtx %>% doXdf(m=rxLinMod(mpg ~ disp, .), .outFile=NULL), "tbl_df"))
})

test_that("factorise works", {
    tbl <- mtx %>% factorise(mpg, cyl, .outFile=NULL)
    expect_true(verifyData(tbl, "data.frame"))
    expect(all(sapply(rxGetVarInfo(tbl)[c("mpg", "cyl")], "[[", "varType") == "factor"),
           "factor conversion failed")
})

test_that("filter works", {
    tbl <- mtx %>% filter(mpg > 16, cyl == 8, .outFile=NULL)
    expect_true(verifyData(tbl, "data.frame"))
})

test_that("mutate works", {
    tbl <- mtx %>% mutate(m2=2*mpg, sw=sqrt(wt), .outFile=NULL)
    expect_true(verifyData(tbl, "data.frame"))
})

test_that("rename works", {
    tbl <- mtx %>% rename(cc=cyl, mm=mpg, .outFile=NULL)
    expect_true(verifyData(tbl, "data.frame"))
    expect_true(all(c("cc", "mm") %in% names(tbl)))
})

test_that("select works", {
    tbl <- mtx %>% select(mpg, cyl, drat, .outFile=NULL)
    expect_true(verifyData(tbl, "data.frame"))
})

test_that("subset works", {
    tbl <- mtx %>% subset(cyl==6, c(2,3,4), .outFile=NULL)
    expect_true(verifyData(tbl, "data.frame"))
})

test_that("summarise works", {
    expect_warning(tbl <- mtx %>% summarise(m=mean(mpg), .method=1, .outFile=NULL))
    expect_true(verifyData(tbl, "data.frame"))
    tbl <- mtx %>% summarise(m=mean(mpg), .method=2, .outFile=NULL)
    expect_true(verifyData(tbl, "data.frame"))
    expect_warning(tbl <- mtx %>% summarise(m=mean(mpg), .method=3, .outFile=NULL))
    expect_true(verifyData(tbl, "data.frame"))
    tbl <- mtx %>% summarise(m=mean(mpg), .method=4, .outFile=NULL)
    expect_true(verifyData(tbl, "data.frame"))
    tbl <- mtx %>% summarise(m=mean(mpg), .method=5, .outFile=NULL)
    expect_true(verifyData(tbl, "data.frame"))
})

test_that("transmute works", {
    tbl <- mtx %>% transmute(m2=2*mpg, sw=sqrt(wt), .outFile=NULL)
    expect_true(verifyData(tbl, "data.frame"))
})


# cleanup
file.remove("mtx.xdf")
