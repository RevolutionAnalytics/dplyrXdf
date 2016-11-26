context("Grouped functionality, output to data frame")
mtx <- rxDataStep(mtcars, "mtx.xdf", overwrite=TRUE)

verifyData <- function(xdf, expectedClass)
{
    is.data.frame(head(xdf)) && class(xdf) == expectedClass  # test for exact class
}


test_that("arrange works", {
    tbl <- mtx %>% group_by(gear) %>% arrange(mpg, disp, .outFile=NULL)
    expect_true(verifyData(tbl, "grouped_df"))
})

test_that("distinct works", {
    tbl <- mtx %>% group_by(gear) %>% distinct(cyl, gear, .outFile=NULL)
    expect_true(verifyData(tbl, "grouped_df"))

    # .keep_all will have no effect when dplyr < 0.5 installed
    skip_if_not(packageVersion("dplyr") >= package_version("0.5"))
    tbl <- mtx %>% group_by(gear) %>% distinct(cyl, gear, .keep_all=TRUE, .outFile=NULL)
    expect_true(verifyData(tbl, "grouped_df"))
    tbl <- mtx %>% group_by(gear) %>% distinct(cyl, gear, .keep_all=FALSE, .outFile=NULL)
    expect_true(verifyData(tbl, "grouped_df"))
})

test_that("do works", {
    tbl <- mtx %>% group_by(gear) %>% do(m=lm(mpg ~ disp, .), .outFile=NULL)
    expect_true(verifyData(tbl, "rowwise_df"))
    tbl <- mtx %>% group_by(gear) %>% doXdf(m=rxLinMod(mpg ~ disp, .), .outFile=NULL)
    expect_true(verifyData(tbl, "rowwise_df"))
    tbl <- mtx %>% group_by(gear) %>% do(data.frame(mpg2 = .$mpg * 2), .outFile=NULL)
    expect_true(verifyData(tbl, "grouped_df"))
    tbl <- mtx %>% group_by(gear) %>% doXdf(rxDataStep(., transforms=list(mpg2=mpg*2)), .outFile=NULL)
    expect_true(verifyData(tbl, "grouped_df"))
})

test_that("factorise works", {
    tbl <- mtx %>% group_by(gear) %>% factorise(mpg, cyl, .outFile=NULL)
    expect_true(verifyData(tbl, "grouped_df"))
    expect(all(sapply(rxGetVarInfo(tbl)[c("mpg", "cyl")], "[[", "varType") == "factor"),
           "factor conversion failed")
})

test_that("filter works", {
    tbl <- mtx %>% group_by(gear) %>% filter(mpg > 16, cyl == 8, .outFile=NULL)
    expect_true(verifyData(tbl, "grouped_df"))
})

test_that("mutate works", {
    tbl <- mtx %>% group_by(gear) %>% mutate(m2=2*mpg, sw=sqrt(wt), .outFile=NULL)
    expect_true(verifyData(tbl, "grouped_df"))
})

test_that("rename works", {
    tbl <- mtx %>% group_by(gear) %>% rename(cc=cyl, mm=mpg, .outFile=NULL)
    expect_true(verifyData(tbl, "grouped_df"))
    expect_true(all(c("cc", "mm") %in% names(tbl)))
})

test_that("select works", {
    tbl <- mtx %>% group_by(gear) %>% select(mpg, cyl, drat, .outFile=NULL)
    expect_true(verifyData(tbl, "grouped_df"))
})

test_that("subset works", {
    tbl <- mtx %>% group_by(gear) %>% subset(cyl==6, c(2,3,4), .outFile=NULL)
    expect_true(verifyData(tbl, "grouped_df"))
})

test_that("summarise works", {
    tbl <- mtx %>% group_by(gear) %>% summarise(m=mean(mpg), .method=1, .outFile=NULL)
    expect_true(verifyData(tbl, "data.frame"))
    tbl <- mtx %>% group_by(gear) %>% summarise(m=mean(mpg), .method=2, .outFile=NULL)
    expect_true(verifyData(tbl, "data.frame"))
    tbl <- mtx %>% group_by(gear) %>% summarise(m=mean(mpg), .method=3, .outFile=NULL)
    expect_true(verifyData(tbl, "data.frame"))
    tbl <- mtx %>% group_by(gear) %>% summarise(m=mean(mpg), .method=4, .outFile=NULL)
    expect_true(verifyData(tbl, "data.frame"))
    tbl <- mtx %>% group_by(gear) %>% summarise(m=mean(mpg), .method=5, .outFile=NULL)
    expect_true(verifyData(tbl, "data.frame"))
})

test_that("transmute works", {
    tbl <- mtx %>% group_by(gear) %>% transmute(m2=2*mpg, sw=sqrt(wt), .outFile=NULL)
    expect_true(verifyData(tbl, "grouped_df"))
})


# cleanup
file.remove("mtx.xdf")
