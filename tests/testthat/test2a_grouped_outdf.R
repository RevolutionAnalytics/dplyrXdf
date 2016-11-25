context("Grouped functionality, output to data frame")
mtx <- rxDataStep(mtcars, "mtx.xdf", overwrite=TRUE)



test_that("arrange works", {
    expect_s3_class(mtx %>% group_by(gear) %>% arrange(mpg, disp, .outFile=NULL), "data.frame")
})

test_that("distinct works", {
    expect_s3_class(mtx %>% group_by(gear) %>% distinct(cyl, gear, .outFile=NULL), "data.frame")

    # .keep_all will have no effect when dplyr < 0.5 installed
    skip_if_not(packageVersion("dplyr") >= package_version("0.5"))
    expect_s3_class(mtx %>% group_by(gear) %>% distinct(cyl, gear, .keep_all=TRUE, .outFile=NULL), "data.frame")
    expect_s3_class(mtx %>% group_by(gear) %>% distinct(cyl, gear, .keep_all=FALSE, .outFile=NULL), "data.frame")
})

test_that("do works", {
    expect_s3_class(mtx %>% group_by(gear) %>% do(m=lm(mpg ~ disp, .)), "data.frame")
    expect_s3_class(mtx %>% group_by(gear) %>% doXdf(m=rxLinMod(mpg ~ disp, .)), "data.frame")


})

test_that("factorise works", {
    expect_s3_class(mtx %>% group_by(gear) %>% factorise(mpg, cyl, .outFile=NULL), "data.frame")
    expect(all(sapply(rxGetVarInfo(mtx %>% group_by(gear) %>% factorise(mpg, cyl, .outFile=NULL))[c("mpg", "cyl")],
                      "[[", "varType") == "factor"),
           "factor conversion failed")
    expect(all(sapply(rxGetVarInfo(mtx %>% group_by(gear) %>% factorise(mpg, cyl, .outFile=NULL,
                      .rxArgs=list(sortLevels=TRUE)))[c("mpg", "cyl")],
                      "[[", "varType") == "factor"),
           "factor conversion failed")
})

test_that("filter works", {
    expect_s3_class(mtx %>% group_by(gear) %>% filter(mpg > 16, cyl == 8, .outFile=NULL), "data.frame")
})

test_that("mutate works", {
    expect_s3_class(mtx %>% group_by(gear) %>% mutate(m2=2*mpg, sw=sqrt(wt), .outFile=NULL), "data.frame")
})

test_that("rename works", {
    expect_s3_class(mtx %>% group_by(gear) %>% rename(cc=cyl, mm=mpg, .outFile=NULL), "data.frame") %>%
        names %>%
        expect_match("cc", all=FALSE) %>%
        expect_match("mm", all=FALSE)
})

test_that("select works", {
    expect_s3_class(mtx %>% group_by(gear) %>% select(mpg, cyl, drat, .outFile=NULL), "data.frame")
})

test_that("subset works", {
    expect_s3_class(mtx %>% group_by(gear) %>% subset(cyl==6, c(2,3,4), .outFile=NULL), "data.frame")
    expect_s3_class(mtx %>% group_by(gear) %>% subset(cyl==6, c(cyl,disp,hp), .outFile=NULL), "data.frame")
    expect_s3_class(mtx %>% group_by(gear) %>% subset_("cyl==6", "c(cyl,disp,hp)", .outFile=NULL), "data.frame")
})

test_that("summarise works", {
    expect_s3_class(mtx %>% group_by(gear) %>% summarise(m=mean(mpg), .method=1, .outFile=NULL), "data.frame")
    expect_s3_class(mtx %>% group_by(gear) %>% summarise(m=mean(mpg), .method=2, .outFile=NULL), "data.frame")
    expect_s3_class(mtx %>% group_by(gear) %>% summarise(m=mean(mpg), .method=3, .outFile=NULL), "data.frame")
    expect_s3_class(mtx %>% group_by(gear) %>% summarise(m=mean(mpg), .method=4, .outFile=NULL), "data.frame")
    expect_s3_class(mtx %>% group_by(gear) %>% summarise(m=mean(mpg), .method=5, .outFile=NULL), "data.frame")
})

test_that("transmute works", {
    expect_s3_class(mtx %>% group_by(gear) %>% transmute(m2=2*mpg, sw=sqrt(wt), .outFile=NULL), "data.frame")
})


# cleanup
file.remove("mtx.xdf")
