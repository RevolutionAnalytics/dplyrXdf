context("Grouped functionality, output to persistent xdf")
mtx <- rxDataStep(mtcars, "mtx.xdf", overwrite=TRUE)



test_that("arrange works", {
    expect(mtx %>% group_by(gear) %>% arrange(mpg, disp, .output="test2b.xdf") %>% class == "RxXdfData", "not RxXdfData")
})

test_that("distinct works", {
    expect(mtx %>% group_by(gear) %>% distinct(cyl, gear, .output="test2b.xdf") %>% class == "RxXdfData", "not RxXdfData")

    # .keep_all will have no effect when dplyr < 0.5 installed
    skip_if_not(packageVersion("dplyr") >= package_version("0.5"))
    expect(mtx %>% group_by(gear) %>% distinct(cyl, gear, .keep_all=TRUE, .output="test2b.xdf") %>%
           class == "RxXdfData", "not RxXdfData")
    expect(mtx %>% group_by(gear) %>% distinct(cyl, gear, .keep_all=FALSE, .output="test2b.xdf") %>%
           class == "RxXdfData", "not RxXdfData")
})

test_that("do works", {
    expect_warning(mtx %>% group_by(gear) %>% do(m=lm(mpg ~ disp, .), .output="test2b.xdf"))
    expect_warning(mtx %>% group_by(gear) %>% doXdf(m=rxLinMod(mpg ~ disp, .), .output="test2b.xdf"))


})

test_that("factorise works", {
    expect(mtx %>% group_by(gear) %>% factorise(mpg, cyl, .output="test1b.xdf") %>% class == "RxXdfData", "not RxXdfData")
    expect(all(sapply(rxGetVarInfo(mtx %>% group_by(gear) %>% factorise(mpg, cyl, .output="test2b.xdf"))[c("mpg", "cyl")],
                      "[[", "varType") == "factor"),
           "factor conversion failed")
    expect(all(sapply(rxGetVarInfo(mtx %>% group_by(gear) %>% factorise(mpg, cyl, .output="test2b.xdf",
                      .rxArgs=list(sortLevels=TRUE)))[c("mpg", "cyl")],
                      "[[", "varType") == "factor"),
           "factor conversion failed")
})

test_that("filter works", {
    expect(mtx %>% group_by(gear) %>% filter(mpg > 16, cyl == 8, .output="test2b.xdf") %>% class == "RxXdfData",
           "not RxXdfData")
})

test_that("mutate works", {
    expect(mtx %>% group_by(gear) %>% mutate(m2=2*mpg, sw=sqrt(wt), .output="test2b.xdf") %>% class == "RxXdfData",
           "not RxXdfData")
})

test_that("rename works", {
    expect(mtx %>% group_by(gear) %>% rename(cc=cyl, mm=mpg, .output="test2b.xdf") %>% class == "RxXdfData",
           "not RxXdfData")
    expect(all(c("cc", "mm") %in% names(mtx %>% group_by(gear) %>% rename(cc=cyl, mm=mpg, .output="test2b.xdf"))),
           "rename failed")

})

test_that("select works", {
    expect(mtx %>% group_by(gear) %>% select(mpg, cyl, drat, .output="test2b.xdf") %>% class == "RxXdfData",
           "not RxXdfData")
})

test_that("subset works", {
    expect(mtx %>% group_by(gear) %>% subset(cyl==6, c(2,3,4), .output="test2b.xdf") %>% class == "RxXdfData",
           "not RxXdfData")
    expect(mtx %>% group_by(gear) %>% subset(cyl==6, c(cyl,disp,hp), .output="test2b.xdf") %>% class == "RxXdfData",
           "not RxXdfData")
    expect(mtx %>% group_by(gear) %>% subset_("cyl==6", "c(cyl,disp,hp)", .output="test2b.xdf") %>% class == "RxXdfData",
           "not RxXdfData")
})

test_that("summarise works", {
    expect(mtx %>% group_by(gear) %>% summarise(m=mean(mpg), .method=1, .output="test2b.xdf") %>% class == "RxXdfData",
           "not RxXdfData")
    expect(mtx %>% group_by(gear) %>% summarise(m=mean(mpg), .method=2, .output="test2b.xdf") %>% class == "RxXdfData",
           "not RxXdfData")
    expect(mtx %>% group_by(gear) %>% summarise(m=mean(mpg), .method=3, .output="test2b.xdf") %>% class == "RxXdfData",
           "not RxXdfData")
    expect(mtx %>% group_by(gear) %>% summarise(m=mean(mpg), .method=4, .output="test2b.xdf") %>% class == "RxXdfData",
           "not RxXdfData")
    expect(mtx %>% group_by(gear) %>% summarise(m=mean(mpg), .method=5, .output="test2b.xdf") %>% class == "RxXdfData",
           "not RxXdfData")
})

test_that("transmute works", {
    expect(mtx %>% group_by(gear) %>% transmute(m2=2*mpg, sw=sqrt(wt), .output="test2b.xdf") %>% class == "RxXdfData",
           "not RxXdfData")
})


# cleanup
file.remove("mtx.xdf")
file.remove("test2b.xdf")
