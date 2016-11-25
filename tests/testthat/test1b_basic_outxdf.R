context("Output to persistent xdf")
mtx <- rxDataStep(mtcars, "mtx.xdf", overwrite=TRUE)



test_that("arrange works", {
    expect(mtx %>% arrange(mpg, disp, .output="test1b.xdf") %>% class == "RxXdfData", "not RxXdfData")
})

test_that("distinct works", {
    expect(mtx %>% distinct(cyl, gear, .output="test1b.xdf") %>% class == "RxXdfData", "not RxXdfData")

    # .keep_all will have no effect when dplyr < 0.5 installed
    skip_if_not(packageVersion("dplyr") >= package_version("0.5"))
    expect(mtx %>% distinct(cyl, gear, .keep_all=TRUE, .output="test1b.xdf") %>% class == "RxXdfData", "not RxXdfData")
    expect(mtx %>% distinct(cyl, gear, .keep_all=FALSE, .output="test1b.xdf") %>% class == "RxXdfData", "not RxXdfData")
})

test_that("do works", {
    expect_warning(mtx %>% do(m=lm(mpg ~ disp, .), .output="test1b.xdf"))
    expect_warning(mtx %>% doXdf(m=rxLinMod(mpg ~ disp, .), .output="test1b.xdf"))


})

test_that("factorise works", {
    expect(mtx %>% factorise(mpg, cyl, .output="test1b.xdf") %>% class == "RxXdfData", "not RxXdfData")
    expect(all(sapply(rxGetVarInfo(mtx %>% factorise(mpg, cyl, .output="test1b.xdf"))[c("mpg", "cyl")],
                      "[[", "varType") == "factor"),
           "factor conversion failed")
    expect(all(sapply(rxGetVarInfo(mtx %>% factorise(mpg, cyl, .output="test1b.xdf", .rxArgs=list(sortLevels=TRUE)))
                      [c("mpg", "cyl")],
                      "[[", "varType") == "factor"),
           "factor conversion failed")
})

test_that("filter works", {
    expect(mtx %>% filter(mpg > 16, cyl == 8, .output="test1b.xdf") %>% class == "RxXdfData", "not RxXdfData")
})

test_that("mutate works", {
    expect(mtx %>% mutate(m2=2*mpg, sw=sqrt(wt), .output="test1b.xdf") %>% class == "RxXdfData", "not RxXdfData")
})

test_that("rename works", {
    expect(mtx %>% rename(cc=cyl, mm=mpg, .output="test1b.xdf") %>% class == "RxXdfData", "not RxXdfData")
    expect(all(c("cc", "mm") %in% names(mtx %>% rename(cc=cyl, mm=mpg, .output="test1b.xdf"))), "rename failed")

})

test_that("select works", {
    expect(mtx %>% select(mpg, cyl, drat, .output="test1b.xdf") %>% class == "RxXdfData", "not RxXdfData")
})

test_that("subset works", {
    expect(mtx %>% subset(cyl==6, c(2,3,4), .output="test1b.xdf") %>% class == "RxXdfData", "not RxXdfData")
    expect(mtx %>% subset(cyl==6, c(cyl,disp,hp), .output="test1b.xdf") %>% class == "RxXdfData", "not RxXdfData")
    expect(mtx %>% subset_("cyl==6", "c(cyl,disp,hp)", .output="test1b.xdf") %>% class == "RxXdfData", "not RxXdfData")
})

test_that("summarise works", {
    expect_warning(mtx %>% summarise(m=mean(mpg), .method=1, .output="test1b.xdf"))
    expect(mtx %>% summarise(m=mean(mpg), .method=2, .output="test1b.xdf") %>% class == "RxXdfData", "not RxXdfData")
    expect(mtx %>% summarise(m=mean(mpg), .method=3, .output="test1b.xdf") %>% class == "RxXdfData", "not RxXdfData")
    expect(mtx %>% summarise(m=mean(mpg), .method=4, .output="test1b.xdf") %>% class == "RxXdfData", "not RxXdfData")
    expect(mtx %>% summarise(m=mean(mpg), .method=5, .output="test1b.xdf") %>% class == "RxXdfData", "not RxXdfData")
})

test_that("transmute works", {
    expect(mtx %>% transmute(m2=2*mpg, sw=sqrt(wt), .output="test1b.xdf") %>% class == "RxXdfData", "not RxXdfData")
})


# cleanup
file.remove("mtx.xdf")
file.remove("test1b.xdf")
