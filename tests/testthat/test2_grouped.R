context("Basic grouped functionality")
mtx <- rxDataStep(mtcars, "mtx.xdf", overwrite=TRUE)



test_that("arrange works", {
    expect_s4_class(mtx %>% group_by(gear) %>% arrange(mpg, disp), "tbl_xdf")
})

test_that("distinct works", {
    expect_s4_class(mtx %>% group_by(gear) %>% distinct(cyl, gear), "tbl_xdf")

    # .keep_all will have no effect when dplyr < 0.5 installed
    skip_if_not(packageVersion("dplyr") >= package_version("0.5"))
    expect_s4_class(mtx %>% group_by(gear) %>% distinct(cyl, gear, .keep_all=TRUE), "tbl_xdf")
    expect_s4_class(mtx %>% group_by(gear) %>% distinct(cyl, gear, .keep_all=FALSE), "tbl_xdf")
})

test_that("do works", {
    expect_s3_class(mtx %>% group_by(gear) %>% do(m=lm(mpg ~ disp, .)), "data.frame")
    expect_s3_class(mtx %>% group_by(gear) %>% doXdf(m=rxLinMod(mpg ~ disp, .)), "data.frame")

    expect_warning(mtx %>% group_by(gear) %>% do(m=lm(mpg ~ disp, .), .rxArgs=42))
})

test_that("factorise works", {
    expect(all(sapply(rxGetVarInfo(mtx %>% group_by(gear) %>% factorise(mpg, cyl))[c("mpg", "cyl")],
                      "[[", "varType") == "factor"),
           "factor conversion failed")
    expect(all(sapply(rxGetVarInfo(mtx %>% group_by(gear) %>% factorise(mpg, cyl, .rxArgs=list(sortLevels=TRUE)))
                      [c("mpg", "cyl")],
                      "[[", "varType") == "factor"),
           "factor conversion failed")
})

test_that("filter works", {
    expect_s4_class(mtx %>% group_by(gear) %>% filter(mpg > 16, cyl == 8), "tbl_xdf")
})

test_that("mutate works", {
    expect_s4_class(mtx %>% group_by(gear) %>% mutate(m2=2*mpg, sw=sqrt(wt)), "tbl_xdf")
})

test_that("rename works", {
    expect_s4_class(mtx %>% group_by(gear) %>% rename(cc=cyl, mm=mpg), "tbl_xdf") %>%
        names %>%
        expect_match("cc", all=FALSE) %>%
        expect_match("mm", all=FALSE)
})

test_that("select works", {
    expect_s4_class(mtx %>% group_by(gear) %>% select(mpg, cyl, drat), "tbl_xdf")
})

test_that("subset works", {
    expect_s4_class(mtx %>% group_by(gear) %>% subset(cyl==6, c(2,3,4)), "tbl_xdf")
    expect_s4_class(mtx %>% group_by(gear) %>% subset(cyl==6, c(cyl,disp,hp)), "tbl_xdf")
    expect_s4_class(mtx %>% group_by(gear) %>% subset_("cyl==6", "c(cyl,disp,hp)"), "tbl_xdf")
})

test_that("summarise works", {
    expect_s4_class(mtx %>% group_by(gear) %>% summarise(m=mean(mpg), .method=1), "tbl_xdf")
    expect_s4_class(mtx %>% group_by(gear) %>% summarise(m=mean(mpg), .method=2), "tbl_xdf")
    expect_s4_class(mtx %>% group_by(gear) %>% summarise(m=mean(mpg), .method=3), "tbl_xdf")
    expect_s4_class(mtx %>% group_by(gear) %>% summarise(m=mean(mpg), .method=4), "tbl_xdf")
    expect_s4_class(mtx %>% group_by(gear) %>% summarise(m=mean(mpg), .method=5), "tbl_xdf")
})

test_that("transmute works", {
    expect_s4_class(mtx %>% group_by(gear) %>% transmute(m2=2*mpg, sw=sqrt(wt)), "tbl_xdf")
})


# cleanup
file.remove("mtx.xdf")
