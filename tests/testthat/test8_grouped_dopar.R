context("Parallel backend")
mtx <- rxDataStep(mtcars, "mtx.xdf", overwrite=TRUE)

doParallel::registerDoParallel()
rxSetComputeContext("dopar")

test_that("arrange works", {
    expect_s4_class(mtx %>% group_by(gear) %>% arrange(mpg, disp), "tbl_xdf")
})

test_that("distinct works", {
    expect_s4_class(mtx %>% group_by(gear) %>% distinct(cyl, gear), "tbl_xdf")
})

test_that("do works", {
    expect_s3_class(mtx %>% group_by(gear) %>% do(m=lm(mpg ~ disp, .)), "data.frame")
    expect_s3_class(mtx %>% group_by(gear) %>% doXdf(m=rxLinMod(mpg ~ disp, .)), "data.frame")
    expect_s3_class(mtx %>% group_by(gear) %>% do(data.frame(mpg2=2*.$mpg)), "data.frame")
    expect_s3_class(mtx %>% group_by(gear) %>% do(rxDataStep(., transforms=list(mpg2=2*mpg))), "data.frame")
    expect_s3_class(mtx %>% group_by(gear) %>% doXdf(rxDataStep(., transforms=list(mpg2=2*mpg))), "data.frame")
})

test_that("factorise works", {
    rxGetVarInfo(mtx %>% group_by(gear) %>% factorise(mpg, cyl))[c("mpg", "cyl")]
})

test_that("filter works", {
    expect_s4_class(mtx %>% group_by(gear) %>% filter(mpg > 16, cyl == 8), "tbl_xdf")
})

test_that("mutate works", {
    expect_s4_class(mtx %>% group_by(gear) %>% mutate(m2=2*mpg, sw=sqrt(wt)), "tbl_xdf")
})

test_that("rename works", {
    expect_s4_class(mtx %>% group_by(gear) %>% rename(cc=cyl, mm=mpg), "tbl_xdf")
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

rxSetComputeContext("local")
doParallel::stopImplicitCluster()

# cleanup
file.remove("mtx.xdf")
