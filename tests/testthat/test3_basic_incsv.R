context("Text input")
write.csv(mtcars, "mtcars.csv", row.names=FALSE)
mtt <- RxTextData("mtcars.csv")


test_that("arrange works", {
    expect_s4_class(mtt %>% arrange(mpg, disp), "tbl_xdf")
})

test_that("distinct works", {
    expect_s4_class(mtt %>% distinct(cyl, gear), "tbl_xdf")

    # .keep_all will have no effect when dplyr < 0.5 installed
    skip_if_not(packageVersion("dplyr") >= package_version("0.5"))
    expect_s4_class(mtt %>% distinct(cyl, gear, .keep_all=TRUE), "tbl_xdf")
    expect_s4_class(mtt %>% distinct(cyl, gear, .keep_all=FALSE), "tbl_xdf")
})

test_that("do works", {
    expect_s3_class(mtt %>% do(m=lm(mpg ~ disp, .)), "data.frame")
    expect_s3_class(mtt %>% doXdf(m=rxLinMod(mpg ~ disp, .)), "data.frame")

    expect_warning(mtt %>% do(m=lm(mpg ~ disp, .), .rxArgs=42))
})

test_that("factorise works", {
    expect_s4_class(mtt %>% factorise(mpg, cyl), "tbl_xdf")
    expect(all(sapply(rxGetVarInfo(mtt %>% factorise(mpg, cyl))[c("mpg", "cyl")],
                      "[[", "varType") == "factor"),
           "factor conversion failed")
})

test_that("filter works", {
    expect_s4_class(mtt %>% filter(mpg > 16, cyl == 8), "tbl_xdf")
})

test_that("mutate works", {
    expect_s4_class(mtt %>% mutate(m2=2*mpg, sw=sqrt(wt)), "tbl_xdf")
})

test_that("rename works", {
    expect_s4_class(mtt %>% rename(cc=cyl, mm=mpg), "tbl_xdf") %>%
        names %>%
        expect_match("cc", all=FALSE) %>%
        expect_match("mm", all=FALSE)
})

test_that("select works", {
    expect_s4_class(mtt %>% select(mpg, cyl, drat), "tbl_xdf")
})

test_that("subset works", {
    expect_s4_class(mtt %>% subset(cyl==6, c(2,3,4)), "tbl_xdf")
    expect_s4_class(mtt %>% subset(cyl==6, c(cyl,disp,hp)), "tbl_xdf")
    expect_s4_class(mtt %>% subset_("cyl==6", "c(cyl,disp,hp)"), "tbl_xdf")
})

test_that("summarise works", {
    expect_warning(mtt %>% summarise(m=mean(mpg), .method=1))
    expect_s4_class(mtt %>% summarise(m=mean(mpg), .method=2), "tbl_xdf")
    expect_warning(mtt %>% summarise(m=mean(mpg), .method=3))
    expect_s4_class(mtt %>% summarise(m=mean(mpg), .method=4), "tbl_xdf")
    expect_s4_class(mtt %>% summarise(m=mean(mpg), .method=5), "tbl_xdf")
})

test_that("transmute works", {
    expect_s4_class(mtt %>% transmute(m2=2*mpg, sw=sqrt(wt)), "tbl_xdf")
})

test_that("setting tbl dir works", {
    expect(getXdfTblDir() != tempdir(), "changing tbl dir failed")
    expect_gt(length(dir(getXdfTblDir())), 0, "no files in tbl dir")
})

# cleanup
file.remove("mtcars.csv")
