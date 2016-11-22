mtx <- rxDataStep(mtcars, "mtx.xdf", overwrite=TRUE)


## base

# arrange
expect_s4_class(mtx %>% arrange(mpg, disp), "tbl_xdf")

# distinct
expect_s4_class(mtx %>% distinct(cyl, gear), "tbl_xdf")
# .keep_all will have no effect when dplyr < 0.5 installed
if(packageVersion("dplyr") >= package_version("0.5"))
{
    expect_s4_class(mtx %>% distinct(cyl, gear, .keep_all=TRUE), "tbl_xdf")
    expect_s4_class(mtx %>% distinct(cyl, gear, .keep_all=FALSE), "tbl_xdf")
}

# do
expect_s3_class(mtx %>% do(m=lm(mpg ~ disp, .)), "data.frame")
expect_s4_class(mtx %>% doXdf(m=rxLinMod(mpg ~ disp, .)), "tbl_xdf")

# factorise
sapply(rxGetVarInfo(mtx %>% factorise(mpg, cyl))[c("mpg", "cyl")], `[[`, "varType") %>%
    sapply(matches("factor"))
sapply(rxGetVarInfo(mtx %>% factorise(mpg, cyl, .rxArgs=list(sortLevels=TRUE)))[c("mpg", "cyl")], `[[`, "varType") %>%
    sapply(matches("factor"))

# filter
expect_s4_class(mtx %>% filter(mpg > 16, cyl == 8), "tbl_xdf")

# mutate
expect_s4_class(mtx %>% mutate(m2=2*mpg, sw=sqrt(wt), "tbl_xdf"))

# rename
expect_s4_class(mtx %>% rename(cc=cyl, mm=mpg), "tbl_xdf")

# select
expect_s4_class(mtx %>% select(mpg, cyl, drat), "tbl_xdf")

# subset
expect_s4_class(mtx %>% subset(cyl==6, c(2,3,4), "tbl_xdf"))
expect_s4_class(mtx %>% subset(cyl==6, c(cyl,disp,hp), "tbl_xdf"))
expect_s4_class(mtx %>% subset_("cyl==6", "c(cyl,disp,hp)"), "tbl_xdf")

# summarise
expect_s4_class(mtx %>% summarise(m=mean(mpg), .method=1), "tbl_xdf")
expect_s4_class(mtx %>% summarise(m=mean(mpg), .method=2), "tbl_xdf")
expect_s4_class(mtx %>% summarise(m=mean(mpg), .method=3), "tbl_xdf")
expect_s4_class(mtx %>% summarise(m=mean(mpg), .method=4), "tbl_xdf")
expect_s4_class(mtx %>% summarise(m=mean(mpg), .method=5), "tbl_xdf")

# transmute
expect_s4_class(mtx %>% transmute(m2=2*mpg, sw=sqrt(wt), "tbl_xdf"))
