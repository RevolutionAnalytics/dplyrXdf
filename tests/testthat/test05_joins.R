context("Joins, left source = xdf")
library(dplyr)
df1 <- data.frame(a=letters[1:20], b=1:20, c=11:30, stringsAsFactors=FALSE)
df2 <- data.frame(a=letters[7:26], d=as.character(1:20), e=11:30, stringsAsFactors=FALSE)
xdf1 <- rxDataStep(df1, "xdf1.xdf", overwrite=TRUE)
xdf2 <- rxDataStep(df2, "xdf2.xdf", overwrite=TRUE)

txt1 <- rxXdfToText(xdf1, "txt1.csv", overwrite=TRUE)
txt2 <- rxXdfToText(xdf2, "txt2.csv", overwrite=TRUE)

df1f <- data.frame(a=factor(letters[1:20]), b=1:20, c=11:30, stringsAsFactors=FALSE)
df2f <- data.frame(a=factor(letters[7:26]), d=as.character(1:20), e=11:30, stringsAsFactors=FALSE)

xdf1f <- rxDataStep(df1f, "xdf1f.xdf", overwrite=TRUE)
xdf2f <- rxDataStep(df2f, "xdf2f.xdf", overwrite=TRUE)

isDplyr5 <- packageVersion("dplyr") >= package_version("0.5")


test_that("xdf to xdf joining works", {
    expect_s4_class(left_join(xdf1, xdf2), "tbl_xdf")
    expect_s4_class(right_join(xdf1, xdf2), "tbl_xdf")
    expect_s4_class(inner_join(xdf1, xdf2), "tbl_xdf")
    expect_s4_class(full_join(xdf1, xdf2), "tbl_xdf")
    expect_s4_class(semi_join(xdf1, xdf2), "tbl_xdf")
    expect_s4_class(anti_join(xdf1, xdf2), "tbl_xdf")
    expect_s4_class(union(xdf1, xdf1), "tbl_xdf")
    if(isDplyr5) expect_s4_class(union_all(xdf1, xdf1), "tbl_xdf")

    expect_s4_class(inner_join(xdf1, xdf2, by=c("b"="d")), "tbl_xdf")
    expect_s4_class(inner_join(xdf1, xdf2, by=c("b"="e")), "tbl_xdf")
    expect_s4_class(inner_join(xdf1, xdf2, by=c("c"="d")), "tbl_xdf")

    expect_s4_class(inner_join(xdf1f, xdf2f), "tbl_xdf")
    expect_s4_class(inner_join(xdf1, xdf2f), "tbl_xdf")
    expect_s4_class(inner_join(xdf1f, xdf2), "tbl_xdf")
})


test_that("xdf to data frame joining works", {
    expect_s4_class(left_join(xdf1, df2), "tbl_xdf")
    expect_s4_class(right_join(xdf1, df2), "tbl_xdf")
    expect_s4_class(inner_join(xdf1, df2), "tbl_xdf")
    expect_s4_class(full_join(xdf1, df2), "tbl_xdf")
    expect_s4_class(semi_join(xdf1, df2), "tbl_xdf")
    expect_s4_class(anti_join(xdf1, df2), "tbl_xdf")
    expect_s4_class(union(xdf1, df1), "tbl_xdf")
    if(isDplyr5) expect_s4_class(union_all(xdf1, df1), "tbl_xdf")

    expect_s4_class(inner_join(xdf1, df2, by=c("b"="d")), "tbl_xdf")
    expect_s4_class(inner_join(xdf1, df2, by=c("b"="e")), "tbl_xdf")
    expect_s4_class(inner_join(xdf1, df2, by=c("c"="d")), "tbl_xdf")

    expect_s4_class(inner_join(xdf1f, df2f), "tbl_xdf")
    expect_s4_class(inner_join(xdf1, df2f), "tbl_xdf")
    expect_s4_class(inner_join(xdf1f, df2), "tbl_xdf")
})


test_that("xdf to csv joining works", {
    expect_s4_class(left_join(xdf1, txt2), "tbl_xdf")
    expect_s4_class(right_join(xdf1, txt2), "tbl_xdf")
    expect_s4_class(inner_join(xdf1, txt2), "tbl_xdf")
    expect_s4_class(full_join(xdf1, txt2), "tbl_xdf")
    expect_s4_class(semi_join(xdf1, txt2), "tbl_xdf")
    expect_s4_class(anti_join(xdf1, txt2), "tbl_xdf")
    expect_s4_class(union(xdf1, txt1), "tbl_xdf")
    if(isDplyr5) expect_s4_class(union_all(xdf1, txt1), "tbl_xdf")

    expect_s4_class(inner_join(xdf1, txt2, by=c("b"="d")), "tbl_xdf")
    expect_s4_class(inner_join(xdf1, txt2, by=c("b"="e")), "tbl_xdf")
    expect_s4_class(inner_join(xdf1, txt2, by=c("c"="d")), "tbl_xdf")

    expect_s4_class(inner_join(xdf1f, df2f), "tbl_xdf")
    expect_s4_class(inner_join(xdf1, df2f), "tbl_xdf")
    expect_s4_class(inner_join(xdf1f, df2), "tbl_xdf")
})

# clean up
file.remove(dir(pattern="\\.(csv|xdf)$"))
