context("joins, left source = csv, output = data frame")

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


test_that("csv to xdf joining -> data frame works", {
    expect_s3_class(left_join(txt1, xdf2, .output=NULL), "data.frame")
    expect_s3_class(right_join(txt1, xdf2, .output=NULL), "data.frame")
    expect_s3_class(inner_join(txt1, xdf2, .output=NULL), "data.frame")
    expect_s3_class(full_join(txt1, xdf2, .output=NULL), "data.frame")
    expect_s3_class(semi_join(txt1, xdf2, .output=NULL), "data.frame")
    expect_s3_class(anti_join(txt1, xdf2, .output=NULL), "data.frame")

    expect_s3_class(inner_join(txt1, xdf2, by=c("b"="d"), .output=NULL), "data.frame")
    expect_s3_class(inner_join(txt1, xdf2, by=c("b"="e"), .output=NULL), "data.frame")
    expect_s3_class(inner_join(txt1, xdf2, by=c("c"="d"), .output=NULL), "data.frame")

    skip_if_not(packageVersion("dplyr") >= package_version("0.5"))
    expect_s3_class(union(txt1, txt1, .output=NULL), "data.frame")
    expect_s3_class(union_all(txt1, txt1, .output=NULL), "data.frame")
})


test_that("csv to data frame joining -> data frame works", {
    expect_s3_class(left_join(txt1, df2, .output=NULL), "data.frame")
    expect_s3_class(right_join(txt1, df2, .output=NULL), "data.frame")
    expect_s3_class(inner_join(txt1, df2, .output=NULL), "data.frame")
    expect_s3_class(full_join(txt1, df2, .output=NULL), "data.frame")
    expect_s3_class(semi_join(txt1, df2, .output=NULL), "data.frame")
    expect_s3_class(anti_join(txt1, df2, .output=NULL), "data.frame")

    expect_s3_class(inner_join(txt1, df2, by=c("b"="d"), .output=NULL), "data.frame")
    expect_s3_class(inner_join(txt1, df2, by=c("b"="e"), .output=NULL), "data.frame")
    expect_s3_class(inner_join(txt1, df2, by=c("c"="d"), .output=NULL), "data.frame")

    skip_if_not(packageVersion("dplyr") >= package_version("0.5"))
    expect_s3_class(union(txt1, df1), .output=NULL, "data.frame")
    expect_s3_class(union_all(txt1, df1), .output=NULL, "data.frame")
})


test_that("csv to csv joining -> data frame works", {
    expect_s3_class(left_join(txt1, txt2, .output=NULL), "data.frame")
    expect_s3_class(right_join(txt1, txt2, .output=NULL), "data.frame")
    expect_s3_class(inner_join(txt1, txt2, .output=NULL), "data.frame")
    expect_s3_class(full_join(txt1, txt2, .output=NULL), "data.frame")
    expect_s3_class(semi_join(txt1, txt2, .output=NULL), "data.frame")
    expect_s3_class(anti_join(txt1, txt2, .output=NULL), "data.frame")

    expect_s3_class(inner_join(txt1, txt2, by=c("b"="d"), .output=NULL), "data.frame")
    expect_s3_class(inner_join(txt1, txt2, by=c("b"="e"), .output=NULL), "data.frame")
    expect_s3_class(inner_join(txt1, txt2, by=c("c"="d"), .output=NULL), "data.frame")

    skip_if_not(packageVersion("dplyr") >= package_version("0.5"))
    expect_s3_class(union(txt1, txt1, .output=NULL), "data.frame")
    expect_s3_class(union_all(txt1, txt1, .output=NULL), "data.frame")
})

# clean up
file.remove(dir(pattern="\\.(csv|xdf)$"))
