context("Joins, left source = csv, output = data frame")

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

verifyData <- function(xdf, expectedClass)
{
    is.data.frame(head(xdf)) && class(xdf) == expectedClass # test for exact class
}


test_that("csv to xdf joining -> data frame works",
{
    expect_true(verifyData(left_join(txt1, xdf2, .outFile=NULL), "data.frame"))
    expect_true(verifyData(right_join(txt1, xdf2, .outFile=NULL), "data.frame"))
    expect_true(verifyData(inner_join(txt1, xdf2, .outFile=NULL), "data.frame"))
    expect_true(verifyData(full_join(txt1, xdf2, .outFile=NULL), "data.frame"))
    expect_true(verifyData(semi_join(txt1, xdf2, .outFile=NULL), "data.frame"))
    expect_true(verifyData(anti_join(txt1, xdf2, .outFile=NULL), "data.frame"))

    expect_true(verifyData(inner_join(txt1, xdf2, by=c("b"="d"), .outFile=NULL), "data.frame"))
    expect_true(verifyData(inner_join(txt1, xdf2, by=c("b"="e"), .outFile=NULL), "data.frame"))
    expect_true(verifyData(inner_join(txt1, xdf2, by=c("c"="d"), .outFile=NULL), "data.frame"))

    expect_true(verifyData(union(txt1, txt1, .outFile=NULL), "data.frame"))
    expect_true(verifyData(union_all(txt1, txt1, .outFile=NULL), "data.frame"))
})


test_that("csv to data frame joining -> data frame works",
{
    expect_true(verifyData(left_join(txt1, df2, .outFile=NULL), "data.frame"))
    expect_true(verifyData(right_join(txt1, df2, .outFile=NULL), "data.frame"))
    expect_true(verifyData(inner_join(txt1, df2, .outFile=NULL), "data.frame"))
    expect_true(verifyData(full_join(txt1, df2, .outFile=NULL), "data.frame"))
    expect_true(verifyData(semi_join(txt1, df2, .outFile=NULL), "data.frame"))
    expect_true(verifyData(anti_join(txt1, df2, .outFile=NULL), "data.frame"))

    expect_true(verifyData(inner_join(txt1, df2, by=c("b"="d"), .outFile=NULL), "data.frame"))
    expect_true(verifyData(inner_join(txt1, df2, by=c("b"="e"), .outFile=NULL), "data.frame"))
    expect_true(verifyData(inner_join(txt1, df2, by=c("c"="d"), .outFile=NULL), "data.frame"))

    expect_true(verifyData(union(txt1, df1, .outFile=NULL), "data.frame"))
    expect_true(verifyData(union_all(txt1, df1, .outFile=NULL), "data.frame"))
})


test_that("csv to csv joining -> data frame works",
{
    expect_true(verifyData(left_join(txt1, txt2, .outFile=NULL), "data.frame"))
    expect_true(verifyData(right_join(txt1, txt2, .outFile=NULL), "data.frame"))
    expect_true(verifyData(inner_join(txt1, txt2, .outFile=NULL), "data.frame"))
    expect_true(verifyData(full_join(txt1, txt2, .outFile=NULL), "data.frame"))
    expect_true(verifyData(semi_join(txt1, txt2, .outFile=NULL), "data.frame"))
    expect_true(verifyData(anti_join(txt1, txt2, .outFile=NULL), "data.frame"))

    expect_true(verifyData(inner_join(txt1, txt2, by=c("b"="d"), .outFile=NULL), "data.frame"))
    expect_true(verifyData(inner_join(txt1, txt2, by=c("b"="e"), .outFile=NULL), "data.frame"))
    expect_true(verifyData(inner_join(txt1, txt2, by=c("c"="d"), .outFile=NULL), "data.frame"))

    expect_true(verifyData(union(txt1, txt1, .outFile=NULL), "data.frame"))
    expect_true(verifyData(union_all(txt1, txt1, .outFile=NULL), "data.frame"))
})

# clean up
file.remove(dir(pattern="\\.(csv|xdf)$"))
