context("Composite Xdf input")
mtc <- RxXdfData("mtc", createCompositeSet=TRUE)
rxDataStep(mtcars, mtc, overwrite=TRUE)

verifyCompositeData <- function(xdf, expectedClass)
{
    isTRUE(xdf@createCompositeSet) && is.data.frame(head(xdf)) && class(xdf) == expectedClass # test for exact class
}


# basic single-table verbs

test_that("filter works",
{
    tbl <- mtc %>% filter(mpg > 15, cyl <= 6)
    expect_true(verifyCompositeData(tbl, "tbl_xdf"))
})

test_that("select works",
{
    tbl <- mtc %>% select(mpg, cyl, drat)
    expect_true(verifyCompositeData(tbl, "tbl_xdf"))
})

test_that("subset works",
{
    tbl <- mtc %>% subset(mpg > 15, c(mpg, cyl, drat))
    expect_true(verifyCompositeData(tbl, "tbl_xdf"))
})

test_that("mutate works",
{
    tbl <- mtc %>% mutate(mpg2=sin(mpg), wt2=sqrt(wt))
    expect_true(verifyCompositeData(tbl, "tbl_xdf"))
})

test_that("transmute works",
{
    tbl <- mtc %>% transmute(mpg2=sin(mpg), wt2=sqrt(wt))
    expect_true(verifyCompositeData(tbl, "tbl_xdf"))
})


# summarise and do

test_that("ungrouped summarise works",
{
    tbl <- mtc %>% summarise(n=n(), mpg2=mean(mpg))
    expect_true(verifyCompositeData(tbl, "tbl_xdf"))
    expect_warning(tbl <- mtc %>% summarise(n=n(), mpg2=mean(mpg), .method=1))
    expect_true(verifyCompositeData(tbl, "tbl_xdf"))
    tbl <- mtc %>% summarise(n=n(), mpg2=mean(mpg), .method=2)
    expect_true(verifyCompositeData(tbl, "tbl_xdf"))
    expect_warning(tbl <- mtc %>% summarise(n=n(), mpg2=mean(mpg), .method=3))
    expect_true(verifyCompositeData(tbl, "tbl_xdf"))
    tbl <- mtc %>% summarise(n=n(), mpg2=mean(mpg), .method=4)
    expect_true(verifyCompositeData(tbl, "tbl_xdf"))
    tbl <- mtc %>% summarise(n=n(), mpg2=mean(mpg), .method=5)
    expect_true(verifyCompositeData(tbl, "tbl_xdf"))
})

test_that("ungrouped do works",
{
    tbl <- mtc %>% do(m=lm(mpg ~ cyl, data=.), w=lm(wt ~ cyl, data=.))
    expect_true(is.data.frame(tbl))
    tbl <- mtc %>% do(
    {
        .$mpg2 <- sin(.$mpg)
        .$cyl2 <- sqrt(.$cyl)
        .
    })
    expect_true(is.data.frame(tbl))
})

# other single-table verbs

test_that("factorise xdf works",
{
    tbl <- mtc %>% factorise(cyl, gear)
    expect_true(verifyCompositeData(tbl, "tbl_xdf") && varTypes(tbl)["cyl"] == "factor")
})

test_that("arrange xdf works",
{
    tbl <- mtc %>% arrange(cyl, gear)
    expect_true(verifyCompositeData(tbl, "tbl_xdf"))
})

test_that("distinct works",
{
    tbl <- mtc %>% distinct()
    expect_true(verifyCompositeData(tbl, "tbl_xdf"))
    tbl <- mtc %>% distinct(cyl, gear)
    expect_true(verifyCompositeData(tbl, "tbl_xdf"))
})

#test_that("rename works",
#{
    #tbl <- mtc %>% rename(mpg2=mpg)
    #expect_true(verifyCompositeData(tbl, "tbl_xdf") && names(tbl)[1] == "mpg2")
#})


# misc functionality

test_that("output to data.frame works",
{
    tbl <- mtc %>% filter(mpg > 15, cyl <= 6, .outFile=NULL)
    expect_true(is.data.frame(tbl))
    tbl <- mtc %>% select(mpg, cyl, drat, .outFile=NULL)
    expect_true(is.data.frame(tbl))
    tbl <- mtc %>% subset(mpg > 15, c(mpg, cyl, drat), .outFile=NULL)
    expect_true(is.data.frame(tbl))
    tbl <- mtc %>% mutate(mpg2=sin(mpg), wt2=sqrt(wt), .outFile=NULL)
    expect_true(is.data.frame(tbl))
    tbl <- mtc %>% transmute(mpg2=sin(mpg), wt2=sqrt(wt), .outFile=NULL)
    expect_true(is.data.frame(tbl))

    tbl <- mtc %>% summarise(n=n(), mpg2=mean(mpg), .outFile=NULL)
    expect_true(is.data.frame(tbl))

    tbl <- mtc %>% factorise(cyl, gear, .outFile=NULL)
    expect_true(is.data.frame(tbl) && varTypes(tbl)["cyl"] == "factor")
    tbl <- mtc %>% distinct(cyl, gear, .outFile=NULL)
    expect_true(is.data.frame(tbl))
    #tbl <- mtc %>% rename(mpg2=mpg, .outFile=NULL)
    #expect_true(is.data.frame(tbl) && names(tbl)[1] == "mpg2")
})

test_that("output to xdf works",
{
    tbl <- mtc %>% filter(mpg > 15, cyl <= 6, .outFile="test14")
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
    tbl <- mtc %>% select(mpg, cyl, drat, .outFile="test14")
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
    tbl <- mtc %>% subset(mpg > 15, c(mpg, cyl, drat), .outFile="test14")
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
    tbl <- mtc %>% mutate(mpg2=sin(mpg), wt2=sqrt(wt), .outFile="test14")
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
    tbl <- mtc %>% transmute(mpg2=sin(mpg), wt2=sqrt(wt), .outFile="test14")
    expect_true(verifyCompositeData(tbl, "RxXdfData"))

    tbl <- mtc %>% summarise(n=n(), mpg2=mean(mpg), .outFile="test14")
    expect_true(verifyCompositeData(tbl, "RxXdfData"))

    tbl <- mtc %>% factorise(cyl, gear, .outFile="test14")
    expect_true(verifyCompositeData(tbl, "RxXdfData") && varTypes(tbl)["cyl"] == "factor")
    tbl <- mtc %>% distinct(cyl, gear, .outFile="test14")
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
    #tbl <- mtc %>% rename(mpg2=mpg, .outFile="test14")
    #expect_true(verifyCompositeData(tbl, "RxXdfData") && names(tbl)[1] == "mpg2")
})


# cleanup
unlink(c("mtc", "test14"), recursive=TRUE)
