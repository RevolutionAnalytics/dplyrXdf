context("Grouped composite Xdf input")
mtc <- RxXdfData("mtc", createCompositeSet=TRUE)
rxDataStep(mtcars, mtc, overwrite=TRUE)

verifyCompositeData <- function(xdf, expectedClass)
{
    isTRUE(xdf@createCompositeSet) && is.data.frame(head(xdf)) && class(xdf) == expectedClass # test for exact class
}


cc <- rxGetComputeContext()

test_that("set useExecBy works",
{
    dplyrxdf_options(useExecBy=FALSE)
    expect_false(dplyrxdf_options()$useExecBy)
})


# basic single-table verbs

test_that("filter works",
{
    tbl <- mtc %>% group_by(gear) %>% filter(mpg > 15, cyl <= 6)
    expect_true(verifyCompositeData(tbl, "grouped_tbl_xdf"))
})

test_that("select works",
{
    tbl <- mtc %>% group_by(gear) %>% select(mpg, cyl, drat)
    expect_true(verifyCompositeData(tbl, "grouped_tbl_xdf"))
})

test_that("subset works",
{
    tbl <- mtc %>% group_by(gear) %>% subset(mpg > 15, c(mpg, cyl, drat))
    expect_true(verifyCompositeData(tbl, "grouped_tbl_xdf"))
})

test_that("mutate works",
{
    tbl <- mtc %>% group_by(gear) %>% mutate(mpg2=sin(mpg), wt2=sqrt(wt))
    expect_true(verifyCompositeData(tbl, "grouped_tbl_xdf"))
})

test_that("transmute works",
{
    tbl <- mtc %>% group_by(gear) %>% transmute(mpg2=sin(mpg), wt2=sqrt(wt))
    expect_true(verifyCompositeData(tbl, "grouped_tbl_xdf"))
})


# summarise and do

test_that("grouped summarise works",
{
    tbl <- mtc %>% group_by(gear) %>% summarise(n=n(), mpg2=mean(mpg))
    expect_true(verifyCompositeData(tbl, "tbl_xdf"))
    tbl <- mtc %>% group_by(gear) %>% summarise(n=n(), mpg2=mean(mpg), .method=1)
    expect_true(verifyCompositeData(tbl, "tbl_xdf"))
    tbl <- mtc %>% group_by(gear) %>% summarise(n=n(), mpg2=mean(mpg), .method=2)
    expect_true(verifyCompositeData(tbl, "tbl_xdf"))
    tbl <- mtc %>% group_by(gear) %>% summarise(n=n(), mpg2=mean(mpg), .method=3)
    expect_true(verifyCompositeData(tbl, "tbl_xdf"))
    tbl <- mtc %>% group_by(gear) %>% summarise(n=n(), mpg2=mean(mpg), .method=4)
    expect_true(verifyCompositeData(tbl, "tbl_xdf"))
    tbl <- mtc %>% group_by(gear) %>% summarise(n=n(), mpg2=mean(mpg), .method=5)
    expect_true(verifyCompositeData(tbl, "tbl_xdf"))
})

test_that("ungrouped do works",
{
    tbl <- mtc %>% group_by(gear) %>% do(m=lm(mpg ~ cyl, data=.), w=lm(wt ~ cyl, data=.))
    expect_true(is.data.frame(tbl))
    tbl <- mtc %>% group_by(gear) %>% do(
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
    tbl <- mtc %>% group_by(gear) %>% factorise(cyl, gear)
    expect_true(verifyCompositeData(tbl, "grouped_tbl_xdf") && varTypes(tbl)["cyl"] == "factor")
})

test_that("arrange xdf works",
{
    tbl <- mtc %>% group_by(gear) %>% arrange(cyl, gear)
    expect_true(verifyCompositeData(tbl, "grouped_tbl_xdf"))
})

test_that("distinct works",
{
    tbl <- mtc %>% group_by(gear) %>% distinct()
    expect_true(verifyCompositeData(tbl, "grouped_tbl_xdf"))
    tbl <- mtc %>% group_by(gear) %>% distinct(cyl, gear)
    expect_true(verifyCompositeData(tbl, "grouped_tbl_xdf"))
})

#test_that("rename works",
#{
#tbl <- mtc %>% group_by(gear) %>% rename(mpg2=mpg)
#expect_true(verifyCompositeData(tbl, "grouped_tbl_xdf") && names(tbl)[1] == "mpg2")
#})


# misc functionality

test_that("output to data.frame works",
{
    tbl <- mtc %>% group_by(gear) %>% filter(mpg > 15, cyl <= 6, .outFile=NULL)
    expect_true(is.data.frame(tbl))
    tbl <- mtc %>% group_by(gear) %>% select(mpg, cyl, drat, .outFile=NULL)
    expect_true(is.data.frame(tbl))
    tbl <- mtc %>% group_by(gear) %>% subset(mpg > 15, c(mpg, cyl, drat), .outFile=NULL)
    expect_true(is.data.frame(tbl))
    tbl <- mtc %>% group_by(gear) %>% mutate(mpg2=sin(mpg), wt2=sqrt(wt), .outFile=NULL)
    expect_true(is.data.frame(tbl))
    tbl <- mtc %>% group_by(gear) %>% transmute(mpg2=sin(mpg), wt2=sqrt(wt), .outFile=NULL)
    expect_true(is.data.frame(tbl))

    tbl <- mtc %>% group_by(gear) %>% summarise(n=n(), mpg2=mean(mpg), .outFile=NULL)
    expect_true(is.data.frame(tbl))

    tbl <- mtc %>% group_by(gear) %>% factorise(cyl, gear, .outFile=NULL)
    expect_true(is.data.frame(tbl) && varTypes(tbl)["cyl"] == "factor")
    tbl <- mtc %>% group_by(gear) %>% distinct(cyl, gear, .outFile=NULL)
    expect_true(is.data.frame(tbl))
    #tbl <- mtc %>% group_by(gear) %>% rename(mpg2=mpg, .outFile=NULL)
    #expect_true(is.data.frame(tbl) && names(tbl)[1] == "mpg2")
})

test_that("output to xdf works",
{
    tbl <- mtc %>% group_by(gear) %>% filter(mpg > 15, cyl <= 6, .outFile="test15")
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
    tbl <- mtc %>% group_by(gear) %>% select(mpg, cyl, drat, .outFile="test15")
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
    tbl <- mtc %>% group_by(gear) %>% subset(mpg > 15, c(mpg, cyl, drat), .outFile="test15")
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
    tbl <- mtc %>% group_by(gear) %>% mutate(mpg2=sin(mpg), wt2=sqrt(wt), .outFile="test15")
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
    tbl <- mtc %>% group_by(gear) %>% transmute(mpg2=sin(mpg), wt2=sqrt(wt), .outFile="test15")
    expect_true(verifyCompositeData(tbl, "RxXdfData"))

    tbl <- mtc %>% group_by(gear) %>% summarise(n=n(), mpg2=mean(mpg), .outFile="test15")
    expect_true(verifyCompositeData(tbl, "RxXdfData"))

    tbl <- mtc %>% group_by(gear) %>% factorise(cyl, gear, .outFile="test15")
    expect_true(verifyCompositeData(tbl, "RxXdfData") && varTypes(tbl)["cyl"] == "factor")
    tbl <- mtc %>% group_by(gear) %>% distinct(cyl, gear, .outFile="test15")
    expect_true(verifyCompositeData(tbl, "RxXdfData"))
    #tbl <- mtc %>% group_by(gear) %>% rename(mpg2=mpg, .outFile="test15")
    #expect_true(verifyCompositeData(tbl, "RxXdfData") && names(tbl)[1] == "mpg2")
})


# cleanup
unlink(c("mtc", "test15"), recursive=TRUE)
