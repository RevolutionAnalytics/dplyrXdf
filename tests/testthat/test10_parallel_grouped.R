context("Parallel backend")
mtx <- rxDataStep(mtcars, "mtx.xdf", overwrite=TRUE)

verifyData <- function(xdf, expectedClass)
{
    is.data.frame(head(xdf)) && class(xdf) == expectedClass # test for exact class
}

cl <- parallel::makeCluster(3)
doParallel::registerDoParallel(cl)
rxSetComputeContext("dopar")


cc <- rxGetComputeContext()

test_that("set useExecBy works",
{
    dplyrxdf_options(useExecBy=FALSE)
    expect_false(dplyrxdf_options()$useExecBy)
})


test_that("arrange works",
{
    expect_true(verifyData(mtx %>% group_by(gear) %>% arrange(mpg, disp), "grouped_tbl_xdf"))
})

test_that("distinct works",
{
    expect_true(verifyData(mtx %>% group_by(gear) %>% distinct(), "grouped_tbl_xdf"))
    expect_true(verifyData(mtx %>% group_by(gear) %>% distinct(cyl), "grouped_tbl_xdf"))
})

test_that("do works",
{
    expect_true(verifyData(mtx %>% group_by(gear) %>% do(m=lm(mpg ~ disp, .)), "rowwise_df"))
    expect_true(verifyData(mtx %>% group_by(gear) %>% do_xdf(m=rxLinMod(mpg ~ disp, .)), "rowwise_df"))
    expect_true(verifyData(mtx %>% group_by(gear) %>% do(data.frame(mpg2=2 * .$mpg)), "grouped_df"))
    expect_true(verifyData(mtx %>% group_by(gear) %>% do(rxDataStep(., transforms=list(mpg2=2 * mpg))), "grouped_df"))
    expect_true(verifyData(mtx %>% group_by(gear) %>% do_xdf(rxDataStep(., transforms=list(mpg2=2 * mpg))), "grouped_df"))
})

test_that("factorise works",
{
    expect_true(all(varTypes(mtx %>% group_by(gear) %>% factorise(mpg, cyl))[c("mpg", "cyl")] == "factor"))
})

test_that("filter works",
{
    expect_true(verifyData(mtx %>% group_by(gear) %>% filter(mpg > 16, cyl == 8), "grouped_tbl_xdf"))
})

test_that("mutate works",
{
    expect_true(verifyData(mtx %>% group_by(gear) %>% mutate(m2=2 * mpg, sw=sqrt(wt)), "grouped_tbl_xdf"))
})

test_that("rename works",
{
    expect_true(verifyData(mtx %>% group_by(gear) %>% rename(cc=cyl, mm=mpg), "grouped_tbl_xdf"))
})

test_that("select works",
{
    expect_true(verifyData(mtx %>% group_by(gear) %>% select(mpg, cyl, drat), "grouped_tbl_xdf"))
})

test_that("subset works",
{
    expect_true(verifyData(mtx %>% group_by(gear) %>% subset(cyl == 6, c(2, 3, 4)), "grouped_tbl_xdf"))
    expect_true(verifyData(mtx %>% group_by(gear) %>% subset(cyl == 6, c(cyl, disp, hp)), "grouped_tbl_xdf"))
})

test_that("summarise works",
{
    expect_true(verifyData(mtx %>% group_by(gear) %>% summarise(m=mean(mpg), .method=1), "tbl_xdf"))
    expect_true(verifyData(mtx %>% group_by(gear) %>% summarise(m=mean(mpg), .method=2), "tbl_xdf"))
    expect_true(verifyData(mtx %>% group_by(gear) %>% summarise(m=mean(mpg), .method=3), "tbl_xdf"))
    expect_true(verifyData(mtx %>% group_by(gear) %>% summarise(m=mean(mpg), .method=4), "tbl_xdf"))
    expect_true(verifyData(mtx %>% group_by(gear) %>% summarise(m=mean(mpg), .method=5), "tbl_xdf"))
})

test_that("transmute works",
{
    expect_true(verifyData(mtx %>% group_by(gear) %>% transmute(m2=2 * mpg, sw=sqrt(wt)), "grouped_tbl_xdf"))
})

test_that("reset compute context works",
{
    expect_identical(rxGetComputeContext(), cc)
})

rxSetComputeContext("local")
parallel::stopCluster(cl)

# cleanup
file.remove("mtx.xdf")
