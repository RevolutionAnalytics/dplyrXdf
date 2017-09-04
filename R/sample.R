#' Do random sampling from an Xdf file
#'
#' @param tbl An Xdf file or a tbl wrapping the same.
#' @param size For \code{sample_n}, the number of rows to select. For \code{sample_frac}, the fraction of rows to select. For a grouped dataset, \code{size} applies to each group.
#' @param replace,weight,.env Not used.
#'
#' @details
#' Sampling from Xdf files is slightly more limited than the data frame case. Only unweighted sampling without replacement is supported, and attempts to specify otherwise will result in a warning. Unlike the other single-table dplyr verbs, \code{sample_n} and \code{sample_frac} do not delete tbl inputs; this is because it's unlikely that a sample is intended to replace the input data entirely.
#'
#' Currently sampling on HDFS data works in the local compute context (on the edge node) but not in the Hadoop or Spark compute contexts.
#'
#' @return
#' An Xdf tbl.
#'
#' @seealso
#' \code{\link[dplyr]{sample_frac}}, \code{\link[dplyr]{sample_n}}, \code{\link{sample}}
#'
#' @examples
#' mtx <- as_xdf(mtcars, overwrite=TRUE)
#'
#' tbl <- sample_n(mtx, 10)
#' nrow(tbl)
#'
#' tbl2 <- sample_frac(mtx, 0.5)
#' nrow(tbl2)
#'
#' tbl3 <- group_by(mtx, vs) %>% sample_frac(0.5)
#' nrow(tbl3)
#'
#' # to get an _approximate_ sample, use filter()
#' tbl4 <- filter(mtx, runif(.rxNumRows) < 0.4)  # keep 40% of rows in the data
#' nrow(tbl4)
#' @rdname sample
#' @export
sample_n.RxXdfData <- function(tbl, size=1, replace=FALSE, weight=NULL, .env=NULL)
{
    sampleUngrouped(tbl, size, replace, weight, FALSE)
}


#' @rdname sample
#' @export
sample_frac.RxXdfData <- function(tbl, size=1, replace=FALSE, weight=NULL, .env=NULL)
{
    sampleUngrouped(tbl, size, replace, weight, TRUE)
}


#' @rdname sample
#' @export
sample_n.grouped_tbl_xdf <- function(tbl, size=1, replace=FALSE, weight=NULL, .env=NULL)
{
    grps <- group_vars(tbl)
    sampleGrouped(tbl, size, replace, weight, FALSE) %>%
        simpleRegroup(grps)
}


#' @rdname sample
#' @export
sample_frac.grouped_tbl_xdf <- function(tbl, size=1, replace=FALSE, weight=NULL, .env=NULL)
{
    grps <- group_vars(tbl)
    sampleGrouped(tbl, size, replace, weight, TRUE) %>%
        simpleRegroup(grps)
}


sampleGrouped <- function(.data, size, replace=FALSE, weight=NULL, frac)
{
    stopIfDistribCC("grouped sampling not supported in Hadoop/Spark compute context")
    if(replace)
        warning("sampling with replacement not supported for Xdf files")
    if(!is.null(weight))
        warning("weighted sampling not supported for Xdf files")

    callGroupedExec(.data, tbl_xdf(.data), sampleBase, size, frac)
}


sampleUngrouped <- function(.data, size, replace=FALSE, weight=NULL, frac)
{
    stopIfDistribCC("sampling not supported in Hadoop/Spark compute context")
    if(replace)
        warning("sampling with replacement not supported for Xdf files")
    if(!is.null(weight))
        warning("weighted sampling not supported for Xdf files")

    .tblDir <- get_dplyrxdf_dir()
    .composite <- is_composite_xdf(.data)
    environment(sampleBase) <- environment()
    sampleBase(.data, size, frac)
}


sampleBase <- function(.data, size, frac)
{
    n <- nrow(.data)
    if(frac)
        size <- round(size * n)
    if(size > n)
        stop("sample size must be less than or equal to number of rows in group")
    if(size < 1)
        stop("sample size must be at least 1")
    sel <- sample.int(n, size=size)

    # .tblDir and .composite assumed to exist in eval environment
    file <- tempfile(tmpdir=.tblDir)
    output <- dplyrXdf:::tbl_xdf(.data, file=file, createCompositeSet=.composite)

    rxDataStep(.data, output, rowSelection=(.rxStartRow + seq_len(.rxNumRows) - 1) %in% .sel,
        transformObjects=list(.sel=sel))
}
