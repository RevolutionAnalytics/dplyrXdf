#' Do random sampling from an Xdf file
#'
#' @param tbl An Xdf file or a tbl wrapping the same.
#' @param size For \code{sample_n}, the number of rows to select. For \code{sample_frac}, the fraction of rows to select. For a grouped dataset, \code{size} applies to each group.
#' @param replace,weight,.env Not used.
#'
#' @details
#' Sampling from Xdf files is slightly more limited than the data frame case. Only unweighted sampling without replacement is supported, and attempts to specify otherwise will result in a warning.
#'
#' @seealso
#' \code{\link[dplyr]{sample_frac}}, \code{\link[dplyr]{sample_n}}, \code{\link{sample}}
#' @rdname sample
#' @export
sample_n.RxXdfData <- function(tbl, size=1, replace=FALSE, weight=NULL, .env=NULL)
{
    if(replace)
        warning("sampling with replacement not supported for Xdf files")
    if(!is.null(weight))
        warning("weighted sampling not supported for Xdf files")

    sampleBase(tbl, size, FALSE)
}


#' @rdname sample
#' @export
sample_frac.RxXdfData <- function(tbl, size=1, replace=FALSE, weight=NULL, .env=NULL)
{
    if(replace)
        warning("sampling with replacement not supported for Xdf files")
    if(!is.null(weight))
        warning("weighted sampling not supported for Xdf files")

    sampleBase(tbl, size, TRUE)
}


#' @rdname sample
#' @export
sample_n.grouped_tbl_xdf <- function(tbl, size=1, replace=FALSE, weight=NULL, .env=NULL)
{
    grps <- group_vars(tbl)
    sampleGroupedXdf(tbl, size, replace, weight, FALSE) %>%
        combineGroups(tbl_xdf(tbl), grps)
}


#' @rdname sample
#' @export
sample_frac.grouped_tbl_xdf <- function(tbl, size=1, replace=FALSE, weight=NULL, .env=NULL)
{
    grps <- group_vars(tbl)
    sampleGroupedXdf(tbl, size, replace, weight, TRUE) %>%
        combineGroups(tbl_xdf(tbl), grps)
}


sampleGroupedXdf <- function(.data, size, replace=FALSE, weight=NULL, frac)
{
    if(replace)
        warning("sampling with replacement not supported for Xdf files")
    if(!is.null(weight))
        warning("weighted sampling not supported for Xdf files")

    callFunc <- if(useExecBy(.data)) callExecBy else callSplit

    callFunc(.data, sampleBase, size, frac)
}


sampleBase <- function(.data, size, frac, .composite=isCompositeXdf(.data), .tblDir=get_dplyrxdf_dir())
{
    n <- nrow(.data)
    if(frac)
        size <- round(size * n)
    if(size > n)
        stop("sample size must be less than or equal to number of rows in group")
    if(size < 1)
        stop("sample size must be at least 1")
    sel <- sample.int(n, size=size)

    file <- tempfile(tmpdir=.tblDir)
    # explicit namespace reference to allow for parallel/execBy backends
    output <- dplyrXdf:::tbl_xdf(.data, file=file, createCompositeSet=.composite)

    rxDataStep(.data, unTbl(output), rowSelection=(.rxStartRow + seq_len(.rxNumRows) - 1) %in% .sel,
        transformObjects=list(.sel=sel))
    output
}
