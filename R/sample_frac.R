#' Random sample from a xdf object
#'
#' @param .data A tbl for an Xdf data source; or a raw Xdf data source.
#' @param size the fraction of rows to sample
#' @param replace default to FALSE. \code{replace=TRUE} now allowed when working on chunks of data.   
#' @param weight default to NULL. weighted sampling now allowed when working on chunks of data.   
#' @param ... other parameters to be passed to \code{RevoScaleR::rxDataStep}
#'
#' @return A tbl with an approximate fraction of rows as set by \cod{size} parameter. 
#'
#' @seealso
#' \code{\link[dplyr]{sample_frac}} in package dplyr
#' 
#' @rdname sample_frac
#' 
#' @examples 
#' mtcarsXdf <- rxDataStep(mtcars )
#' sample_frac(mtcarsXdf, .1)
#' @export
sample_frac.tbl_xdf <- function(.data, size  = 1, replace = FALSE, weight = NULL, ...){
  
  if ( size > 1 | size < 0 ) stop('Sample fraction must be greater than zero and less or equal to one')
  if ( replace) stop('Sampling with replacement is not allowed when working on chunks of data')
  if ( !is.null(weight)) stop('Weigthed sampling is not allowed when working on chunks of data')
  
  
  rxDataStep(inData = .data, 
             rowSelection = as.logical(rbinom(.rxNumRows, 1 , size)), 
             transformEnvir = environment(), ...)
}

