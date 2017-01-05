#' Select columns in a data source
#'
#' @param .data A data source.
#'
#' @details
#' All the special functions mentioned in the documentation for \code{\link[dplyr]{select}} will also work with dplyrXdf. Note that renaming a variable is \emph{not} supported in dplyrXdf \code{select}. If you want to do this, follow the \code{select} with a \code{rename}; the latter is very fast, as it only modifies the metadata portion of an xdf file, not the data itself.
#' Collect brings data back into an R data.frame (stored in a tbl_df). 
#'
#' @return
#' An object of class "tbl_df", "tbl", "data.frame"
#'
#' @seealso
#' \code{\link[dplyr]{collect}} in package dplyr
#' @rdname collect
#' @examples
#' mtcars <- rxDataStep(mtcars, "mtcars.xdf", overwrite=TRUE)
#' class(mtcars)
#' out <- mtcars %>% 
#'   group_by(cyl) %>%
#'   summarise(avg_mpg = mean(mpg)) %>%
#'   collect()
#' out
#' class(out)
#' @export
collect.tbl_xdf <- function(.data){
   out <- RevoScaleR::rxDataStep(.data,outFile = NULL)
   class(out) <- c("tbl_df","tbl","data.frame")
   out
 }
