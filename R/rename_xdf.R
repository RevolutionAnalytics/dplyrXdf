#' @export
rename_.RxFileData <- function(.data, ..., .dots)
{
    dots <- lazyeval::all_dots(.dots, ...)

    # identify Revo-specific arguments
    if(any(names(dots) == ".rxArgs"))
    {
        warning("rename doesn't support .rxArgs argument", call.=FALSE)
        dots[[".rxArgs"]] <- NULL
    }

    grps <- groups(.data)
    vars <- rename_vars_(names(.data), dots)

    if(!hasTblFile(.data))
        .data <- tbl(.data, newTblFile())

    names(.data) <- names(vars)
    if(!is.null(grps))  # check if grouping vars were renamed
    {
        renamed <- vars[vars != names(vars)]
        renamed <- grps %in% names(renamed)
        grps[renamed] <- vars[renamed]
        .data <- group_by_(.data, .dots=grps)
    }
    else .data
}


