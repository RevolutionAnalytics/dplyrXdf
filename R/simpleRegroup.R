simpleRegroup <- function(x, grps=NULL)
{
    if(is.null(grps) ||
       (inherits(x, "RxDataSource") && !inherits(x, "tbl_xdf")))
        x
    else group_by_(x, .dots=grps)
}
