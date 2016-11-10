simpleRegroup <- function(x, grps=NULL)
{
    if(length(grps) == 0 ||
       (inherits(x, "RxDataSource") && !inherits(x, "tbl_xdf")))
        x
    else group_by_(x, .dots=grps)
}
