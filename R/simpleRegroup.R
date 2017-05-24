simpleRegroup <- function(x, grps=NULL)
{
    #if(length(grps) == 0 ||
       #(inherits(x, "RxDataSource") && !inherits(x, "tbl_xdf")))
        #x
    if(length(grps) > 0 && inherits(x, "tbl_xdf"))
    {
        x <- as(x, "grouped_tbl_xdf")
        x@groups <- grps
    }
    x
}
