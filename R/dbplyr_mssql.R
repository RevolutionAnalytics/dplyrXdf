#' SQL Server methods for dplyr/dbplyr
#'
#' Saving tables on SQL Server with dplyr requires creating some custom methods. These are automatically called by \code{compute}, \code{collect} and \code{copy_to}.
#'
#' @param con A DBI connection
#' @param table,name A table name
#' @param sql A query
#' @param temporary Whether to generate a temporary table (which will be automatically prefixed with \code{'##'})
#' @aliases db_compute db_save_query
#' @rdname sql
#' @rawNamespace export("db_compute.Microsoft SQL Server")
`db_compute.Microsoft SQL Server` <- function(con, table, sql, temporary=TRUE, unique_indexes=list(), indexes=list(), ...)
{
    # check that name has prefixed '##' if temporary
    if(temporary && substr(table, 1, 1) != "#")
        table <- paste0("##", table)

    if(!is.list(indexes))
        indexes <- as.list(indexes)

    if(!is.list(unique_indexes))
        unique_indexes <- as.list(unique_indexes)

    db_save_query(con, sql, table, temporary=temporary)
    db_create_indexes(con, table, unique_indexes, unique=TRUE)
    db_create_indexes(con, table, indexes, unique=FALSE)
    table
}


#' @rdname sql
#' @rawNamespace export("db_save_query.Microsoft SQL Server")
`db_save_query.Microsoft SQL Server` <- function(con, sql, name, temporary=TRUE, ...)
{
    # check that name has prefixed '##' if temporary
    if(temporary && substr(name, 1, 1) != "#")
        name <- paste0("##", name)

    tt_sql <- dbplyr::build_sql("select * into ", dbplyr::ident_q(name), " from (", sql, ") ", dbplyr::ident_q(name), con=con)

    DBI::dbExecute(con, tt_sql)
    name
}
