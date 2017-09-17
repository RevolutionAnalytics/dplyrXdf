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
