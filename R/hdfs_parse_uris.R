normalizeHdfsPaths <- function(paths)
{
    sapply(paths, normalizeHdfsPath)
}


normalizeHdfsPath <- function(path)
{
    # remove stray .'s
    path <- gsub("/\\./", "/", convertBS(path))
    path <- sub("^\\./", "", path)

    uri <- parseUrl(path)

    if(is.null(uri$host))
    {
        userDir <- getHdfsUserDir()
        if(uri$path == ".")
            uri$path <- userDir
        else if(nchar(uri$path) > 0 && substr(uri$path, 1, 1) != "/")
            uri$path <- file.path(userDir, uri$path, fsep="/")
    }

    if(!is.null(uri$scheme) && !is.null(uri$host))
        buildUrl(uri$scheme, uri$host, uri$path, uri$port)
    else uri$path
}


makeHdfsUri <- function(host, path)
{
    if(inherits(host, "RxHdfsFileSystem"))
        host <- host$hostName
    path <- convertBS(path)
    if(hasUriScheme(host) && !hasUriScheme(path))
        gsub("(?<!:)//", "/", file.path(host, path, fsep="/"), perl=TRUE)
    else path
}


# remove backslash cruft: can sometimes stray over on Windows
convertBS <- function(path)
{
    gsub("\\\\", "/", path)
}


hasUriScheme <- function(path)
{
    if(length(path) > 0)
        grepl("^([[:alpha:]+.-]+):", path)
    else FALSE
}



# based on httr::parse_url and httr::build_url
parseUrl <- function(url)
{
    stopifnot(length(url) == 1)

    extract <- function(pattern)
    {
        if(!grepl(pattern, url))
            return(NULL)
        m <- regexpr(pattern, url, perl=TRUE)
        piece <- substr(url, attr(m, "capture.start"), attr(m, "capture.length") + attr(m, "capture.start") - 1L)
        regmatches(url, m) <<- ""
        piece
    }
    fragment <- extract("#(.*)$")
    scheme <- extract("^([[:alpha:]+.-]+):")
    netloc <- extract("^//([^/?]*)/?")

    if(identical(netloc, ""))
    {
        url <- paste0("/", url)
        port <- hostname <- NULL
    }
    else if(!is.null(netloc))
    {
        pieces <- strsplit(netloc, "@")[[1]]
        hostname <- if(length(pieces) == 1) pieces else pieces[2]

        hostname <- strsplit(hostname, ":")[[1]]
        port <- if(length(hostname) > 1) hostname[2] else NULL
        hostname <- hostname[1]
    }
    else port <- hostname <- NULL

    list(scheme=scheme, hostname=hostname, path=url, port=port)
}


buildUrl <- function(scheme, hostname, path, port=NULL)
{
    if(!is.null(port))
        port <- paste0(":", port)
    path <- paste(gsub("^/", "", path), collapse="/")
    paste0(scheme, "://", hostname, port, "/", path)
}
