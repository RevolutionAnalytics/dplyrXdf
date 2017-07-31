#' @exportClass tbl_xdf
setClass("tbl_xdf", contains="RxXdfData", slots=c(hasTblFile="logical"))


# do not call this directly: will not initialise all fields
setMethod("initialize", "tbl_xdf", function(.Object, ...) {
    .Object <- callNextMethod()
    .Object@hasTblFile <- !file.exists(.Object@file)
    .Object
})


#' @export
tbl_xdf <- function(xdf=NULL, file=NULL, createCompositeSet=NULL, fileSystem=rxGetFileSystem(xdf), ...)
{
    if(is.null(createCompositeSet))
    {
        createCompositeSet <- if(inherits(xdf, "RxXdfData"))
            isCompositeXdf(xdf)
        else in_hdfs(fileSystem) # default to TRUE on HDFS, if xdf not supplied
    }

    if(is.null(file))
    {
        tmpdir <- get_dplyrxdf_dir(fileSystem)
        if(is.null(tmpdir))
        {
            set_dplyrxdf_dir(fileSystem=fileSystem)
            tmpdir <- get_dplyrxdf_dir(fileSystem)
        }

        if(!createCompositeSet)
            file <- tempfile(tmpdir=tmpdir, fileext=".xdf")
        else file <- tempfile(tmpdir=tmpdir)

        if(in_hdfs(fileSystem))
        {
            file <- gsub("\\\\", "/", file) # backslash nonsense
            if(!.dxOptions$hdfsWorkDirCreated)
                make_dplyrxdf_dir(fileSystem)
        }
    }
    else file <- validateXdfFile(file, createCompositeSet)

    # call RxXdfData() directly or indirectly to create xdf object
    # this is NOT the S4 class constructor so cannot use callNextMethod()
    xdf <- if(!inherits(xdf, "RxXdfData"))
        RxXdfData(file=file, fileSystem=fileSystem, createCompositeSet=createCompositeSet, ...)
    else modifyXdf(xdf, file=file, fileSystem=fileSystem, createCompositeSet=createCompositeSet, ...)

    xdf <- as(xdf, "tbl_xdf")

    # hasTblFile should really be isDeletable; misnomer is for back-compatibility
    xdf@hasTblFile <- !file.exists(xdf@file)
    xdf
}


setMethod("coerce", list(from="RxFileData", to="tbl_xdf"), function(from, to, strict=TRUE) {
    out <- tbl_xdf(fileSystem=rxGetFileSystem(from))
    rxImport(from, out, rowsPerRead=.dxOptions$rowsPerRead)
})

