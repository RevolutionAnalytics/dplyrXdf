#' @exportClass tbl_xdf
#' @export tbl_xdf
tbl_xdf <- setClass("tbl_xdf", contains="RxXdfData", slots=c(hasTblFile="logical"))


setMethod("initialize", "tbl_xdf", function(.Object, xdf=NULL, file=NULL, createCompositeSet=NULL, ...) {
    fileSystem <- rxGetFileSystem(xdf)
    if(is.null(createCompositeSet))
        createCompositeSet <- isCompositeXdf(xdf)

    if(is.null(file))
    {
        tmpdir <- get_dplyrxdf_dir(fileSystem)
        if(!createCompositeSet)
            file <- tempfile(tmpdir=tmpdir, fileext=".xdf")
        else file <- tempfile(tmpdir=tmpdir)

        if(isHdfs(fileSystem))
        {
            file <- gsub("\\\\", "/", file) # backslash nonsense
            if(!.dxOptions$hdfsWorkDirCreated)
                make_dplyrxdf_dir(fileSystem)
        }
    }
    else file <- validateXdfFile(file, createCompositeSet)

    arglst <- rlang::modify(list(.Object, file=file, fileSystem=fileSystem, createCompositeSet=createCompositeSet),
        !!!list(...))
    .Object <- rlang::invoke("callNextMethod", arglst, .env=parent.frame(), .bury=NULL)
    # hasTblFile should really be isDeletable; misnomer is for back-compatibility
    .Object@hasTblFile <- !file.exists(.Object@file)
    .Object
})


setMethod("coerce", list(from="RxFileData", to="tbl_xdf"), function(from, to, strict=TRUE) {
    out <- tbl_xdf(fileSystem=rxGetFileSystem(from))
    rxImport(from, out, rowsPerRead=.dxOptions$rowsPerRead)
})

