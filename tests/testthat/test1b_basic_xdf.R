source("misc/srcFiles.r")

#library(dplyrXdf)

mtx <- rxDataStep(mtcars, "misc/mtx.xdf", overwrite=TRUE)


## base - output to data frame

# arrange
deleteXdfTbls()
head(mtx %>% arrange(mpg, disp, .output="misc/test1b.xdf"))

# distinct
deleteXdfTbls()
head(mtx %>% distinct(cyl, gear, .output="misc/test1b.xdf"))
# .keep_all will have no effect when dplyr < 0.5 installed
head(mtx %>% distinct(cyl, gear, .keep_all=TRUE, .output="misc/test1b.xdf"))
head(mtx %>% distinct(cyl, gear, .keep_all=FALSE, .output="misc/test1b.xdf"))

# do
deleteXdfTbls()
mtx %>% do(m=lm(mpg ~ disp, .), .output="misc/test1b.xdf")
mtx %>% doXdf(m=rxLinMod(mpg ~ disp, .), .output="misc/test1b.xdf")

# factorise
deleteXdfTbls()
rxGetVarInfo(mtx %>% factorise(mpg, cyl, .output="misc/test1b.xdf"))[c("mpg", "cyl")]
rxGetVarInfo(mtx %>% factorise(mpg, cyl, .rxArgs=list(sortLevels=TRUE), .output="misc/test1b.xdf"))[c("mpg", "cyl")]

# filter
deleteXdfTbls()
head(mtx %>% filter(mpg > 16, cyl == 8, .output="misc/test1b.xdf"))

# mutate
deleteXdfTbls()
head(mtx %>% mutate(m2=2*mpg, sw=sqrt(wt), .output="misc/test1b.xdf"))

# rename
deleteXdfTbls()
head(mtx %>% rename(cc=cyl, mm=mpg, .output="misc/test1b.xdf"))

# select
deleteXdfTbls()
head(mtx %>% select(mpg, cyl, drat, .output="misc/test1b.xdf"))

# subset
deleteXdfTbls()
head(mtx %>% subset(cyl==6, c(2,3,4), .output="misc/test1b.xdf"))
head(mtx %>% subset(cyl==6, c(cyl,disp,hp), .output="misc/test1b.xdf"))
head(mtx %>% subset_("cyl==6", "c(cyl,disp,hp)", .output="misc/test1b.xdf"))

# summarise
deleteXdfTbls()
head(mtx %>% summarise(m=mean(mpg), .method=1, .output="misc/test1b.xdf"))
head(mtx %>% summarise(m=mean(mpg), .method=2, .output="misc/test1b.xdf"))
head(mtx %>% summarise(m=mean(mpg), .method=3, .output="misc/test1b.xdf"))
head(mtx %>% summarise(m=mean(mpg), .method=4, .output="misc/test1b.xdf"))
head(mtx %>% summarise(m=mean(mpg), .method=5, .output="misc/test1b.xdf"))

# transmute
deleteXdfTbls()
head(mtx %>% transmute(m2=2*mpg, sw=sqrt(wt), .output="misc/test1b.xdf"))

