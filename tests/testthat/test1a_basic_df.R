mtx <- rxDataStep(mtcars, "misc/mtx.xdf", overwrite=TRUE)


## base - output to data frame

# arrange
deleteXdfTbls()
head(mtx %>% arrange(mpg, disp, .output=NULL))

# distinct
deleteXdfTbls()
head(mtx %>% distinct(cyl, gear, .output=NULL))
# .keep_all will have no effect when dplyr < 0.5 installed
head(mtx %>% distinct(cyl, gear, .keep_all=TRUE, .output=NULL))
head(mtx %>% distinct(cyl, gear, .keep_all=FALSE, .output=NULL))

# do
deleteXdfTbls()
mtx %>% do(m=lm(mpg ~ disp, .), .output=NULL)
mtx %>% doXdf(m=rxLinMod(mpg ~ disp, .), .output=NULL)

# factorise
deleteXdfTbls()
summary(mtx %>% factorise(mpg, cyl, .output=NULL))
summary(mtx %>% factorise(mpg, cyl, .rxArgs=list(sortLevels=TRUE), .output=NULL))

# filter
deleteXdfTbls()
head(mtx %>% filter(mpg > 16, cyl == 8, .output=NULL))

# mutate
deleteXdfTbls()
head(mtx %>% mutate(m2=2*mpg, sw=sqrt(wt), .output=NULL))

# rename
deleteXdfTbls()
head(mtx %>% rename(cc=cyl, mm=mpg, .output=NULL))

# select
deleteXdfTbls()
head(mtx %>% select(mpg, cyl, drat, .output=NULL))

# subset
deleteXdfTbls()
head(mtx %>% subset(cyl==6, c(2,3,4), .output=NULL))
head(mtx %>% subset(cyl==6, c(cyl,disp,hp), .output=NULL))
head(mtx %>% subset_("cyl==6", "c(cyl,disp,hp)", .output=NULL))

# summarise
deleteXdfTbls()
head(mtx %>% summarise(m=mean(mpg), .method=1, .output=NULL))
head(mtx %>% summarise(m=mean(mpg), .method=2, .output=NULL))
head(mtx %>% summarise(m=mean(mpg), .method=3, .output=NULL))
head(mtx %>% summarise(m=mean(mpg), .method=4, .output=NULL))
head(mtx %>% summarise(m=mean(mpg), .method=5, .output=NULL))

# transmute
deleteXdfTbls()
head(mtx %>% transmute(m2=2*mpg, sw=sqrt(wt), .output=NULL))

