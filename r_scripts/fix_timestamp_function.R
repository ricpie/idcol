library(plyr)
library(reshape2)
library(lubridate)
library(tools)
library(data.table)
library(anytime)
library(tcltk)
library(grDevices)

fileCleanerTimeStamp <- function(filerun){
  
  #extract the header. Import differently based on it.
  header <- read.csv(filerun, nrow=19, blank.lines.skip=F, header=F, stringsAsFactor=F)
  header1 <- read.csv(filerun, nrow=20, blank.lines.skip=F, header=F, stringsAsFactor=F)
  header2 <- read.csv(filerun, nrow=21, blank.lines.skip=F, header=F, stringsAsFactor=F)
  if (identical(header1$V1[21],"Unit")) { #For standard ibutton
    headerstart = 19
    header <- read.csv(filerun, nrow=headerstart, blank.lines.skip=F, header=F, stringsAsFactor=F)
    datas <- as.data.table(read.csv(filerun, skip=headerstart, colClasses=c('character', 'character', 'numeric'))[ ,1:3])
    
  } else if  (identical(header2$V2[20],"Unit")) { #For cellphone-downloaded ibutton
    headerstart = 19
    header <- read.csv(filerun, nrow=headerstart, blank.lines.skip=F, header=F, stringsAsFactor=F)
    datas <- as.data.table(read.csv(filerun, skip=headerstart, colClasses=c('character', 'character', 'numeric'))[ ,1:3])
    names(datas) = c("Date.Time","Unit","Value")    
    
  } else if  (identical(header$V1[16],"Unit")) { #For cellphone-downloaded ibutton
    headerstart = 15
    header <- read.csv(filerun, nrow=headerstart, blank.lines.skip=F, header=F, stringsAsFactor=F)
    datas <- as.data.table(read.csv(filerun, skip=headerstart, colClasses=c('character', 'character', 'numeric'))[ ,1:3])
    names(datas) = c("Date.Time","Unit","Value")    
    
  } else if  (identical(header$V2[9],"Unit")) { #For cellphone-downloaded ibutton
    headerstart = 8
    header <- read.csv(filerun, nrow=headerstart, blank.lines.skip=F, header=F, stringsAsFactor=F)
    datas <- as.data.table(read.csv(filerun, skip=headerstart, colClasses=c('character', 'character', 'numeric'))[ ,1:3])
    names(datas) = c("Date.Time","Unit","Value")    
    
  } else { #For Wellzion
    datas = tryCatch({ datas<- as.data.table(read.csv(filerun, sep="\"",fileEncoding="UCS-2LE", header=1, stringsAsFactor=F)) %>%
      dplyr::mutate(Unit = "C") 
    names(datas) = gsub("Thermocouple..","Thermocouple.",names(datas))
    dplyr::rename(datas,Date.Time = Timestamp,Value = Thermocouple.C.) %>%
      dplyr::select("Date.Time","Unit","Value")  
    }, error = function(error_condition) {
      datas<- as.data.table(read.csv(filerun, sep="\"",fileEncoding="UCS-2LE", header=1, stringsAsFactor=F)) %>%
        dplyr::mutate(Unit = "C") 
      datas <- datas[,c(4,6,12)]
      names(datas) = c("Date.Time","Unit","Value")    
      datas
    }, finally={
      datas<- as.data.table(read.csv(filerun, sep="\"",fileEncoding="UCS-2LE", header=1, stringsAsFactor=F)) %>%
        dplyr::mutate(Unit = "C") 
      datas <- datas[,c(4,6,12)]
      names(datas) = c("Date.Time","Unit","Value")
      datas
    })
    
    header$V1 = NULL
    datas <- as.data.table(datas)
    
  }
  
  print(filerun)
  
  #extract the data
  #check for F files
  datas[Unit=="F", Value:=(Value-32) * 5/9]
  datas[Unit=="F", Unit:="C"]
  
  
  #NA-replace data from disconnected values
  datas$Value[datas$Value < -270] <- NA #dplyr::mutate(datas,Value = if_else(Value < 0,0,Value)) #Remove -270 data that sometimes appears due to disconnected Wellzions.
  
  #Look for inverted data, which may be the cae if the thermocouples are connected with the opposite polarity.  
  
  
  #Flip if more than 1% of the data is negative
  # if (quantile(datas$Value,.001) < 0) {
  #   datas$Value <- 40 - datas$Value
  # }

  #Flip this file only. BAS_21035_4_DL1
  if (grepl('BAS_21035_4_DL1',filerun,fixed=TRUE)) {
    datas$Value <- 40 - datas$Value
  }
  
  #Get the number of unique values in the first date position.
  #These functions are pretty slow unfortunately.
  date_string_start_fun <- function(xx,y) {substring(xx,1,sapply(xx, function(x) unlist(gregexpr(y,x,perl=TRUE))[1])-1) }
  date_string_middle_fun <- function(xx,y,start,end) {substring(xx, sapply(xx, function(x) unlist(gregexpr(y,x,perl=TRUE))[start])+1
                                                                ,sapply(xx, function(x) unlist(gregexpr(y,x,perl=TRUE))[end])-1) }
  date_string_end_fun <- function(xx,y) {substring(xx, sapply(xx, function(x) unlist(gregexpr(y,x,perl=TRUE))[2])+1
                                                   ,sapply(xx, function(x) unlist(gregexpr(y,x,perl=TRUE)))-1) }
  
  DT1dash <-length(unique(date_string_start_fun(datas$Date.Time[1],"-")))#Grab first position of the date string assuming it uses a dash delimiter.
  DT2dash <- length(unique(date_string_middle_fun(datas$Date.Time[1],"-",1,2)))#Grab middle position of the date string assuming it uses a dash delimiter.
  DT3dash <- length(unique(date_string_end_fun(datas$Date.Time[1],"-")))#Grab last position of the date string assuming it uses a dash delimiter.
  
  DT1slash <-length(unique(date_string_start_fun(datas$Date.Time[1],"/")))#Grab first position of the date string assuming it uses a dash delimiter.
  DT2slash <- length(unique(date_string_middle_fun(datas$Date.Time[1],"/",1,2)))#Grab middle position of the date string assuming it uses a dash delimiter.
  DT3slash <- length(unique(date_string_end_fun(datas$Date.Time[1],"/")))#Grab last position of the date string assuming it uses a dash delimiter.
  
  slashpresence <- unique((sapply(regmatches(datas[1,Date.Time], gregexpr("/", datas[1,Date.Time])), length)))>1
  dashpresence <- unique((sapply(regmatches(datas[1,Date.Time], gregexpr("-", datas[1,Date.Time])), length)))>1
  #check number of ":" in datetime stamp
  colonpresence <- unique((sapply(regmatches(datas[1,Date.Time], gregexpr(":", datas[1,Date.Time])), length)))
  
  dir.create(path=paste(dirname(filerun),'/Corrected Timestamps',sep=""), showWarnings = FALSE)
  
  options(warn=-1)
  
  # datas <- dplyr::mutate(datas,Value = if_else(Value < 0,quantile(Value, .05),Value)) #Remove -270 data that sometimes appears due to disconnected Wellzions.
  datas$Date.TimeBackup = datas$Date.Time
  #If there are more unique values in DT1 than DT2, then the first value is days. This could be defeated if there is only one day of data. Would also not work if there are a lot of different formats with varying levels of leading zeros.
  newfilename = paste(dirname(filerun),'/Corrected Timestamps/',basename(filerun),sep="")
  if ((dashpresence) & (colonpresence==1) & DT1dash>DT2dash) { #D-M-Y hh:mm
    datas[,Date.Time:=dmy_hm(as.character(Date.Time))]
    
  } else if ((dashpresence) & (colonpresence==2) & DT1dash>DT2dash) {  #D-M-Y hh:mm:ss
    datas[,Date.Time:=dmy_hms(as.character(Date.Time))]
  } else if ((dashpresence) & (colonpresence==1) & DT3dash>DT2dash) {  #Y-m-d hh:mm
    datas[,Date.Time:=ymd_hm(as.character(Date.Time))]
  } else if ((dashpresence) & (colonpresence==2) & DT3dash>DT2dash)  {  #y-m-d hh:mm:ss
    datas[,Date.Time:=ymd_hms(as.character(Date.Time))]
  } else if ((slashpresence) & (colonpresence==2) & DT1slash>=DT2slash)  {  #D/M/Y hh:mm:ss
    datas[,Date.Time:=dmy_hms(as.character(Date.Time))]
  } else if ((slashpresence) & (colonpresence==1) & DT1slash>=DT2slash)  {  #D/M/Y hh:mm
    datas[,Date.Time:=dmy_hm(as.character(Date.Time))]
  } else if ((slashpresence) & (colonpresence==1) & DT2slash>DT1slash)  {  #M/D/Y hh:mm
    datas[,Date.Time:=mdy_hm(as.character(Date.Time))]
  } else if ((slashpresence) & (colonpresence==2) & DT2slash>DT1slash)  {  #M/D/Y hh:mm:ss
    datas[,Date.Time:=mdy_hms(as.character(Date.Time))]
  } else  {
    datas[,Date.Time:=mdy_hms(as.character(Date.Time))]
  }
  headerrows <- data.frame(asd=matrix("", nrow = 19, ncol = 1))
  
  cat(paste(header$V1,headerrows$asd,headerrows$asd,sep=",",collapse="\n"), file=newfilename)
   cat("\n", sep=",", file=newfilename,append=TRUE)
  # if there are a bunch of bad dates still, try to fix the rest.
  if(sum(is.na(datas$Date.Time))>100){
    a <- datas$Date.Time
    b <- dmy_hm(as.character(datas$Date.TimeBackup)) # Produces NA when format is not "%d.%m.%Y"
    a[is.na(a)] <- b[!is.na(b)] # Combine both while keeping their ranks
    datas$Date.Time <- a # Put it back in your dataframe
  }
  datas[,Date.TimeBackup:=NULL]
  datas <- datas[complete.cases(datas), ]
  
  write.table(datas, newfilename, sep=",", append=TRUE, row.names=F, quote=F)
  

  #Plot the data 
  plot_names <- gsub(".csv",".png",newfilename)
  
  
  tryCatch({ 
    datas2 <- datas
    datas2$Date.Time <- parse_date_time(datas2$Date.Time, orders = c("y-m-d HMS","y-m-d HM", "m/d/y HM", "m/d/y HMS"))#,"d/m/y HM","d-m-y HM"))
    
    #Prepare some text for looking at the ratios of high to low temps.
    percentiles <- quantile(datas2$Value,c(.05,.95))
    quantile_rpd <- unname(100*(percentiles[2]-percentiles[1])/percentiles[2])
    cat_string <- paste("min T(C) = ",as.character(percentiles[1]),
                        ", max T(C) = ",as.character(percentiles[2]),
                        "max/min = ", strtrim(as.character(quantile_rpd),5))
    png(filename=plot_names,width = 550, height = 480, res = 100)
    plot(datas2$Date.Time, datas2$Value, main=plot_names,#xaxt = "n",
         type = "p", xlab = cat_string, ylab="Temp (C)",prob=TRUE,cex.main = .6,cex = .5)
    #axis(1, datas2$Date.Time, format(datas2$Date.Time, "%m/%d"), cex.axis = .7)
    grid(nx = 5, ny = 10, col = "lightgray", lty = "dotted",
         lwd = par("lwd"), equilogs = TRUE)
    dev.off()
    
  }, error = function(error_condition) {
    
  }, finally={
    
  })
  
}


fileFlip <- function(fileflip){
  #extract the header
  header <- read.csv(fileflip, nrow=8, blank.lines.skip=F, header=F, stringsAsFactor=F)
  #extract the data
  datas <- as.data.table(read.csv(fileflip, skip=8, colClasses=c('character', 'character', 'numeric')))
  
  datas$Date.Time <- lubridate::ymd_hms(datas$Date.Time ) 
  datas <- dplyr::arrange(datas, Date.Time)
  
  cat(paste(header$V1, "\n"), file=paste(file_path_sans_ext(fileflip),'dateflipped.csv',sep=""))
  write.table(datas, paste(file_path_sans_ext(fileflip),'dateflipped.csv',sep=""), sep=",", append=TRUE, row.names=F, quote=F)
  
}


