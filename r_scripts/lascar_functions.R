# fwrite(meta_data, file="r_scripts/lascar_dummy_meta_data.csv") 
dummy_meta_data <- fread('r_scripts/lascar_dummy_meta_data.csv')

lascar_ingest <- function(file, output=c('raw_data', 'meta_data'), local_tz="Asia/Dhaka",dummy='dummy_meta_data',lascar_cali_coefs="lascar_cali_coefs"){
  dummy_meta_data <- get(dummy, envir=.GlobalEnv)
  badfileflag <- 0
  
  base::message(file, " QA-QC checking in progress")
  
  meta_data <- dummy_meta_data
  meta_data$file <- file
  meta_data$qa_date = as.Date(Sys.Date())
  meta_data$flags <- 'No usable data'
  suppressWarnings(meta_data[, c('smry', 'analysis') := NULL])
  
  #separate out raw, meta, and sensor data
  raw_data_temp <- fread(file, skip = 0,sep=",",fill=TRUE)
  if (nrow(raw_data_temp)<3 || ncol(raw_data_temp)<4){
    badfileflag <- 1; slashpresence <- 1
    return(list(raw_data=NULL, meta_data=meta_data))
  } else{
    
    #Handle whether the data is from a Lascar or PATS+
    raw_data <- fread(file, skip = 2,sep=",",fill=TRUE)
    if (raw_data[1,1]=="HW Version"){
      raw_data <- raw_data[30:dim(raw_data)[1],c(1,1,7)]
    } else {raw_data<- raw_data[,1:3]}
    setnames(raw_data, c("SampleNum", "datetime", "CO_raw"))
    raw_data<-raw_data[complete.cases(raw_data), ]
    raw_data$CO_raw = as.numeric(raw_data$CO_raw)
    
    #since samples are short, simply "guessing" doesn't work -- for example, some files were taken between 8/8 and 8/10.
    #Use a simple algorithm to determine, of the three DT options below, which one has the smallest overall range
    date_formats <- suppressWarnings(melt(data.table(
      dmy = raw_data[, as.numeric(difftime(max(dmy_hms(datetime)), min(dmy_hms(datetime)),units = "days"))],
      ymd = raw_data[, as.numeric(difftime(max(ymd_hms(datetime)), min(ymd_hms(datetime)),units = "days"))],
      mdy = raw_data[, as.numeric(difftime(max(mdy_hms(datetime)), min(mdy_hms(datetime)),units = "days"))]
    )))
    
    #Clunkily deal with this date format
    slashpresence <- unique((sapply(regmatches(raw_data[,datetime], gregexpr("/", raw_data[,datetime])), length)))>1
    
    #Some files don't have seconds in the timestamps... also clunky but oh well.
    semicolons <- unique((sapply(regmatches(raw_data[,datetime], gregexpr(":", raw_data[,datetime])), length)))<2
    if (semicolons==TRUE) {raw_data[, datetime:=paste0(datetime,":00")]}
    
    if(all(is.na(date_formats$value)) || badfileflag==1){
      base::message(basename(file), " has no parseable date.")
      #add "fake" meta_data
      meta_data <- dummy_meta_data
      meta_data$fullname = file
      meta_data$basename = basename(file)
      meta_data$qa_date = as.Date(Sys.Date())
      meta_data$flags <- 'No usable data'
      meta_data$qc <- "bad"
      meta_data[, c('smry', 'analysis') := NULL]
      return(list(raw_data=NULL, meta_data=meta_data))
    }else{
      
      # #deal with dates and times
      # raw_data[, datetime:=as.POSIXct(datetime)]
      
      date_format <- as.character(date_formats[value==date_formats[!is.na(value),min(value)], variable])
      
      if(slashpresence==1){raw_data[, datetime:=dmy_hms(as.character(datetime), tz=local_tz)]} else
        if(date_format=="mdy"){raw_data[, datetime:=mdy_hms(as.character(datetime), tz=local_tz)]} else
          if(date_format=="ymd"){raw_data[, datetime:=ymd_hms(as.character(datetime), tz=local_tz)]} else
            if(date_format=="dmy"){raw_data[, datetime:=dmy_hms(as.character(datetime), tz=local_tz)]}
      
      
      #Sampling duration
      dur = difftime(max(raw_data$datetime),min(raw_data$datetime),units = 'days')
      dur_minutes = difftime(max(raw_data$datetime),min(raw_data$datetime),units = 'mins')
      samplerate_minutes = length(raw_data$datetime)/as.numeric(dur_minutes)
      
      filename = as.data.frame(file) %>% 
        dplyr::mutate(file = file_path_sans_ext(basename(as.character(file)))) %>% 
        tidyr::separate(file,
          c("HHID","sampletype","loggerID","date"))
      
      meta_data <- data.table(
        fullname=file,
        basename=basename(file),
        qa_date = as.Date(Sys.Date()),
        datetime_start = raw_data$datetime[1],
        sampletype=filename$sampletype,
        HHID=filename$HHID,
        loggerID=filename$loggerID,
        file_date = filename$date,
        samplerate_minutes = samplerate_minutes,
        sampling_duration = dur
      )
      
      #Add some meta_data into the mix
      raw_data[,datetime := floor_date(datetime, unit = "minutes")]
      raw_data[,loggerID := meta_data$loggerID]
      raw_data[,HHID := meta_data$HHID]
      raw_data[,sampletype := meta_data$sampletype]
      
      
      if(all(output=='meta_data')){return(meta_data)}else
        if(all(output=='raw_data')){return(raw_data)}else
          if(all(output == c('raw_data', 'meta_data'))){
            return(list(meta_data=meta_data, raw_data=raw_data))
          }
    }  
  }
}

# file = file_list_lascar[1]
lascar_qa_fun <- function(file, setShiny=TRUE,output= 'meta_data',local_tz="Asia/Dhaka",lascar_cali_coefs="lascar_cali_coefs"){
  ingest = tryCatch({
    ingest <- lascar_ingest(file, output=c('raw_data', 'meta_data'),local_tz="Asia/Dhaka",dummy='dummy_meta_data',lascar_cali_coefs = "lascar_cali_coefs")
  }, error = function(e) {
    print('error ingesting')
    ingest = NULL
  })
  
  if(is.null(ingest)){return(NULL)}else{
    
    sample_duration_thresholds = c(1296,1584)
    
    meta_data <- ingest$meta_data
    if('flags' %in% colnames(meta_data)){
      return(meta_data)
    }else{
      raw_data <- ingest$raw_data
      calibrated_data <- as.data.table(apply_lascar_calibration(meta_data$loggerID,raw_data,lascar_cali_coefs))
      
      #create a rounded dt variable
      my_breaks=seq(round_date(min(calibrated_data$datetime,na.rm=TRUE),unit="1 minutes"), 
                       round_date(max(calibrated_data$datetime,na.rm=TRUE),unit="1 minutes"),
                       by = '1 min')
      calibrated_data[,datetime := as.POSIXct(cut(datetime,my_breaks, tz=local_tz))]
      calibrated_data = calibrated_data[complete.cases(calibrated_data),]
      #Filter the data based on actual start and stop times - once I get them!
      # calibrated_data <- calibrated_data[ecm_tags=='deployed']
      
     
      #Calculate daily average concentration flag
      Daily_avg <- mean(calibrated_data$CO_ppm,na.rm = TRUE)
      Daily_sd <- sd(calibrated_data$CO_ppm,na.rm = TRUE)
      
      
      #sample duration
      sample_duration <- as.numeric(difftime(max(calibrated_data$datetime), min(calibrated_data$datetime), units='days'))
      sample_duration_flag <- if(sample_duration < sample_duration_thresholds[1]/1440){1}else{0}
      
      meta_data = cbind(meta_data,Daily_sd,sample_duration_flag)
      
      #Plot the CO data and save it
      tryCatch({ 
        #Prepare some text for looking at the ratios of high to low temps.
        plot_name = gsub(".txt",".png",basename(file))
        plot_name = paste0("~/Dropbox/IDCOL Bangladesh (shared)/Data/QA Reports/Instrument Plots/Lascar_",gsub(".csv",".png",plot_name))
        # plot_namez = paste0("QA Reports/Instrument Plots/Lascar_",gsub(".csv|.txt","_Zoom.png",basename(file)))
        percentiles <- quantile(calibrated_data$CO_ppm,c(.05,.95))
        cat_string <- paste("5th % CO (ppm) = ",as.character(percentiles[1]),
                            ", 95th % CO (ppm)  = ",as.character(percentiles[2]))
        if(!file.exists(plot_name)){
          png(filename=plot_name,width = 550, height = 480, res = 100)
          plot(calibrated_data$datetime, calibrated_data$CO_ppm, main=plot_name,
               type = "p", xlab = cat_string, ylab="Calibrated CO (ppm)",prob=TRUE,cex.main = .6,cex = .5)
          grid(nx = 5, ny = 10, col = "lightgray", lty = "dotted",
               lwd = par("lwd"), equilogs = TRUE)
          axis(3, calibrated_data$datetime, format(calibrated_data$datetime, "%b %d %y"), cex.axis = .7)
          
          dev.off()
          
          # png(filename=plot_namez,width = 550, height = 480, res = 100)
          # cookingstart = as.numeric(match("cooking",calibrated_data$emission_tags))
          # datasubset = calibrated_data[cookingstart:(cookingstart+200),]
          # plot(datasubset$datetime, datasubset$CO_ppm, main=plot_name,
          #      type = "p", xlab = cat_string, ylab="Calibrated CO (ppm)",prob=TRUE,cex.main = .6,cex = .5)
          # axis(3, datasubset$datetime, format(datasubset$datetime, "%b %d %y"), cex.axis = .7)
          # grid(nx = 5, ny = 10, col = "lightgray", lty = "dotted",
          #      lwd = par("lwd"), equilogs = TRUE)
          # dev.off()
          
        }
      }, error = function(error_condition) {
      }, finally={})
      
      return(meta_data)
    }
  }
}


apply_lascar_calibration <- function(loggerIDval,raw_data,lascar_cali_coefs='lascar_cali_coefs') {
  logger_cali <- as.data.table(lascar_cali_coefs)[LascarID==loggerIDval,]
  if (!nrow(logger_cali)){
    logger_cali <- data.table(
      loggerID = NA,
      COslope = 1,
      COzero = 0,
      R2 = NA,
      instrument = NA)
  }
  calibrated_data <- dplyr::mutate(raw_data,CO_ppm = CO_raw*logger_cali$COslope )
  calibrated_data
}

lascar_cali_fun <- function(file,local_tz="Asia/Dhaka",lascar_cali_coefs='lascar_cali_coefs',output='calibrated_data'){
  
  ingest = tryCatch({
    ingest <- lascar_ingest(file, output=c('raw_data', 'meta_data'),local_tz="Asia/Dhaka",dummy='dummy_meta_data')
  }, error = function(e) {
    print('error ingesting')
    ingest = NULL
  })
  
  if(is.null(ingest)){return(NULL)}else{
    
    meta_data <- ingest$meta_data
    if('flags' %in% colnames(meta_data)){
      return(meta_data)
    }else{
      raw_data <- ingest$raw_data
      calibrated_data <- as.data.table(apply_lascar_calibration(meta_data$loggerID,raw_data,lascar_cali_coefs))
      
      #Add a '2' if it is a PATS+.  Possible for there to be multiple duplicates.. K, K2, K22, etc.
      if(grepl("LAS|CAS",meta_data$loggerID)){ 
        calibrated_data[,sampletype := meta_data$sampletype]
      }else {calibrated_data[,sampletype := paste0(meta_data$sampletype,"2")]}
      
      return(calibrated_data)
    }
  }
}

