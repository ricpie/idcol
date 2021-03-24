# fwrite(meta_data, file="r_scripts/upas_dummy_meta_data.csv") 
# file = file_list[1]


# fwrite(meta_data, file="r_scripts/upas_dummy_meta_data.csv") 
# file = file_list[1]

#Tidy version


UPAS_qa_tidy <- function(upas_data){
  
  upas_data_meta <- upas_data %>% 
    dplyr::group_by(file) %>% 
    mutate(basename = file_path_sans_ext(basename(file)),
           basename = gsub(' ','',basename) ,) %>%
    tidyr::separate(basename,into=c("datestart","hhid","sampletype","loggerID","filterID","flag"), sep = "_",fill="warn") %>%
    mutate(datestart = ymd(datestart)) %>%
    select(-SampledVolume,-StartDateTimeUTC) %>%
    left_join(rbindlist(lapply(file_list_upas,read_upas_header),fill = TRUE) %>%
                select(-c("VolumetricFlowRate","UPASlogFilename", "UPASserial", "UPASfirmware", "SampleName", "CartridgeID", "LogFileMode")),
              by="file",
              all.x=TRUE) %>%
    dplyr::group_by(SampleName) %>%
    dplyr::mutate(DateTimeLocal=as.POSIXct(DateTimeLocal,format='%Y-%m-%dT%H:%M:%S',tz=local_tz),
                  DateTimeUTC=as.POSIXct(DateTimeUTC,format='%Y-%m-%dT%H:%M:%S'),
                  datetime_start = min(DateTimeLocal),
                  datetime_end = max(DateTimeLocal),
                  qa_date = as.Date(Sys.Date()),
                  inletp_flag = ifelse(quantile(PumpP,.95)>1200 , 1, 0),
                  temp_flag = ifelse(!quantile(PCBT,.5) %between% temp_thresholds, 1, 0),
                  rh_flag = ifelse(quantile(PumpRH,.5) > rh_threshold , 1, 0),
                  AverageVolumetricFlowRate = round(mean(VolumetricFlowRate, na.rm=T),3),
                  flow_sd = round(sd(VolumetricFlowRate, na.rm=T), 3),
                  flow_5th_percentile =quantile(VolumetricFlowRate, 0.05,na.rm=T),
                  flow_95th_percentile =quantile(VolumetricFlowRate, 0.95,na.rm=T),
                  percent_flow_deviation = round(length(!(VolumetricFlowRate %between% upas_flow_thresholds))/n(),2),
                  flow_flag = if(percent_flow_deviation>flow_cutoff_threshold | 
                                  is.na(percent_flow_deviation) | 
                                  flow_5th_percentile<upas_flow_thresholds[1] | 
                                  flow_95th_percentile>upas_flow_thresholds[2]){1}else{0},
                  dur_flag = ifelse(SampledRuntime %between% (48*c(.8,1.2)),0,1),
                  gps_lat_med = median(GPSlat,na.rm = T),
                  gps_lon_med = median(GPSlon,na.rm = T),
                  shutdown_flag = ifelse(ShutdownMode %like% '1|3',0,1),
                  qc = ifelse(shutdown_flag==1 | flow_flag == 1 ,'bad','good')) %>%
    dplyr::ungroup() 
  
  return(upas_data_meta)
}





dummy_meta_data <- fread('r_scripts/upas_dummy_meta_data.csv')

UPAS_ingest_fun <- function(file, output=c('raw_data', 'meta_data'),local_tz, dummy='dummy_meta_data'){
  
  dummy_meta_data <- get(dummy, envir=.GlobalEnv)
  
  #separate out raw, meta, and sensor data
  raw_data=read.table(file, header=T,quote = "",sep=",",skip=59,na.string=c("","null","NaN")) %>%
    dplyr::mutate(SampleTime = gsub('"',"",X.SampleTime),
                  SampleTime=as.POSIXct(SampleTime,format='%H:%M:%S',tz=local_tz)) %>%
    dplyr::mutate(DateTimeLocal=as.POSIXct(DateTimeLocal,format='%Y-%m-%dT%H:%M:%S',tz=local_tz)) %>%
    dplyr::mutate(DateTimeUTC=as.POSIXct(DateTimeUTC,format='%Y-%m-%dT%H:%M:%S')) %>%
    dplyr::select(-X.SampleTime)
  
  raw_data = as.data.table(raw_data)
  
  # setnames(raw_data, c("SampleTime","UnixTime","DateTimeUTC","datetime","VolumetricFlowRate","SampledVolume","PumpT","PCBT","FdpT","PumpP","PCBP","FdPdP","PumpRH",  "AtmoRho", "PumpPow1","PumpPow2","PumpV",   "MassFlow","BFGvolt","BFGenergy","GPSlat","GPSlon","GPSalt","GPSsat","GPSspeed","GPShdop" ))
  
  meta_data_in <- read_upas_header(file, update_names=FALSE) 
  
  filename <- parse_filename_fun_cscb(file)
  
  if(all(is.na(filename$hhid))){
    #add "fake" meta_data
    meta_data <- dummy_meta_data
    meta_data$file <- file
    meta_data$qa_date <- as.Date(Sys.Date())
    meta_data$flags <- 'No usable data'
    meta_data$datestart <- filename$datestart
    meta_data$datetime_start <- NA
    meta_data[, c('smry', 'analysis') := NULL]
    return(list(raw_data=NULL, meta_data=meta_data))
  }else{
    meta_data <- data.table(
      file=filename$fullname,
      hhid=filename$hhid,
      filterID=filename$filterID,
      sampletype=filename$sampletype, 
      fieldworkerID=filename$fieldworkerID, 
      fieldworkernum=filename$fieldworkernum, 
      HHID=filename$HHID,
      datestart = filename$datestart,
      datetime_start = raw_data$datetime[1],
      loggerID=filename$loggerID,
      shutdown_reason=meta_data_in$ShutdownReason,
      StartBatteryCharge=meta_data_in$StartBatteryCharge,
      ProgrammedRuntime_hrs=meta_data_in$ProgrammedRuntime/60/60,
      dutycycle=meta_data_in$DutyCycle,
      AverageVolumetricFlowRate = meta_data_in$AverageVolumetricFlowRate,
      RunTimeHrs=as.numeric(meta_data_in$LoggedRuntime),
      software_vers = meta_data_in$UPASfirmware,
      filterID_upas = meta_data_in$SampleName,
      app_version = meta_data_in$AppVersion,
      qa_date = as.Date(Sys.Date())
    )
  }
  
  if(all(output=='meta_data')){return(meta_data)}else
    if(all(output=='raw_data')){return(raw_data)}else
      if(all(output == c('raw_data', 'meta_data'))){
        return(list(meta_data=meta_data, raw_data=raw_data))}
}


UPAS_qa_fun <- function(file, local_tz){
  ingest <- UPAS_ingest_fun(file, output=c('raw_data', 'meta_data'),local_tz)
  filename <- parse_filename_fun_cscb(file)
  
  if(is.null(ingest)){return(NULL)}else{
    
    meta_data <- ingest$meta_data
    if('flags' %in% colnames(meta_data) | file.info(file)$size <3000){
      return(meta_data)
    }else{
      raw_data <- ingest$raw_data
      raw_data_long <- melt(raw_data[, c(3,4, 5, 8, 10, 12, 13, 14, 19:24)], id.var='DateTimeLocal')
      # setnames(raw_data, c("SampleTime","UnixTime","DateTimeUTC","datetime","VolumetricFlowRate","SampledVolume","PumpT","PCBT","FdpT","PumpP","PCBP","FdPdP","PumpRH",  "AtmoRho", "PumpPow1","PumpPow2","PumpV",   "MassFlow","BFGvolt","BFGenergy","GPSlat","GPSlon","GPSalt","GPSsat","GPSspeed","GPShdop" ))
      
      #create a rounded dt variable
      raw_data[, round_time:=round_date(DateTimeLocal, 'hour')]
      
      #inlet pressure flag
      inletp_flag <- if(raw_data_long[variable=='PumpP' & !is.na(value), any(value>1200)]){1}else{0}
      #temperature range flag
      temp_flag <- if(raw_data_long[variable=='PCBT' & !is.na(value), any(!(value %between% temp_thresholds))]){1}else{0}
      rh_flag <- if(!is.na(raw_data_long[variable=='PumpRH' & !is.na(value), (mean(value)>rh_threshold)])&raw_data_long[variable=='PumpRH' & !is.na(value), (mean(value)>rh_threshold)]){1}else{0}
      #mean flow rate, flows outside of range
      flow_mean <- round(raw_data[, mean(VolumetricFlowRate, na.rm=T)],3)
      flow_sd <- round(raw_data[, sd(VolumetricFlowRate, na.rm=T)], 3)
      flow_5th_percentile <- raw_data[, quantile(VolumetricFlowRate, 0.05,na.rm=T)]
      flow_95th_percentile <- raw_data[, quantile(VolumetricFlowRate, 0.95,na.rm=T)]
      percent_flow_deviation <- round(length(raw_data[!(VolumetricFlowRate %between% upas_flow_thresholds), VolumetricFlowRate])/dim(raw_data)[1],2)
      flow_flag <- if(percent_flow_deviation>flow_cutoff_threshold | is.na(percent_flow_deviation) | flow_5th_percentile<upas_flow_thresholds[1] | flow_95th_percentile>upas_flow_thresholds[2]){1}else{0}
      filename_flag <- filename$filename_flag
      
      #sample duration
      sample_duration_days <- raw_data_long[!is.na(value), as.numeric(difftime(max(DateTimeLocal), min(DateTimeLocal), units='days'))]
      datetime_start <- min(raw_data_long$DateTimeLocal)
      datetime_end <- max(raw_data_long$DateTimeLocal)
      gps_lat_med <- median(raw_data$GPSlat)
      gps_lon_med <- median(raw_data$GPSlon)
      if(meta_data$shutdown_reason=='1' | meta_data$shutdown_reason =='3'){shutdown_flag=0}else{shutdown_flag=1}
      
      #overall flag
      if(shutdown_flag==1 | flow_flag == 1 | filename_flag ==1){qc='bad'}else{qc='good'}
      
      meta_data = cbind(meta_data,datetime_start,datetime_end,sample_duration_days,gps_lat_med,gps_lon_med,flow_mean,flow_sd,flow_5th_percentile,flow_95th_percentile,percent_flow_deviation,flow_flag,inletp_flag,temp_flag,rh_flag,shutdown_flag,filename_flag,qc)
      
      meta_data[, flag_total:=sum(flow_flag,inletp_flag,temp_flag,rh_flag,shutdown_flag,filename_flag), by=.SD]
      meta_data[, c('smry', 'analysis') := TRUE]
      
      return(meta_data)
    }
  }
}


