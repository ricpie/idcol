library(plyr)
library(reshape2)
library(lubridate)
library(tools)
library(data.table)

fileCleaner <- function(filerunthermo){
  #extract the header
  header <- read.csv(filerunthermo, nrow=19, blank.lines.skip=F, header=F, stringsAsFactor=F)
  #extract the data
  datas <- as.data.table(read.csv(filerunthermo, skip=19, colClasses=c('character', 'character', 'numeric')))
  if (ncol(datas) == 4) {
      colnames(datas) <- c("Date.Time", "Unit", "Value",NA)
  }   else{
      colnames(datas) <- c("Date.Time", "Unit", "Value")
  }
      
  # Filter out -270 values and values greater than 1000...they represent bad connections in the wires.
  datas <- dplyr::filter(datas,Value>0 & Value<1000)
  
  parse_order = c("y-m-d HMS", "m/d/y HMS p!","m/d/y HM p!","m/d/y HMS","m/d/y HM")
  # If units are not in the second column, combine date and time columns
  if (datas$Unit[1] != "C" & !grepl("DMY",filerunthermo, fixed=TRUE)) {
    datas <- dplyr::mutate(datas,Date.Time = parse_date_time(paste(Date.Time, Unit),
                            orders = parse_order)) %>%
                            dplyr::mutate(Unit = "C") %>%
                            dplyr::select(Date.Time,Unit,Value)

  } else if (datas$Unit[1] != "C" & grepl("DMY",filerunthermo, fixed=TRUE)) {
    datas <- dplyr::mutate(datas,Date.Time = parse_date_time(paste(Date.Time, Unit),
                            orders =  c("d-m-y HM p!", "d-m-y HMS p!","d/m/y HM p!",
                                  "d/m/Y! HM p!","d/m/Y! HMS p!","d/m/y HM","d/m/y HMS","d-m-y HM"))) %>%
                            dplyr::mutate(Unit = "C") %>%
                            dplyr::select(Date.Time,Unit,Value)
    
  } else if (grepl("DMY",filerunthermo, fixed=TRUE)) {
    datas <- dplyr::mutate(datas,Date.Time = parse_date_time(paste(Date.Time, Unit),
                            orders =  c("d-m-y HM p!", "d-m-y HMS p!","d/m/y HM p!",
                                                        "d/m/Y! HM p!","d/m/Y! HMS p!","d/m/y HM","d-m-y HM")))
  
  } else {
    datas <- dplyr::mutate(datas,Date.Time = parse_date_time(Date.Time,orders = parse_order))  %>%
                            dplyr::select(Date.Time,Unit,Value)
  }
  
  #Take 3-minute averages.
  datas <- dplyr::mutate(datas,DeviceTimeFloor = 
                            as.POSIXct(floor(as.numeric(Date.Time) / (3 * 60)) * (3 * 60), origin='1970-01-01 6:00:00')) %>%
      dplyr::group_by(DeviceTimeFloor) %>%
      dplyr::mutate(Value = mean(Value)) %>%
      dplyr::filter(!duplicated(DeviceTimeFloor)) %>%
      dplyr::ungroup() %>%
      dplyr::mutate(Date.Time = DeviceTimeFloor) %>%
      dplyr::mutate(Date.Time = as.character(Date.Time,"%m/%d/%y %H:%M")) %>%
      dplyr::select(Date.Time,Unit,Value) %>%
      dplyr::filter(!is.na(Date.Time)) 
    
  
  #Save data.
  # cat(paste(header$V1, collapse="\n"), file=paste(file_path_sans_ext(filerunthermo),'minaves.csv',sep=""))
  write.table(datas, paste(file_path_sans_ext(filerunthermo),'MinAves.csv',sep=""), sep=",", append=TRUE, row.names=F, quote=F)
  
  
}
