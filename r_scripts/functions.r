#________________________________________________________
# require libraries
  library(tidyverse)
  library(lubridate)
  library(dplyr)
#________________________________________________________

#________________________________________________________
# check for outliers
  is_outlier <- function(x) {
    
    out <- (x < quantile(x, 0.25, na.rm = TRUE) - 1.5 * IQR(x, na.rm = TRUE)) | 
           (x > quantile(x, 0.75, na.rm = TRUE) + 1.5 * IQR(x, na.rm = TRUE))
    
  # return
    return(out)
}
#________________________________________________________

#________________________________________________________
# filter data for time periods of interest only
# requires df with time windows (id, start, end)
# df with id, time
# appends rep variable
filter_times <- function(times, df){
    
    rows <- nrow(times)
    
    # loop idsx
    for(i in 1:rows){
      # filter by date and time
      tmp <- dplyr::filter(df, date == times$date[i]) %>%
             dplyr::filter(hh_id == times$hh_id[i],
                           time >= times$start[i],
                           time <= times$end[i])
      
      # if first match
      if(exists("out", inherits = FALSE) == FALSE & nrow(tmp) > 0){
        out <- tmp
      }
      
      # if not first match with data
      if(exists("out", inherits = FALSE) == TRUE & nrow(tmp) > 0){
        out <- rbind(out, tmp)
      }
      # end for loop
    }
    
    # return
    return(out)
}
#________________________________________________________

#________________________________________________________
# Calculate the molecular weight of study pollutants
# Molecuar weights are calculated using the average
# standard atomic weights of each individual elements
#
# Atomic weights are from the NIST Physical Reference Data Website
calc_mw <- function(tbl){

  dplyr::mutate(tbl, mw = (num_c * 12.0106) +
                          (num_h * 1.007975) +
                          (num_o * 15.9994),
                       mw = ifelse(other == "S" & !is.na(other),
                                   mw + 32.0675, mw))

}
#________________________________________________________

#________________________________________________________
# return the molecular weight of carbon
mw_c <- function() {12.0106}
#________________________________________________________

#________________________________________________________
# convert ppmv to ug/m^3
# mw = molecular weight g/mol
# t = temperature oC
# p = pressure kPa
convert_ppmv_mgm3 <- function(ppmv, mw, t = 25, p = 101325){

  (ppmv * mw)*(p /(8.3144 * (t + 273.15))) * (1 / 1000)
  
}
#________________________________________________________

#________________________________________________________
get_lm_eqn <- function(m){

  eq <- substitute(~~R^2~"="~r2, 
                   list(r2 = format(summary(m)$r.squared, digits = 3)))

  as.character(as.expression(eq))

}
#________________________________________________________

#________________________________________________________
# apply kirchstetter loading correction to microaeth data
ma_loading_corr <- function(data) {
  a <- 0.88
  b <- 0.12
  data %>% 
    dplyr::mutate(Tr = exp(-atn / 100)) %>%
    dplyr::mutate(rK = (a * Tr + b)) %>%
    dplyr::mutate(bc_corr = bc / (0.6 * rK))
  
}
#________________________________________________________

stacking_stovedays_fun <- function(ok_cooking_events_padded){
  
  # Initialize stacking_stove_days dataframe
  stacking_stovedays = data.frame(group=character(),stove_descriptions=character(),HHID=character(),
                                  percent_stovedays=character())
  grouplist <- unique(ok_cooking_events_padded$group)
  
  
  for (g in 1:length(unique(grouplist))){
    ok_cooking_events_padded_group <- dplyr::filter(ok_cooking_events_padded, group==grouplist[g]) #Subset of hhid j
    HH_list <- unique(ok_cooking_events_padded_group$HHID)
    
    #Loop through households, then through first stove, then through second. Get percent stove days in second loop.
    for (j in 1:length(unique(HH_list))) {
      temp_events <- dplyr::filter(ok_cooking_events_padded_group, HHID==HH_list[j]) #Subset of hhid j
      temp_stoves <- unique(temp_events$stove_descriptions)
      length_temp_stoves <- length(unique(temp_events$stove_descriptions))
      
      for (jj in 1:length_temp_stoves) { #For the first stove
        temp_events_overlap1 <- dplyr::filter(temp_events, stove_descriptions==temp_stoves[jj]) %>% #Subset of stove jj
          dplyr::group_by(day_month_year) %>% #Get data from unique days only
          dplyr::select(group,stove_descriptions,HHID,use_flag,day_month_year,start_time,end_time) #Keep all data, for the time overlap calc.
        temp_events_stove1 <- dplyr::filter(temp_events_overlap1,row_number(day_month_year) == 1) #Keep only first row, for the stove-days calc
        
        
        #Get the percent stovedays for the solo stoves.
        percent_stovedays <- sum(temp_events_stove1$use_flag)/dim(temp_events_stove1)[1] *100
        stacking_stovedays <- rbind(stacking_stovedays,
                                    data.frame(group=temp_events_stove1$group[1],
                                               stove_descriptions=temp_events_stove1$stove_descriptions[1],
                                               HHID=temp_events_stove1$HHID[1], percent_stovedays))
        
        if (length_temp_stoves>1 && jj<length_temp_stoves) {
          for (jjj in (jj+1):length_temp_stoves) { #For the second stove.
            
            temp_events_stove2 <- dplyr::filter(temp_events, stove_descriptions==temp_stoves[jjj]) %>% #Subset of stove jj
              dplyr::group_by(day_month_year) %>% #Get data from unique days only
              dplyr::select(group,stove_descriptions,HHID,use_flag,day_month_year,start_time,end_time) %>%
              dplyr::filter(row_number(day_month_year) == 1) %>%
              dplyr::inner_join(temp_events_stove1,by = "day_month_year")
            
            #How is this going to join - every combo of day-month-year?
            temp_events_overlap2 <- dplyr::filter(temp_events, stove_descriptions==temp_stoves[jjj]) %>% #Subset of stove jj
              dplyr::group_by(day_month_year) %>% #Get data from unique days only
              dplyr::select(group,stove_descriptions,HHID,use_flag,day_month_year,start_time,end_time) %>%
              dplyr::full_join(temp_events_overlap1,by = "day_month_year")
            
            percent_stovedays2 <- sum(temp_events_stove2$use_flag.x==TRUE & temp_events_stove2$use_flag.y==TRUE)/
              dim(temp_events_stove1)[1] *100
            
            #Get the percent stove days for the stove combos
            stacking_stovedays <- rbind(stacking_stovedays,
                                        data.frame(group=temp_events_stove2$group.x[1], 
                                                   stove_descriptions=paste(temp_events_stove2$stove_descriptions.x[1],'+',
                                                                            temp_events_stove2$stove_descriptions.y[1]),
                                                   HHID=temp_events_stove2$HHID.x[1], percent_stovedays = percent_stovedays2))
            
          }
        }
      }
    }
  }
  stacking_stovedays
}



#________________________________________________________
#Get counts of times when multiple stoves were in use at once. %of cooking events that were simultaneous?
#%of mimi events that had another stove at the same time (for days when they were both monitored).
#use the padded data set - keep all data that has a corresponding stove's data from the same dates, and use the useflag to ignore non-simul events.
simul_events_fun <- function(ok_cooking_events_padded){
  
  # Initialize stacking_stove_days dataframe
  simul_by_stove_combos = data.frame(group=character(),stove_descriptions=character(),HHID=character(),
                                  simul_counts=numeric(), days_simuldata_avail=numeric(),
                                  fraction_days_simul=numeric(),percent_days_simul=numeric())
  HH_list <- unique(ok_cooking_events_padded$HHID)
  #Loop through households, then through first stove, then through a second one. Get percent stove days in second loop.
  for (j in 1:length(unique(HH_list))) {#Loop through households
    temp_events <- dplyr::filter(ok_cooking_events_padded, HHID==HH_list[j]) #Subset of hhid j
    temp_stoves <- unique(temp_events$stove_descriptions)
    length_temp_stoves <- length(unique(temp_events$stove_descriptions))
    
    #Worry about simulcooking if there are more than two stoves only.
    if (length_temp_stoves>1) { 
      
        for (jj in 1:(length_temp_stoves-1)) { #Loop through stoves.
          
            for (jjj in (jj+1):length_temp_stoves) { #For the second stove.
              #Keep data for days in which stove jj and stove jjj both have data contributing.
              temp_events_both_have_data <- dplyr::group_by(temp_events,day_month_year) %>%
                dplyr::filter(length(unique(stove_descriptions))==2) %>%
                dplyr::ungroup()
              
              #How to deal with use_flag = false?
              temp_events_stovejj <- as.data.table(dplyr::filter(temp_events_both_have_data, stove_descriptions==temp_stoves[jj])) #Subset of stove jj
              temp_events_stovejj<-setkey(temp_events_stovejj,start_time,end_time)
              
              temp_events_stovejjj <- as.data.table(dplyr::filter(temp_events_both_have_data, stove_descriptions==temp_stoves[jjj])) #Subset of stove jjj
         
              simul_counts <- dim(asdf<-foverlaps(temp_events_stovejjj,temp_events_stovejj,type="any") %>% dplyr::filter(!is.na(start_time)))[1]
              days_simuldata_avail = length(unique(temp_events_both_have_data$day_month_year))
              fraction_days_simul = simul_counts/days_simuldata_avail
              
              #Get the percent simultaneous cooking events for the stove combos
              simul_by_stove_combos <- rbind(simul_by_stove_combos,
                                          data.frame(group=temp_events_stovejjj$group[1], 
                                                     stove_descriptions=paste(temp_events_stovejj$stove_descriptions[1],'+',
                                                                              temp_events_stovejjj$stove_descriptions[1]),
                                                     HHID=temp_events_stovejj$HHID[1], simul_counts = simul_counts,
                                                     days_simuldata_avail = days_simuldata_avail, fraction_days_simul=fraction_days_simul,
                                                     percent_days_simul=100*fraction_days_simul))
  
        }
      }
    }
  }
  simul_by_stove_combos
}

#for each SUM data file, get cooking events.
event_fun <- function(i,sumsarized_filtered){
  #Grab data from file i, and keep only the entries that are marked as cooking
  temp <- dplyr::filter(sumsarized_filtered,file_indices == i) %>%
    dplyr::filter(state == TRUE) %>% dplyr::filter(!duplicated(datetime)) 
  tempALL <- dplyr::filter(sumsarized_filtered,file_indices == i) 
  
  qc_temp <- "ok" #Default to ok data quality, gets demoted based on later checks.
  
  #Organize/check/fix datetimes of sum placements and removals. If there is a start and end time available for the monitoring period, use it to keep the data from the file.  Disregard this if the datetime_removal is NA, since it means we don't have a fixed known end date.  In this case we assume the end of the file is the end of the monitoring preiod and keep all the data from the given file.  Provide the start and end date in the event-building loop below.
  min_time_file = as.POSIXct(min(tempALL$datetime))
  max_time_file = as.POSIXct(max(tempALL$datetime))
  if ( metadata_date_ignore == 1) {   #datetime_placed is changed to the local file value if it is below the start_date_range, and datetime_placed is too, according to the end_date_range
    datetime_placed_temp = as.POSIXct(min_time_file)
    datetime_removal_temp = as.POSIXct(max_time_file) 
    qc_temp <- "ok" #These get filtered out, assuming it represents poorly time formatted data.
  }  else if ((max_time_file>end_date_range | min_time_file<start_date_range)) {   #datetime_placed is changed to the local file value if it is below the start_date_range, and datetime_placed is too, according to the end_date_range
      datetime_placed_temp = as.POSIXct(min_time_file)
      datetime_removal_temp = as.POSIXct(max_time_file) 
      qc_temp <- "out_of_placement_range" #These get filtered out, assuming it represents poorly time formatted data.
  } else if (is.na(as.POSIXct(temp$datetime_removal[1])) | is.na(as.POSIXct(temp$datetime_placed[1]))){  #If date is NA, use the file's dates.  They have already been midnight-trimmed in the load_data.r function.
    datetime_placed_temp = as.POSIXct(min_time_file)
    datetime_removal_temp = as.POSIXct(max_time_file)
    qc_temp <- "NA_metadata"
  } else if (min_time_file> as.POSIXct(temp$datetime_placed[1])) {  #If placement time is before time of first data point, switch to first data point.
    datetime_placed_temp = as.POSIXct(min_time_file)
    datetime_removal_temp = as.POSIXct(max_time_file)
    qc_temp <- "placement_before_data"
  } else if (as.POSIXct(min(temp$datetime_placed))> as.POSIXct(max(temp$datetime_removal))) {  #If placement time is greater than the datetime placed, switch them, it's a mistake.
    datetime_placed_temp = as.POSIXct(min_time_file)
    datetime_removal_temp = as.POSIXct(max_time_file)
    qc_temp <- "placement_greaterthan_removal"
  } else { #If not NA, a value must have been found in the meta data, use it.
    datetime_placed_temp = as.POSIXct(min(temp$datetime_placed))
    datetime_removal_temp = as.POSIXct(max(temp$datetime_removal))
    qc_temp <- "metadata_dates_used"
  }      
  
  #Separate case to handle data from SUMs placed before start_date_range (possible deployment start date)
  if (min_time_file<start_date_range) {  
    datetime_placed_temp = as.POSIXct(start_date_range)
    qc_temp <- "out_of_placement_range" #These get filtered out, assuming it represents poorly time formatted data.
  }
  if (max_time_file>end_date_range) {  
    datetime_removal_temp = as.POSIXct(end_date_range) 
    qc_temp <- "out_of_placement_range" #These get filtered out, assuming it represents poorly time formatted data.
  }
  
  
  #Put cooking events into a single table.
  if (dim(temp)[1]>1 && any(temp$state==TRUE)) {
    
    time_difference <- as.numeric(difftime(temp$datetime,lag(temp$datetime),units="mins"))
    time_difference <- time_difference[!is.na(time_difference)] #
    
    breakstart <- c(0,which((time_difference>cooking_group) == TRUE))+1 #Start of a cooking event
    breakend <- c(which((time_difference>cooking_group) == TRUE),
                  if (tail(temp$state,n=1) == TRUE){dim(temp)[1]}) #End of cooking event. 
    #Tail part is in case the time series ends while still cooking...need to account for th
    
    
    #Add cooking events to the cooking_events data frame.
    cooking_events <- rbind(cooking_events,
                            data.frame(start_time= as.POSIXct(temp$datetime[breakstart]),
                                       end_time=as.POSIXct(temp$datetime[breakend]), 
                                       group=as.factor(temp$group[breakstart]),
                                       region=as.factor(temp$region[breakstart]),
                                       HHID=as.factor(temp$HHID[breakstart]),
                                       logger_id=factor(temp$logger_id[breakstart]),
                                       stove=factor(temp$stove[breakstart]),
                                       logging_duration_days = as.numeric(difftime(datetime_removal_temp,datetime_placed_temp,units = "days")),
                                       datetime_placed = datetime_placed_temp,
                                       datetime_removal = datetime_removal_temp,
                                       file_indices = temp$file_indices[breakstart], 
                                       filename = temp$filename[breakstart],
                                       fullsumsarizer_filename = temp$fullsumsarizer_filename[breakstart],
                                       comments = temp$comments[1],
                                       use_flag=as.logical(rep(1,length(breakstart)[1])),
                                       qc_dates = qc_temp))
  } else{
    #If no cooking events are found, still create an entry, though with the use flag as FALSE, so 
    #that we know that there was data collected and zero events in that period.  Set the use_flag to true here to differntiate from the too-short cooking events.
    
    temp <- dplyr::filter(sumsarized_filtered,file_indices == i)  #Create new temp here so 
    #we can get info about the sample that does not have cooking events.
    cooking_events <- rbind(cooking_events,
                            data.frame(start_time=as.POSIXct(temp$datetime[1]),
                                       end_time=as.POSIXct(temp$datetime[1]), 
                                       group=as.factor(temp$group[1]),
                                       region=as.factor(temp$region[1]),
                                       HHID=as.factor(temp$HHID[1]), 
                                       logger_id=factor(temp$logger_id[1]),
                                       stove=factor(temp$stove[1]), 
                                       logging_duration_days = as.numeric(difftime(datetime_removal_temp,datetime_placed_temp,units = "days")),
                                       datetime_placed = datetime_placed_temp,
                                       datetime_removal = datetime_removal_temp,
                                       file_indices = temp$file_indices[1], 
                                       filename = temp$filename[1],
                                       fullsumsarizer_filename = temp$fullsumsarizer_filename[1],
                                       comments = temp$comments[1],
                                       qc_dates = qc_temp,
                                       use_flag=FALSE))
  }
  return(cooking_events)
}

#Pad the data set to ensure that files without events are taken into account, and that days without events are properly accounted for in analyses.
pad_fun <- function(i,ok_cooking_events_unfiltered){
  uniquers <- unique(ok_cooking_events_unfiltered$fullsumsarizer_filename)
  temp <- dplyr::filter(ok_cooking_events_unfiltered,uniquers[i]==fullsumsarizer_filename) %>%
    dplyr::arrange(start_time)
  
  if (dim(temp)[1]>0) {    
    # generate a time sequence with 1 day intervals to fill in
    # missing dates
    all.dates <- data.frame(dates = seq(temp$datetime_placed[1], temp$datetime_removal[1], by="day"),stringsAsFactors = FALSE) %>%
      dplyr::mutate(day_month_year = as.Date(dates)) %>%
      dplyr::filter(!day_month_year %in% temp$day_month_year) #Get rid of extra days
    
    # Convert all dates to a data frame. Note that we're putting
    # the new dates into a column called "start_time" just like the
    # original column. This will allow us to merge the data.
    all.dates.frame <- data.frame(list(start_time=all.dates$dates),list(end_time=all.dates$dates), 
                                  list(day_month_year = as.Date(all.dates$dates)),
                                  list(week_year = format(all.dates$dates,"%V-%y")),
                                  list(day_of_week = weekdays(all.dates$dates)),stringsAsFactors = FALSE) %>%
      dplyr::mutate(month_year = format(start_time,"%b-%y")) %>%
      dplyr::mutate(month_year = factor(month_year, unique(month_year), ordered=TRUE))
    
    
    # Merge the two datasets: the full dates and original data
    merged.data <- merge(all.dates.frame, temp, all=T) %>%
      tidyr::fill(filename,fullsumsarizer_filename,HHID,stove,logger_id,group,region,
                  stove_descriptions,logging_duration_days,datetime_placed,
                  datetime_removal,units,comments,
                  start_hour,.direction = c("up")) %>%
      tidyr::fill(filename,fullsumsarizer_filename,HHID,stove,logger_id,group,region,
                  stove_descriptions,logging_duration_days,datetime_placed,
                  datetime_removal,units,comments,
                  start_hour,.direction = c("down"))  %>%
      dplyr::mutate(use_flag = replace(use_flag, is.na(use_flag), FALSE)) 
    
    
    merged.data %>% mutate_if(is.factor, as.character) -> merged.data
    
    merged.data$use_flag[is.na(merged.data$use_flag)] <- FALSE
    merged.data[is.na(merged.data)] <- 0
    # The above merge set the new observations to NA.
    # To replace those with a 0, we must first find all the rows
    # and then assign 0 to them.
    
    ok_cooking_events_padded <- rbind(ok_cooking_events_padded,merged.data)
  }
}
