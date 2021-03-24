#________________________________________________________
# load relevant libraries
library(tidyverse)
library(lubridate)
library(stringr)
library(reshape2)
library(gsubfn)


#________________________________________________________
#Load metadata and fix any issues with it.
filter_metadata_fun <- function(metadata){

metadata <- dplyr::mutate(metadata, HHID = gsub('54asdf5','74162',HHID)) %>%
  # dplyr::mutate(HHID = gsub('3-2-05-02','320502',HHID)) %>%
  # dplyr::mutate(HHID = gsub('3-2-5-3','320503',HHID)) %>%
  # dplyr::mutate(HHID = gsub('3-2-05-04','320504',HHID)) %>%
  # dplyr::mutate(HHID = gsub('3-2-05-05','320505',HHID)) %>%
  # dplyr::mutate(HHID = gsub('3-7-5-01','370501',HHID)) %>% 
  # dplyr::mutate(HHID = gsub('3 2 05 01','320501',HHID)) %>%
  # dplyr::mutate(HHID = gsub('3 2 05 02','320502',HHID)) %>%
  # dplyr::mutate(HHID = gsub('3300405','330405',HHID)) %>%
  # dplyr::mutate(HHID = gsub('3  2 05 03','320503',HHID)) %>%
  # dplyr::mutate(HHID = gsub('3 2 05 03','320503',HHID)) %>%
  # dplyr::mutate(HHID = gsub('3 2 05 04','320504',HHID)) %>%
  # dplyr::mutate(HHID = gsub('3 2 05 05','320505',HHID)) %>%
  # dplyr::mutate(HHID = gsub('3-2-05-07','320507',HHID)) %>%
  # dplyr::mutate(HHID = gsub('3-2-05-08','320508',HHID)) %>%
  # dplyr::mutate(HHID = gsub('3-2-05-09','320509',HHID)) %>%
  # dplyr::mutate(HHID = gsub('FE4D','320501',HHID)) %>%
  # dplyr::mutate(HHID = gsub('E9EC','370502',HHID)) %>%
  # dplyr::mutate_all(funs(toupper))  %>%
  # dplyr::mutate(logger_id = gsub('ODCD','0DCD',logger_id)) %>% # ODCD ODDC ODBC OCE9 OFF2 Off2 OFA2
  # dplyr::mutate(logger_id = gsub('ODDC','0DDC',logger_id)) %>%
  # dplyr::mutate(logger_id = gsub('ODBC','0DBC',logger_id)) %>%
  # dplyr::mutate(logger_id = gsub('ODDC','0DDC',logger_id)) %>%
  # dplyr::mutate(logger_id = gsub('OCE9','0CE9',logger_id)) %>%
  # dplyr::mutate(logger_id = gsub('ODDC','0DDC',logger_id)) %>%
  # dplyr::mutate(logger_id = gsub('OFF2','0FF2',logger_id)) %>%
  # dplyr::mutate(logger_id = gsub('OFA2','0FA2',logger_id)) %>%
  # dplyr::mutate(logger_id = gsub('O337','0377',logger_id)) %>%
  # dplyr::mutate(logger_id = gsub('0337','0377',logger_id)) %>%
  dplyr::mutate(dot_name = logger_id)
  


}