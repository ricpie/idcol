#Load functions
source('thresholds-master/UPAS_thresholds.R')
source('thresholds-master/lascar_thresholds.R')
source('thresholds-master/ecm_thresholds.R')
source('r_scripts/UPAS_functions.R')
source('r_scripts/UPAS_functions_rp.R')
source('r_scripts/merging_functions.R')
source('r_scripts/generic_functions.R')
odk_path="../../Data/ODK Data"
odk_data = odk_import_fun(odk_path)
# equipment_IDs_fun()
local_tz = "Africa/Nairobi"
todays_date <- gsub("-", "", as.character(Sys.Date()))

#Load paths
upasfilepath <- "~/Dropbox/World Bank CSCB Field Folder/Field Data/UPAS Data"
file_list_upas <- list.files(upasfilepath, pattern='.csv|.CSV', full.names = T,recursive = T)
file_list_upas <- file_list_upas[sapply(file_list_upas, file.size) > 3000]



# Excel metadata import
# gravimetric_path <- "../Data/Data from the team/Gravimetric/UNOPS E2E_v2.xlsx"
# gravimetric_data <- grav_import_fun(gravimetric_path)


# metadata_ambient <- ambient_import_fun(path_other,sheetname='Ambient Sampling')
# ambient_data = readRDS("~/Dropbox/UNOPS emissions exposure/E2E Data Analysis/Processed Data/ambient_data.rds")



