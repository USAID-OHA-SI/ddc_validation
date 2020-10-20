## PROJECT: ddc_validation
## AUTHOR:  A.Chafetz | USAID
## PURPOSE: compare value outputs between DDC and processed files
## LICENSE: MIT
## DATE:    2020-10-20
## UPDATE:  

# DEPENDENCIES ------------------------------------------------------------

library(tidyverse)
library(vroom)
library(Wavelength)
library(glamr)
library(lubridate)

# GLOBAL VARIABLES --------------------------------------------------------

fldr_mer <- "Data/FY20Q3"
fldr_processed <- "Data/2020.13_processed"
fldr_ddc_init <- "Data/DDC_2020.13i"
fldr_ddc_adj <- "Data/DDC_2020.13c"


# IMPORT ------------------------------------------------------------------

#mer data (Q3c)
  df_mer <- list.files(fldr_mer, full.names = TRUE) %>% 
    map_dfr(vroom, col_types = c(.default = "c", fy = "i", 
                                    mer_results = "d", mer_targets = "d"))

#processed output
  df_prcssd <- list.files(fldr_processed, full.names = TRUE) %>% 
    map_dfr(vroom, col_types = c(.default = "c", fy = "i", val = "d"))
  

#DDC initial output
  df_ddc_init <- list.files(fldr_ddc_init, "Tableau", full.names = TRUE) %>% 
    vroom(col_types = c(.default = "c", fy = "i", hfr_pd = "i",
                           val = "d", mer_results = "d", mer_targets = "d"))

#DDC initial status + error reports
  df_status_init <- list.files(fldr_ddc_init, "Status", full.names = TRUE) %>% 
    vroom()
  df_err_check_init <- list.files(fldr_ddc_init, "Error-OUIM", full.names = TRUE)%>% 
    vroom()
  df_err_mech_init <- list.files(fldr_ddc_init, "Error-MechCheck", full.names = TRUE)%>% 
    vroom()
  df_err_uid_init <- list.files(fldr_ddc_init, "Error-OrgUnitUIDCheck", full.names = TRUE)%>% 
    vroom()
  
#DDC adjusted output
  df_ddc_adj <- list.files(fldr_ddc_adj, "Tableau", full.names = TRUE) %>% 
    vroom(col_types = c(.default = "c", fy = "i", hfr_pd = "i",
                        val = "d", mer_results = "d", mer_targets = "d"))
  
#DDC adjusted status + error reports
  df_status_adj <- list.files(fldr_ddc_adj, "Status", full.names = TRUE) %>% 
    vroom()
  df_err_check_adj <- list.files(fldr_ddc_adj, "Error-OUIM", full.names = TRUE)%>% 
    vroom()
  df_err_mech_adj <- list.files(fldr_ddc_adj, "Error-MechCheck", full.names = TRUE)%>% 
    vroom()
  df_err_uid_adj <- list.files(fldr_ddc_adj, "Error-OrgUnitUIDCheck", full.names = TRUE)%>% 
    vroom()
  
#org hierarchy
  df_org <- list.files("../Wavelength/out/DATIM", "org", full.names = TRUE) %>% 
    vroom()

  
# SETUP COMPARISON --------------------------------------------------------
  
  #function to filter 
  set_comparision <- function(df, comp_ctry, ctry_ind){
    dplyr::filter(df, {{ctry_ind}} == comp_ctry)
  }
  
  #comparison country
  comp_ctry <- "Vietnam"
  
  #filter all dfs to comparison country
  # df_mer_comp <- set_comparision(df_mer, comp_ctry, countryname)
  df_server_comp <- set_comparision(df_server, comp_ctry, countryname)
  df_ddc_comp <- set_comparision(df_ddc, comp_ctry, countryname)
  df_err_comp <- set_comparision(df_err, comp_ctry, cntry_nme)
  df_status_comp <- set_comparision(df_status, comp_ctry, cntry_nme)