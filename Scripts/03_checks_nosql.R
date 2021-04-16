## PROJECT: ddc_validation
## AUTHOR:  A.Chafetz | USAID
## PURPOSE: compare value outputs between DDC and processed files
## LICENSE: MIT
## DATE:    2020-10-20
## UPDATE:  2020-11-17

# DEPENDENCIES ------------------------------------------------------------

library(tidyverse)
library(vroom)
library(Wavelength)
library(glamr)
library(lubridate)
library(skimr)
library(glamr)

# GLOBAL VARIABLES --------------------------------------------------------

fldr_mer <- "Data/FY21"
fldr_processed <- "Data/2021.01_processed"
fldr_ddc <- "Data/DDC_2021.01i"

pd_date <- "2020-10-01"

# IMPORT ------------------------------------------------------------------

#mer data (FY21 targets)
  df_mer <- list.files(fldr_mer, full.names = TRUE) %>% 
    map_dfr(vroom, col_types = c(.default = "c", fy = "i", 
                                    # mer_results = "d", 
                                 mer_targets = "d"))

#processed output
  df_prcssd <- list.files(fldr_processed, full.names = TRUE) %>%
    vroom(col_types = c(.default = "c", fy = "i", val = "d", date = "D"))

#DDC initial output
  df_ddc <- return_latest(fldr_ddc, "Tableau") %>% 
    vroom(col_types = c(.default = "c", fy = "i", hfr_pd = "i",
                           val = "d", mer_results = "d", mer_targets = "d", date = "D"))

#DDC initial status + error reports
  df_status <- return_latest(fldr_ddc, "Status") %>% 
    vroom()
  
  df_err <- return_latest(fldr_ddc, "Error") %>%  
    vroom()
  
#org hierarchy
  df_org <- return_latest("../Wavelength/out/DATIM", "org") %>% 
    vroom()

  
  

# MUNGE -------------------------------------------------------------------


  df_org_ctry <- df_org %>% 
    select(orgunituid, countryname)
  
  df_prcssd <- left_join(df_prcssd, df_org_ctry, by = "orgunituid")
 
  

# GLOBAL REVIEW -----------------------------------------------------------

  df_err %>% 
    filter(validation_result == "Error",
           operatingunit == "Nigeria") %>% 
    count(validation_check, validation_message )
  
  df_err %>% 
    filter(validation_result == "Error") %>% 
    count(operatingunit, file_name, validation_check) %>%
    spread(validation_check,n) %>% 
    arrange(operatingunit, file_name) %>% 
    print(n = Inf)
    
  
  
  
   
# SETUP COMPARISON --------------------------------------------------------
  
#function to filter 
  set_comparision <- function(df, comp_ctry, ctry_ind){
      dplyr::filter(df, {{ctry_ind}} == comp_ctry)
  }
  
  

#comparison country
  comp_ctry <- "Kazakhstan"
  
#filter all dfs to comparison country
  df_mer_comp <- set_comparision(df_mer, comp_ctry, countryname)
  # df_server_comp <- set_comparision(df_server, comp_ctry, countryname)
  df_prcssd_com <- set_comparision(df_prcssd, comp_ctry, countryname)
  df_ddc_comp <- set_comparision(df_ddc, comp_ctry, operatingunit)
  # df_status_comp <- set_comparision(df_status, comp_ctry, cntry_nme)
  df_status_comp <- df_status %>% filter(str_detect(upload_file_nme, comp_ctry))
  # df_err_comp <- set_comparision(df_err, comp_ctry, cntry_nme)
  df_err_comp <- df_err %>% filter(str_detect(upload_file_nme, operatingunit))

# ERRORS ------------------------------------------------------------------

  
  df_status_comp %>% 
    glimpse
  
  (flags <- df_err_comp %>%
    distinct(vldtn_typ))
  
  lst_flags <- pull(flags, vldtn_typ)
  
  if(any(str_detect(lst_flags, "multiple_periods"))){
    df_err_comp %>% 
      distinct(hfr_pd) %>% 
      arrange(hfr_pd) %>% 
      pull(hfr_pd) %>% 
      paste0(collapse = ", ")
  }
  
  if(any(str_detect(lst_flags, "mech_ou_check"))){
    
    
    print(paste0("flagged: ", 
                 paste(unique(df_err_comp$mech_code), collapse = ", "),
                 " valid: ", 
                 paste(unique(df_err_comp$mech_code), collapse = ", ")))
    }
  
  
  if(any(str_detect(lst_flags, "multiple_periods"))){
    df_err_comp %>% 
      distinct(hfr_pd) %>% 
      arrange(hfr_pd) %>% 
      pull(hfr_pd) %>% 
      paste0(collapse = ", ")
  }
  
  if(any(str_detect(lst_flags, "mech_ou_check"))){
    df_err_comp %>% 
      distinct(mech_code)
  }
  

  
# COMPARISON --------------------------------------------------------------
  
  fltr_grp <- c("operatingunit","indicator")
  tbl_mer <- df_mer %>%
    group_by_at(vars(!!!fltr_grp)) %>%
    summarise(across(starts_with("mer"), sum, na.rm = TRUE)) %>%
    ungroup() %>%
    pivot_longer(starts_with("mer"),
                 names_to = "type",
                 values_to = "value_prcssd") %>% 
    mutate(date = as.Date(pd_date)) %>% 
    select(date, everything())

  # tbl_mer <- map_dfr(.x = c("2020-08-31", "2020-09-07", "2020-09-14","2020-09-21"),
  #                    .f = ~ mutate(tbl_mer, date = .x)) %>%
  #   select(date, everything()) %>%
  #   mutate(date = as_date(date))
  
  
  fltr_grp <- c("operatingunit","date", "indicator")
  
  tbl_prcssd <- df_prcssd %>%
    filter(hfr_freq %in% c("month", "month agg")) %>% 
    group_by_at(vars(!!!fltr_grp)) %>% 
    summarise(across(val, sum, na.rm = TRUE)) %>% 
    ungroup() %>% 
    rename(hfr_results = val) %>% 
    pivot_longer(c(starts_with("mer"), hfr_results),
                 names_to = "type",
                 values_to = "value_prcssd")
  
  tbl_ddc <- df_ddc %>%
    group_by_at(vars(!!!fltr_grp)) %>% 
    summarise(across(c(val, starts_with("mer")), sum, na.rm = TRUE)) %>% 
    ungroup() %>% 
    rename(hfr_results = val) %>% 
    pivot_longer(c(starts_with("mer"), hfr_results),
                 names_to = "type",
                 values_to = "value_ddc")
  
 
  
  tbl_full <- tbl_prcssd %>%
    bind_rows(tbl_mer) %>% 
    full_join(tbl_ddc) %>% 
    select(operatingunit, type, date, indicator, everything()) %>% 
    mutate(delta = value_ddc/value_prcssd,
           delta = ifelse(is.nan(delta), 1, delta),
           delta = ifelse(is.infinite(delta), 0, delta)) %>%
    arrange(operatingunit, type, indicator, date) 
  
  
  tbl_full %>% 
    filter(operatingunit %in% unique(df_prcssd$operatingunit),
           type == "hfr_results") %>% 
    skim()
  
  tbl_full %>% 
    filter(operatingunit %in% unique(df_prcssd$operatingunit),
           type == "hfr_results") %>% 
    print(n = Inf)
    View()
  
  
    tbl_full %>% 
      filter(operatingunit == "Nigeria",
             indicator %in% c("HTS_TST", "HTS_TST_POS", "TX_NEW", "TX_CURR", "TX_MMD"),
             type == "hfr_results") %>% 
      print(n = Inf)
    
  slice_max(tbl_full, order_by = value_ddc_adj, n = 15)
    
  write_csv(tbl_full, "Dataout/HFR_2020.13_validation.csv", na = "")
  
  skimr::skim(tbl_full)
  
  
  df_prcssd %>% 
    filter(countryname == "Kazakhstan")
  
  df_ddc %>% 
    filter(countryname == "Kazakhstan") %>% 
    View()
  
  
set.seed(43)
sel_orgunit <- df_prcssd %>% 
  filter(operatingunit == "Nigeria",
         val > 0) %>% 
  pull(orgunituid) %>% 
  sample(1)

df_prcssd %>% 
  filter(orgunituid == sel_orgunit,
         indicator == 'HTS_TST') %>% 
  select(date, orgunit, mech_code, indicator, sex, agecoarse, otherdisaggregate, val) %>% 
  arrange(sex, agecoarse)

df_ddc %>% 
  filter(orgunituid == sel_orgunit,
         indicator == 'HTS_TST') %>% 
  select(date, orgunit, mech_code, indicator, sex, agecoarse, otherdisaggregate, val) %>% 
  arrange(sex, agecoarse)
  

test_prcssd <- df_prcssd %>% 
  filter(orgunituid == sel_orgunit,
         indicator == 'HTS_TST') 

test_ddc <- df_ddc %>% 
  filter(orgunituid == sel_orgunit,
         indicator == 'HTS_TST') %>% 
  select(names(test_prcssd))



