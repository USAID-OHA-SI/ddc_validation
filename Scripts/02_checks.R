## PROJECT: ddc_validation
## AUTHOR:  A.Chafetz | USAID
## PURPOSE: compare value outputs between USAID/SQLView and DDC
## LICENSE: MIT
## DATE:    2020-09-23
## UPDATE:  2020-10-06

# DEPENDENCIES ------------------------------------------------------------

library(tidyverse)
library(Wavelength)
library(glamr)
library(lubridate)

# GLOBAL VARIABLES --------------------------------------------------------


# IMPORT ------------------------------------------------------------------

  #mer data (Q3i)
    df_mer <- list.files("data/FY20Q3", full.names = TRUE) %>% 
      map_dfr(read_csv, col_types = c(.default = "c", fy = "i", 
                                      mer_results = "d", mer_targets = "d"))
    
  #server output
    df_server <- hfr_read("data/HFR_2020.12_Tableau_20200924.zip")
  
  #DDC output
    df_ddc <- return_latest("data", "hfr_2020.12.*csv") %>% 
      read_csv(col_types = c(.default = "c", fy = "i", hfr_pd = "i",
                             val = "d", mer_results = "d", mer_targets = "d"))
    
  #DDC status
    df_status <- return_latest("data", "status") %>% 
      read_csv()
    
  #Error report
    df_err <- return_latest("data", "[E|e]rror") %>% 
      read_csv()
    
  #org hierarchy
    df_org <- list.files("../Wavelength/out/DATIM", "org", full.names = TRUE) %>% 
      read_csv()

# MUNGE -------------------------------------------------------------------

  #mutate date for ddc data
    df_ddc <- df_ddc %>% 
      mutate(date = mdy(date))
    
  #limit to just 2020.12 to match DDC Tableau
    df_server <- df_server %>% 
        filter(hfr_pd == 12)
    
    df_status <- df_status %>% 
      filter(rptng_prd == 202012)
  
    df_err <- df_err %>% 
      filter(rptng_prd == 2020.12)

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

# COMPARISON --------------------------------------------------------------

    #review status
      glimpse(df_status_comp)
    
    #review errors if any
      df_err_comp %>% 
        distinct(vldtn_rslt, vldtn_msg, vldtn_typ)
    
      df_err_comp %>% 
        filter(vldtn_typ %in% c("missing_orgunitid_datim", "orgunituid_ou_check")) %>% 
        distinct(org_unit, org_unit_uid)
      
      df_err_comp %>% 
        filter(vldtn_typ %in% c("missing_orgunitid_datim", "orgunituid_ou_check")) %>% 
        distinct(mech_or_prtnr_nme)
      
      df_err_comp %>% 
        filter(vldtn_typ == "mech_ou_check") %>% 
        distinct(mech_code)
      
 
  #review values
    # fltr_grp <- c("indicator")
    # tbl_mer <- df_mer_comp %>% 
    #   group_by_at(vars(!!!fltr_grp)) %>%
    #   summarise(across(starts_with("mer"), sum, na.rm = TRUE)) %>% 
    #   ungroup() %>% 
    #   pivot_longer(starts_with("mer"),
    #                names_to = "type",
    #                values_to = "value_mer")
    # 
    # tbl_mer <- map_dfr(.x = c("2020-08-03", "2020-08-10", "2020-08-17","2020-08-24"),
    #                    .f = ~ mutate(tbl_mer, date = .x)) %>% 
    #   select(date, everything()) %>% 
    #   mutate(date = as_date(date))
    
    fltr_grp <- c("date", "indicator")
    tbl_server <- df_server_comp %>% 
      group_by_at(vars(!!!fltr_grp)) %>% 
      summarise(across(c(val, starts_with("mer")), sum, na.rm = TRUE)) %>% 
      ungroup() %>% 
      rename(hfr_results = val) %>% 
      pivot_longer(c(starts_with("mer"), hfr_results),
                   names_to = "type",
                   values_to = "value_server")
    
    tbl_ddc <- df_ddc_comp %>%
      group_by_at(vars(!!!fltr_grp)) %>% 
      summarise(across(c(val, starts_with("mer")), sum, na.rm = TRUE)) %>% 
      ungroup() %>% 
      rename(hfr_results = val) %>% 
      pivot_longer(c(starts_with("mer"), hfr_results),
                   names_to = "type",
                   values_to = "value_ddc")
    
    tbl_full <- tbl_server %>%
      full_join(tbl_ddc) %>% 
      select(type, date, indicator, everything()) %>% 
      mutate(delta = value_ddc/value_server) %>%
      arrange(indicator, date) 
    
      map(unique(tbl_full$type),
          ~ dplyr::filter(tbl_full, type == .x) %>% prinf())

    
    #results
    tbl_server <- df_server_comp %>% 
      filter(!is.na(val)) %>% 
      count(date, mech_code, indicator, wt = mer_results, name = "val_server")
    
    tbl_ddc <- df_ddc_comp %>%
      filter(!is.na(val)) %>%
      count(date, mech_code, indicator, wt = mer_results, name = "val_ddc")
    
    full_join(tbl_server, tbl_ddc) %>% 
      mutate(delta = val_ddc/val_server) %>%
      arrange(date, indicator) %>% 
      prinf()

    
    tbl_server <- df_server_comp %>% 
      filter(!is.na(val)) %>% 
      count(date, indicator, wt = val, name = "val_server")
    
    tbl_ddc <- df_ddc_comp %>%
      filter(!is.na(val)) %>%
      count(date, indicator, wt = val, name = "val_ddc")
    
    full_join(tbl_server, tbl_ddc) %>% 
      mutate(delta = val_ddc/val_server) %>%
      arrange(date, indicator) %>% 
      prinf()
    
    
    tbl_server <- df_server_comp %>%
      filter(indicator == "HTS_TST_POS",
             val > 0) %>% 
      count(date, mech_code, orgunit, orgunituid, wt = val, name = "val_server")
    
    tbl_ddc <- df_ddc_comp %>%
      filter(indicator == "HTS_TST_POS",
             val > 0) %>% 
      count(date, mech_code, orgunit, orgunituid, wt = val, name = "val_ddc")
    
    full_join(tbl_server, tbl_ddc) %>% 
      mutate(delta = val_ddc/val_server) %>%
      arrange(date) %>% 
      prinf()
    
    
    tbl_server <- df_server_comp %>%
      filter(indicator == "HTS_TST_POS",
             orgunituid == "dlrLnvsCZlk",
             val > 0) %>% 
      count(date, mech_code, orgunit, orgunituid, agecoarse, sex, wt = val, name = "val_server")
    
    tbl_ddc <- df_ddc_comp %>%
      filter(indicator == "HTS_TST_POS",
             orgunituid == "dlrLnvsCZlk",
             val > 0) %>% 
      count(date, mech_code, orgunit, orgunituid, agecoarse, sex, wt = val, name = "val_ddc")
    
    full_join(tbl_server, tbl_ddc) %>% 
      mutate(delta = val_ddc/val_server) %>%
      arrange(date) %>% 
      prinf()
    
    
    
    set.seed(42)
    comp_uid <- df_server_comp %>% 
      distinct(orgunituid) %>% 
      sample_n(1) %>% 
      pull()

    
    
    df_server_comp %>% 
      filter(orgunituid == "ZOinAilrD4X",
             indicator == "TX_NEW",
             date == max(date))
    
    df_ddc_comp %>% 
      filter(orgunituid == "ZOinAilrD4X",
             indicator == "TX_NEW",
             date == max(date))
    
    df_server %>% 
      filter(!is.na(val),
             orgunituid ==comp_uid) %>% 
      nrow()
    
    df_ddc_comp %>% 
      filter(!is.na(val),
             orgunituid ==comp_uid) %>% 
      nrow()
    
    
    df_server %>% 
      filter(!is.na(val),
             orgunituid ==comp_uid,
             indicator == "TX_CURR") %>% 
      nrow()
    
    df_ddc_comp %>% 
      filter(!is.na(val),
             orgunituid ==comp_uid,
             indicator == "TX_CURR") %>% 
      nrow()