## PROJECT: ddc_validation
## AUTHOR:  A.Chafetz | USAID
## PURPOSE: compare value outputs between USAID/SQLView and DDC
## LICENSE: MIT
## DATE:    2020-09-23
## UPDATE:  

# DEPENDENCIES ------------------------------------------------------------

library(tidyverse)
library(Wavelength)
library(glamr)

# GLOBAL VARIABLES --------------------------------------------------------


# IMPORT ------------------------------------------------------------------

  #server output
    df_server <- hfr_read("data/HFR_2020.12_Tableau_20200924.zip")
  
  #DDC output
    df_ddc <- return_latest("data", "HFR_2020.12.*csv") %>% 
      hfr_read()
    
  #DDC status
    df_status <- read_csv("data/cntry_hfr_sbmsn_status_Outputcsv.csv")
    
  #Error report
    df_err <- read_csv("data/Error_sbmsn_output.csv")

# MUNGE -------------------------------------------------------------------

  #limit server data to just 2020.12 to match DDC
    df_server <- df_server %>% 
        filter(hfr_pd == 12)
  
  #limit status to 2020.12
    df_status <- df_status %>% 
      filter(rptng_prd == 202012)
  
  #comparison country
    comp_ctry <- "Eswatini"
    
  #filter to comparison country
    df_server_comp <- df_server %>% 
      filter(countryname == comp_ctry)
    
    df_ddc_comp <- df_ddc %>% 
      filter(countryname == comp_ctry)
    
    df_err_comp <- df_err %>% 
      filter(cntry_nme == comp_ctry,
             rptng_prd == 2020.12)
    
    df_status_comp <- df_status %>% 
      filter(cntry_nme == comp_ctry)
    
# COMPARISON --------------------------------------------------------------

    glimpse(df_status_comp)
    
    df_err_comp %>% 
      distinct(vldtn_rslt, vldtn_msg, vldtn_typ)
    
    df_err_comp %>% 
      filter(vldtn_typ %in% c("missing_orgunitid_datim", "orgunituid_ou_check")) %>% 
      distinct(org_unit, org_unit_uid)
    
    df_err_comp %>% 
      filter(vldtn_typ %in% c("missing_orgunitid_datim", "orgunituid_ou_check")) %>% 
      distinct(mech_or_prtnr_nme)
    
    
    
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