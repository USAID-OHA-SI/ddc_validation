## PROJECT:  HFR
## AUTHOR:   A.CHAFETZ | USAID
## PURPOSE:  iterate email submission reports
## LICENSE:  MIT
## DATE:     2021-11-15
## UPDATED:  2021-11-16


# DEPENDENCIES ------------------------------------------------------------

library(tidyverse)
library(vroom)
library(Wavelength)
library(glamr)
library(lubridate)
library(glue)
library(scales)
library(knitr)
library(rmarkdown)
library(here)
library(googledrive)
library(fs)
library(glitr)
library(extrafont)


# GLOBAL VARIABLES --------------------------------------------------------

  load_secrets()


# IMPORT DATA -------------------------------------------------------------

  #FY21 HFR Outputs
  stash_outgoing("HFR_Submission", "Data", gdrive = FALSE)
  stash_outgoing("HFR_Tableau", "Data", gdrive = FALSE)

  #DDC submission status
  df_stat <- read_csv("Data/HFR_Submission_Status.csv", col_types = c(.default = "c")) 

  #HFR data
  df_hfr <- hfr_read("Data/HFR_Tableau_SQLview.csv")


# MUNGE SUBMISSION STATUS -------------------------------------------------

  #remove submitter from file name
  df_stat <- df_stat %>% 
    mutate(file_name = file_name %>% 
             str_remove(".xlsx$") %>% 
             str_replace(" - ", " [") %>% 
             paste0("]"))  
  
  #table of files with status and records  
  df_stat <- df_stat %>% 
    filter(fy == max(fy, na.rm = TRUE)) %>% 
    filter(hfr_pd == max(hfr_pd, na.rm = TRUE)) %>% 
    mutate(across(c(records_successfully_processed, records_not_processed_due_to_errors), as.double),
           countryname = ifelse(operatingunit == countryname, countryname, glue("{operatingunit}/{countryname}")),
           hfr_pd = glue("{fy}.{hfr_pd}")) %>% 
    select(countryname, hfr_pd, file_name, status, records_successfully_processed, records_not_processed_due_to_errors)



# MUNGE HFR BASE ----------------------------------------------------------

  #current period
  curr_date <- max(df_hfr$date, na.rm = TRUE)
  
  #limit time period
  df_hfr_agg <- df_hfr %>% 
    filter(date >= curr_date - months(6))
  
  #combine OU and countryname for regional missions
  df_hfr_agg <- df_hfr_agg %>%
    mutate(countryname = ifelse(operatingunit == countryname, 
                                operatingunit, glue("{operatingunit}/{countryname}"))) %>% 
    rename(hfr_results = val)
  
  #aggregate to the date x orgunit x mech x indicator level
  df_hfr_comp <- df_hfr_agg %>% 
    mutate(has_value = ifelse(!is.na(hfr_results), 1L, 0L)) %>% 
    group_by(countryname, orgunituid, date, 
             mech_code, indicator, expect_reporting) %>%
    summarise(hfr_results = sum(hfr_results, na.rm = TRUE),
              has_value = sum(has_value, na.rm = TRUE)) %>% 
    ungroup()
  
  #classify sites and reporting
  df_hfr_comp <- df_hfr_comp %>% 
    group_by(date, orgunituid, mech_code, indicator) %>% 
    mutate(#has_hfr_reporting = ifelse(hfr_results > 0, TRUE, FALSE),
           has_hfr_reporting = ifelse(has_value > 0, TRUE, FALSE),
           is_datim_site = expect_reporting == TRUE) %>% 
    ungroup() %>% 
    tidylog::filter(!(has_hfr_reporting == TRUE & is_datim_site == FALSE)) 

  
  #aggregate across all sites in countryname across indicator and period
  df_hfr_comp <- df_hfr_comp %>% 
    group_by(countryname, indicator, date) %>% 
    summarise_at(vars(has_hfr_reporting, is_datim_site), sum, na.rm = TRUE) %>% 
    ungroup()
  
  #calculate completeness
  df_hfr_comp <- df_hfr_comp %>% 
    mutate(completeness = case_when(is_datim_site >  0 ~ has_hfr_reporting / is_datim_site)) 
  
  

# MUNGE HFR RESULTS -------------------------------------------------------

  df_hfr_viz <- df_hfr_agg %>%
    mutate(ind_alt = ifelse(indicator == "TX_MMD", glue("{indicator} {otherdisaggregate}"), indicator)) %>% 
    filter(ind_alt %ni% c("TX_MMD NA", "TX_MMD <3 months")) %>% 
    group_by(countryname, date, indicator, ind_alt) %>% 
    summarise(hfr_results = sum(val, na.rm = TRUE)) %>% 
    ungroup()
  
  df_hfr_viz <- df_hfr_viz %>% 
    mutate(group = case_when(ind_alt %in% c("HTS_TST_POS", "TX_NEW") ~ "Testing & Linkage",
                             str_detect(ind_alt, "TX") ~ "Treatment",
                             TRUE ~ ind_alt),
           ind_alt = factor(ind_alt, c("PrEP_NEW","VMMC_CIRC",
                                       "HTS_TST", "HTS_TST_POS", "TX_NEW",  
                                       "TX_CURR", "TX_MMD 3-5 months", "TX_MMD 6 months or more"))) %>% 
    group_by(countryname, group) %>% 
    mutate(grp_max = max(hfr_results, na.rm = TRUE)) %>% 
    ungroup() %>% 
    mutate(group = case_when(ind_alt == "HTS_TST" ~ "Testing & Linkage",
                             ind_alt %in% c("PrEP_NEW","VMMC_CIRC") ~"Prevention",
                             TRUE ~ "group"),
           fill_color_rug = case_when(ind_alt == "PrEP_NEW" ~ old_rose,
                                      TRUE ~ scooter_med),
           lab_com = "78%",
           lab_pd = case_when(date == max(date) ~ label_number_si()(hfr_results)))
    

# VIZ ---------------------------------------------------------------------

  
  df_hfr_viz %>% 
    # filter(str_detect(ind_alt, "TX_(C|M)")) %>% 
    ggplot(aes(date, hfr_results)) +
    geom_blank(aes(y = grp_max)) +
    geom_col(fill = scooter) +
    # geom_text(aes(label = lab_pd), vjust = -.8, na.rm = TRUE,
    #            family = "Source Sans Pro SemiBold", color = scooter) +
    # geom_label(aes(y = grp_max*1.15, label = lab_com),
    # geom_label(aes(y = 0, label = lab_com), 
    #            family = "Source Sans Pro", color = "#505050") +
    facet_wrap(~ind_alt, scales = "free_y") +
    coord_cartesian(clip = "off") +
    scale_y_continuous(label = label_number_si()) +
    scale_x_date(date_breaks = "1 month", date_labels = "%b") +
    labs(x = NULL, y = NULL) +
    si_style_ygrid()
  
  
 
  