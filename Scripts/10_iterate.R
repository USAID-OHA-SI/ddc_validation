## PROJECT:  HFR
## AUTHOR:   A.CHAFETZ | USAID
## PURPOSE:  iterate error reports
## LICENSE:  MIT
## DATE:     2020-11-19
## UPDATED:  2021-02-04


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


# GLOBAL VARIABLES --------------------------------------------------------
    
  #Gdrive report folder 
    gdrive_fldr <- "1UESgXMSNqQs4VlE7gicU0PQLnykvKB9s"
    

# AUTHENTICATE ------------------------------------------------------------
  
    load_secrets()



# DOWNLOAD ----------------------------------------------------------------

  #identify latest error report
    latest_err_rpt <- s3_objects(
      bucket = "gov-usaid",
      prefix = "ddc/uat/processed/hfr/outgoing/Detailed"
    ) %>%
      s3_unpack_keys() %>%
      filter(
        str_detect(
          str_to_lower(sys_data_object),
          pattern = "^detailed_error_output_.*.csv$")
      ) %>%
      pull(key) %>%
      sort() %>%
      last()
    
  #print latest
    basename(latest_err_rpt)
    
  #download
    s3_download(
      bucket = "gov-usaid",
      object = latest_err_rpt,
      filepath = file.path("Data", basename(latest_err_rpt)))
    
    
  #identify latest submission status report
    latest_err_status <- s3_objects(
      bucket = "gov-usaid",
      prefix = "ddc/uat/processed/hfr/outgoing/HFR_Submission"
    ) %>%
      s3_unpack_keys() %>%
      arrange(last_modified) %>% 
      pull(key) %>%
      last() 
    
  #print latest
    basename(latest_err_status)
    
  #download
    s3_download(
      bucket = "gov-usaid",
      object = latest_err_status,
      filepath = file.path("./out/DDC", basename(latest_err_status))
    )
    
    
# IMPORT ------------------------------------------------------------------

  #DDC error reports
    df_err <- read_csv(file.path("Data", basename(latest_err_rpt)), 
                       col_types = c(.default = "c"))
  
  #DDC submission status
    df_stat <- read_csv(file.path("./out/DDC", basename(latest_err_status)),
                        col_types = c(.default = "c"))
      
  #remove submitter from file name
    df_err <- df_err %>% 
      mutate(file_name = str_remove(file_name, " - .*"))

    df_stat <- df_stat %>% 
      mutate(file_name = str_remove(file_name, " - .*"))

 # ITERATE -----------------------------------------------------------------

  #using markdown/error_report.Rmd
    
  #pull distinct files with errors from the error report to iterate over
    filename <- df_err %>%
      filter(validation_type != "wrn_tmp_invalid-filename") %>% 
      distinct(file_name) %>% 
      pull()
  
  #files and file name
    reports <- tibble(
      output_file = glue(here("markdown","{filename}-ERROR_REPORT.docx")),
      params = map(filename, ~list(filename = .))
    )

  #create markdown folder under wd if it doesn't exist
    if(dir.exists("markdown") == FALSE){
      dir.create("markdown")
    }

  #create reports
    reports %>%
      pwalk(render, 
            input = here("R","error_reports.Rmd"))


# UPLOAD ------------------------------------------------------------------

  #open google drive folder to move files to archive
    drive_browse(as_id(gdrive_fldr))

  #identify reports that were created
    printed_reports <- list.files(here("markdown"), "docx", full.names = TRUE)
  
  #push to GDrive
    walk(.x = printed_reports,
         .f = ~ drive_upload(.x, 
                            path = as_id(gdrive_fldr), 
                            name = str_remove(basename(.x), ".docx"),
                            type = "document",
                            overwrite = TRUE)
         )




