## PROJECT:  HFR
## AUTHOR:   A.CHAFETZ | USAID
## PURPOSE:  iterate error reports
## LICENSE:  MIT
## DATE:     2020-11-19
## UPDATED:  2023-01-18


# DEPENDENCIES ------------------------------------------------------------

library(tidyverse)
library(vroom)
library(Wavelength)
library(gagglr)
library(grabr)
library(lubridate)
library(glue)
library(scales)
library(knitr)
library(rmarkdown)
library(here)
library(googledrive)
library(fs)


# GLOBAL VARIABLES --------------------------------------------------------
    
  #Gdrive report folder (for upload) 
    gdrive_fldr <- as_id("1UESgXMSNqQs4VlE7gicU0PQLnykvKB9s")
    gdrive_archive <- as_id("1xebMO8O51_42ETGAAf1SEPCe0FspD-kR")
    
    bkt_name <- "gov-usaid"

# AUTHENTICATE ------------------------------------------------------------
  
    load_secrets()

    
# CONFIRM SUBMISSIONS ------------------------------------------

  # DDC / HFR Process Date
    
    pdate <- '2022-12-15'
    
# DOWNLOAD ----------------------------------------------------------------

  # #identify latest error report
  #   latest_err_rpt <- s3_objects(
  #       bucket = bkt_name,
  #       prefix = "ddc/uat/processed/hfr/outgoing/Detailed",
  #       unpack_keys = TRUE
  #     ) %>% 
  #     filter(
  #       str_detect(
  #         str_to_lower(sys_data_object),
  #         pattern = "^detailed_error_output_.*.csv$")
  #     ) %>%
  #     filter(last_modified == max(last_modified)) %>%
  #     pull(key) %>%
  #     sort() %>%
  #     last()
  #   
  # #print latest
  #   basename(latest_err_rpt)
  #   
  # #download
  #   s3_download(
  #     bucket = bkt_name,
  #     object = latest_err_rpt,
  #     filepath = file.path("Data", basename(latest_err_rpt)))
  #   
  #   
  # #identify latest submission status report
  #   latest_err_status <- s3_objects(
  #       bucket = bkt_name,
  #       prefix = "ddc/uat/processed/hfr/outgoing/HFR_Submission",
  #       unpack_keys = TRUE
  #     ) %>% 
  #     filter(last_modified == max(last_modified)) %>% 
  #     pull(key) 
  #   
  # #print latest
  #   basename(latest_err_status)
  #   
  # #download
  #   s3_download(
  #     bucket = "gov-usaid",
  #     object = latest_err_status,
  #     filepath = file.path("./out/DDC", basename(latest_err_status))
  #   )
    
    
# IMPORT ------------------------------------------------------------------

  #DDC error reports
    # df_err <- read_csv(file.path("Data", basename(latest_err_rpt)), 
    #                    col_types = c(.default = "c"))

    file.info(return_latest("../Wavelength/out/DDC/", "Detailed_Error"))$mtime
    df_err <- return_latest("../Wavelength/out/DDC/", "Detailed_Error") %>%
      read_csv(col_types = c(.default = "c"))

  #DDC submission status
    # df_stat <- read_csv(file.path("./out/DDC", basename(latest_err_status)),
    #                     col_types = c(.default = "c"))
    
    file.info(return_latest("../Wavelength/out/DDC/", "Submission_Status"))$mtime
    df_stat <- return_latest("../Wavelength/out/DDC/", "Submission_Status") %>%
      read_csv(col_types = c(.default = "c"))
      
  #remove submitter from file name
    df_err <- df_err %>% 
      mutate(file_name = file_name %>% 
               str_remove(".xlsx$") %>% 
               str_replace(" - ", " [") %>% 
               paste0("]"))

    df_stat <- df_stat %>% 
      mutate(file_name = file_name %>% 
               str_remove(".xlsx$") %>% 
               str_replace(" - ", " [") %>% 
               paste0("]"))
    
  # # Check if non-processed files were reported in error outputs
  #   df_sheets_check %>%
  #     filter(is.na(key_raw)) %>% 
  #     distinct(file_name) %>% 
  #     mutate(file_name = str_remove(file_name, " - .*")) %>% 
  #     left_join(df_stat, by = "file_name") %>% 
  #     filter(is.na(operatingunit)) %>% 
  #     pull(file_name)
  #   
  #   df_sheets_check %>%
  #     filter(is.na(key_raw)) %>% 
  #     distinct(file_name) %>% 
  #     mutate(file_name = str_remove(file_name, " - .*")) %>% 
  #     left_join(df_err, by = "file_name") %>% 
  #     filter(is.na(processed_date)) %>% 
  #     pull(file_name)

 # ITERATE -----------------------------------------------------------------

  # TODO: add non-processed files to the error reports []
  
    
  #using markdown/error_report.Rmd
    
  #pull distinct files with errors from the error report to iterate over
    filename <- df_err %>%
      filter(validation_type != "wrn_tmp_invalid-filename",
             processed_date >= pdate) %>% 
      distinct(file_name) %>% 
      pull()
  
  #files and file name
    reports <- tibble(
      output_file = glue(here("markdown","{filename}-ERROR_REPORT.docx")),
      params = map(filename, ~list(filename = .))
    )

  #create markdown folder under wd if it doesn't exist
    if(dir.exists("markdown") == FALSE) {
      dir.create("markdown")
    }

  #delete old files
    unlink("markdown/*")
    
  #create reports
    reports %>%
      pwalk(render, 
            input = here("Scripts","error_reports.Rmd"))


# UPLOAD ------------------------------------------------------------------

  #open google drive folder to move files to archive
    drive_browse(gdrive_fldr)

    old_files <- drive_ls(gdrive_fldr, type = "document")$id
    walk(old_files, 
         ~ drive_mv(.x, gdrive_archive)) 
    
  #rename any with an apostrophe in the filename
    file_rn <- list.files(path = "markdown","'", full.names = TRUE)
    if(length(file_rn) > 0)
      file.rename(file_rn, str_remove(file_rn, "'"))
    
  #identify reports that were created
    printed_reports <- list.files(here("markdown"), "docx", full.names = TRUE)
  
  #push to GDrive
    walk(.x = printed_reports,
         .f = ~ drive_upload(.x, 
                            path = gdrive_fldr, 
                            name = str_remove(basename(.x), ".docx"),
                            type = "document",
                            overwrite = TRUE)
         )




