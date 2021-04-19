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
    
  #Gdrive report folder (for upload) 
    gdrive_fldr <- "1UESgXMSNqQs4VlE7gicU0PQLnykvKB9s"
    
    bkt_name <- "gov-usaid"

# AUTHENTICATE ------------------------------------------------------------
  
    load_secrets()


# FUNCTIONS

    #' @title Read sheets from S3 Objects / Excel
    #' 
    #' @param bucket
    #' @param object_key
    #' @param access_key
    #' @param secret_key
    #' 
    s3_excel_sheets <- 
      function(bucket, object_key,
               access_key = NULL,
               secret_key = NULL) {
        
        # Notification
        print(basename(object_key))
        
        # Check keys
        if (is.null(access_key))
          access_key = glamr::get_s3key("access")
        
        if (is.null(secret_key))
          secret_key = glamr::get_s3key("secret")
        
        # Create excel temp file
        tmpfile <- base::tempfile(fileext = ".xlsx")
        
        # Save S3 Object to temp file
        s3_obj <- aws.s3::save_object(
          bucket = bucket,
          object = object_key,
          file = tmpfile,
          key = access_key,
          secret = secret_key
        )
        
        # Read sheets from file
        sheets <- readxl::excel_sheets(s3_obj)
        
        # Clean up 
        file.remove(tmpfile)
        
        return(sheets)
      }
    
    
    #' @title connect_text
    #' 
    #' @param txt       String charactors
    #' @param connector Charactor used as connector
    #' 
    connect_text <- function(txt, 
                             connections = "[^a-zA-Z0-9]",
                             connector = "_") {
      
      text <- base::sapply(txt, function(x) {
        x %>% 
          stringr::str_split(connections) %>% 
          base::unlist() %>% 
          stringi::stri_remove_empty() %>% 
          base::paste0(collapse = connector)
      }, USE.NAMES = FALSE)
      
      return(text)
    }

    
    #' @title Create a Temp Directory
    #' @return Full folder path
    #'
    temp_folder <- function() {
      tmp <- fs::dir_create(fs::file_temp())
      cat("Missing files saved to", tmp)
      
      return(tmp)
    }
    
# CONFIRM SUBMISSIONS ------------------------------------------

  # DDC / HFR Process Date
    
    pdate <- '2021-04-15'
  
    curr_date <- ymd(Sys.Date())
  
    curr_yr <- year(curr_date)
    curr_mth <- month(curr_date)
  
  
  # Raw submissions (latest files)
    df_raws <- s3_objects(
        bucket = bkt_name,
        prefix = "ddc/uat/raw/hfr/incoming",
        n = Inf,
        unpack_keys = TRUE
      ) 
  
    df_raws <- df_raws %>% 
      filter(last_modified >= pdate, nchar(sys_data_object) > 1) %>% 
      select(key, filename = sys_data_object, last_modified) %>% 
      arrange(filename) 

    
  # Raw Submissions - sheets
    df_sheets <- df_raws %>% 
      pull(key) %>%
      map_dfr(function(x) {
        tibble(
          key_raw = x,
          file_name = basename(x),
          sheet_name = s3_excel_sheets(bkt_name, x))
        
      }) %>% 
      filter(str_detect(str_to_lower(trimws(sheet_name)), "hfr")) %>% 
      mutate(
        sheet_name_clean = connect_text(
          str_replace_all(sheet_name, " |,", ""), # comma should be removed
          "_"),
        filename = paste0(str_remove(file_name, ".xlsx$"), "_", sheet_name_clean)
      ) %>% 
      arrange(file_name) 
    
    # HFR Processed
      df_procs <- glamr::s3_objects(
          bucket = bkt_name,
          prefix = "ddc/uat/processed/hfr/incoming/",
          n = Inf,
          unpack_keys = TRUE
        ) %>% 
        select(key_proc = key, filename = sys_data_object, last_modified) %>% 
        filter(last_modified >= pdate,
               str_detect(str_to_lower(filename), 
                          "wide.csv$|long.csv$|limited.csv$")) %>%
        arrange(filename) 
    
    # Compare S3 Sheets to Processed/hfr/incoming csv file
      df_sheets_check <- df_sheets %>% 
        left_join(
          df_procs %>% 
            mutate(
              filename = str_remove(filename, 
                                    "_Wide.csv$|_Long.csv|_Limited.csv$")
            ), 
          by = c("filename" = "filename")) %>% 
        rename(proc_filename = filename) %>% 
        arrange(file_name)
        
    # List of files not being processed
      df_sheets_check %>%
        filter(is.na(key_proc)) %>% 
        distinct(file_name) %>% 
        pull(file_name)
      
    # Download non-processed files
      fkeys <- df_sheets_check %>%
        filter(is.na(key_proc)) %>%  
        distinct(key_raw) %>% 
        pull(key_raw) 
      
      tmp <- temp_folder()
      
      fkeys %>% 
        map(~s3_download(bucket = bkt_name,
                         object = .x,
                         filepath = file.path(tmp, basename(.x))))
      
      raw_files <- list.files(tmp, ".xlsx$", full.names = TRUE)
      
    # Check meta tabs for unprocessed files
      df_metadata <- raw_files %>% 
        map_dfr(function(x) {
          
          meta_tab <- 'meta'
          vars <- c("Operating Unit/Country", 
                    "HFR FY and Period",
                    "Template version",
                    "Template type")
          
          sheets <- readxl::excel_sheets(path = x)
          
          if (meta_tab %in% str_to_lower(sheets)) {
            df_meta <- readxl::read_excel(path = x, 
                               sheet = 'meta',
                               skip = 1,
                               range = "B2:C5",
                               col_names = c("var", "value"))
          } else {
            df_meta <- tibble(
              var = vars,
              value = NA
            )
          }
          
          df_meta %>% 
            mutate(filename = basename(x),
                   var = if_else(var == "Operating Unit", 
                                 "Operating Unit/Country", 
                                 var)) %>% 
            pivot_wider(names_from = var, values_from = value) %>% 
            janitor::clean_names()
        }) 
      
    # Check extra columns / rows
      df_structure <- raw_files %>% 
        map_dfr(function(x) {
          
          
          sheets <- readxl::excel_sheets(path = x)
          
          df <- sheets %>% 
            str_subset("meta", negate = TRUE) %>% 
            map_dfr(function(y) {
              
              readxl::read_excel(path = x, sheet = y, skip = 1, col_types = "text") %>% 
                mutate(file_name = basename(x), sheet_name = y) %>% 
                pivot_longer(cols = -ends_with("_name"),
                             names_to = "variable", 
                             values_to = "value") 
                
              
            })
          
          return(df)
        }) 
      
    # Empty Column
      df_structure %>% 
        distinct(file_name, sheet_name, variable) %>% 
        filter(str_detect(variable, '^\\.')) %>% 
        prinf()
      
    # Empty Rows
      df_structure %>% 
        filter(variable == 'date', is.na(value)) %>% 
        prinf()
      
      #unlink(tmp, recursive = TRUE)
    
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
    
  # Check if non-processed files were reported in error outputs
    df_sheets_check %>%
      filter(is.na(key)) %>% 
      distinct(file_name) %>% 
      mutate(file_name = str_remove(file_name, " - .*")) %>% 
      left_join(df_stat, by = "file_name") %>% 
      filter(is.na(operatingunit)) %>% 
      pull(file_name)
    
    df_sheets_check %>%
      filter(is.na(key)) %>% 
      distinct(file_name) %>% 
      mutate(file_name = str_remove(file_name, " - .*")) %>% 
      left_join(df_err, by = "file_name") %>% 
      filter(is.na(processed_date)) %>% 
      pull(file_name)

 # ITERATE -----------------------------------------------------------------

  # TODO: add non-processed files to the error reports []
  
    
  #using markdown/error_report.Rmd
    
  #pull distinct files with errors from the error report to iterate over
    filename <- df_err %>%
      filter(validation_type != "wrn_tmp_invalid-filename",
             processed_date >= "2021-01-14") %>% 
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

  #create reports
    reports %>%
      pwalk(render, 
            input = here("Scripts","error_reports.Rmd"))


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




