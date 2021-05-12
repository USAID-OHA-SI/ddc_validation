## PROJECT: ddc_validation
## AUTHOR:  B.Kagniniwa | USAID
## PURPOSE: Quick look at the structure of submissions
## LICENSE: MIT
## DATE:    2020-05-12

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

# VARIABLES --------------------------------------------------------

  # S3 Bucket
    bkt_name <- "gov-usaid"
    
  # Run 2021-05-10
  dir_raws <- "./Data/ddc-run-2021-05-10"
  dir_procs <- "./Data/ddc-run-2021-05-10/processed"
  
  # Sprint 25 - Test
  dir_files <- "./Data/ddc-test2"
  dir_proc_files <- "./Data/ddc-test2/processed"
    
  
# FUNCTIONS ----

  #' @title Create a Temp Directory
  #' @return Full folder path
  #'
  temp_folder <- function() {
    tmp <- fs::dir_create(fs::file_temp())
    cat("Folder at: ", tmp)
    
    return(tmp)
  }
  
  #' @title Destroy exising temp folder
  #' @param tmp
  #' 
  temp_destroy <- function(tmp){
    
    if (dir.exists(tmp)) {
      unlink(tmp, recursive = TRUE)
      cat("Temp folder destroyed: ", tmp)
    } 
    else {
      cat("Temp folder does not exist: ", tmp)
    }
  }
  
  #' @title Extract Metadata from HFR Submission
  #' 
  #' @param file_subm
  #' 
  #' @return df 
  #' 
  hfr_metadata <- function(file_subm) {
      
    print(file_subm)
    
    # Vars
    meta_tab <- 'meta'
    
    meta_vars <- c("Operating Unit/Country", 
                   "HFR FY and Period",
                   "Template version",
                   "Template type")
    
    # Extract tabs name
    sheets <- readxl::excel_sheets(path = file_subm)
    
    # Read metadata if tab exists
    if (meta_tab %in% str_to_lower(sheets)) {
      df_meta <- readxl::read_excel(path = file_subm, 
                                    sheet = 'meta',
                                    skip = 1,
                                    range = "B2:C5",
                                    col_names = c("var", "value"))
    } else {
      df_meta <- tibble(
        var = meta_vars,
        value = NA
      )
    }
      
    # Transform etadata
    df_meta <- df_meta %>% 
      mutate(filename = basename(file_subm),
             var = if_else(var == "Operating Unit", 
                           "Operating Unit/Country", var)) %>% 
      pivot_wider(names_from = var, values_from = value) %>% 
      janitor::clean_names() %>% 
      mutate(template_version = as.numeric(template_version)) %>% 
      rename_at(vars(starts_with("hfr_fy")), 
                str_sub, start = 1, end = 17)
      
    # Structure
    df_subm <- sheets %>% 
      str_subset("meta", negate = TRUE) %>% 
      map_dfr(function(s) {
        
        print(s)
        
        # Ignore the content of non-hfr tabs
        if (!str_detect(str_to_lower(str_trim(s, side = "both")), "^hfr")) {
          return(tibble(
            filename = basename(file_subm), 
            sheet = s,
            rows = NA,
            rows_empty = NA,
            cols = NA,
            cols_empty = NA
          ))
        }
        
        # Read tab content
        df <- readxl::read_excel(
          path = file_subm, 
          sheet = s, 
          skip = 1,
          col_types = "text"
        ) 
        
        # Count empty rows
        r_empty <- df %>% 
          #names() %>% 
          #first() %>% 
          filter(is.na(!!sym(names(df)[1]))) %>% 
          #filter(df, !!sym(names(.)) %>% 
          nrow()
        
        # Count empty columns
        c_empty <- df %>% 
          names() %>% 
          str_subset("\\d$") %>% 
          length()
        
        # Build output
        df <- df %>% 
          mutate(filename = basename(file_subm), 
                 sheet = s,
                 rows = nrow(df),
                 rows_empty = r_empty,
                 cols = ncol(df),
                 cols_empty = c_empty) %>% 
          select(filename:cols_empty) %>% 
          distinct()
      })
      
    # Merge meta to content structure
    df_meta <- df_meta %>% 
      left_join(df_subm, by = "filename")
    
    return(df_meta)
  }
  
  
# DATA ----
 
  
  df_files <- s3_objects(
    bucket = bkt_name,
    #prefix = "ddc/dev/raw/hfr/incoming",
    prefix = "ddc/dev/raw/hfr/archive",
    n = Inf,
    unpack_keys = TRUE
  ) %>% 
    filter(str_detect(str_to_lower(sys_data_object), "^test_")) 
  
  df_files %>% glimpse()
  
  df_proc_files <- s3_objects(
    bucket = bkt_name,
    prefix = "ddc/dev/processed/hfr/incoming",
    n = Inf,
    unpack_keys = TRUE
  ) %>% 
    filter(str_detect(str_to_lower(sys_data_object), "^test_")) 
  
  # Download files
  df_proc_files %>% 
    pull(key) %>% 
    map(~s3_download(bkt_name, .,
                     filepath = file.path(dir_proc_files, basename(.))))
  
  # Run
  df_raws %>% 
    pull(key) %>% 
    map(~s3_download(bkt_name, .,
                     filepath = file.path(dir_raws, basename(.))))
  
  df_procs %>% 
    pull(key_proc) %>% 
    map(~s3_download(bkt_name, .,
                     filepath = file.path(dir_procs, basename(.))))
  
# CHECKS ----
  
  # Metadata
  meta <- dir_raws %>% 
    list.files(pattern = ".xlsx$", full.names = TRUE) %>% 
    map_dfr(hfr_metadata)
  
  