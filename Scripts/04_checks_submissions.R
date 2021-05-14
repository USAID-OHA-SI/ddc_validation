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
    
  # Date 
    pdate <- '2021-05-10'
    
  # Run 2021-05-10
    dir_raws <- "./Data/ddc-run-2021-05-10"
    dir_procs <- file.path(dir_raws, "processed")
    dir_outs <- file.path(dir_raws, "outputs")
  
    
  
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
  
  #' @title Destroy exising folder
  #' @param dir Full folder path
  #' 
  dir_destroy <- function(dir){
    
    if (dir.exists(dir)) {
      unlink(dir, recursive = TRUE)
      cat("Temp folder destroyed: ", dir)
    }else {
      cat("Temp folder does not exist: ", dir)
    }
  }
  
  
  #' @title Build S3 Paths
  #' 
  #' @param dt_type Data type, raw or processed
  #' @param dt_route  Data route: incoming, outgoing, intermediate, ...
  #' @param system    Data system, Data Development Commons
  #' @param sys_env   System environment
  #' @param usecase   Use Case or subsystem
  #' @param prefix   Search key / pattern, optional
  #' 
  #' @examples 
  #' s3_path("raw", "incoming")
  #' 
  s3_path <- function(dt_type, dt_route,
                      system = "ddc",
                      sys_env = "uat",
                      usecase = "hfr",
                      prefix = NULL) {
    
    key <- tryCatch({
      
      # Build object path
      path <- glue::glue("{system}/{sys_env}/{dt_type}/{usecase}/{dt_route}/") %>% 
        stringr::str_to_lower(.)
      
      # Check path exists
      #message("Checking main path ...")
      
      exists <- aws.s3::object_exists(object = path, 
                                      bucket = "gov-usaid",
                                      key = glamr::get_s3key("access"),
                                      secret = glamr::get_s3key("secret"))
      
      if (!exists) {
        message("Main path does not exist.")
        return(NULL)
      }
      
      #message("Main path is valid.")

      # Append search pattern
      if (!is.null(prefix)) {
        #message("Appending search prefix ...")
        path <- path %>%
          base::paste0(prefix)
      }
      
      return(path)
    },
    error = function(err) {
      message("An error occured: ")
      print(err)
      return(NULL)
    },
    warning = function(wrn) {
      message("There is a warning: ")
      print(wrn)
      return(NA)
    },
    finally = {
      #message("Done!")
    })
    
    return(key)
  }
  
  
  #' @title Download DDC/HFR Outputs
  #' 
  #' @param df
  #' @param output
  #' 
  s3_outputs <- function(df, output = "errors") {
    
    outputs <- c("submissions", "mechanisms", "errors", "tableau")
    
    
    
    
  }
  
  #' @title Extract Metadata from HFR Submission
  #' 
  #' @param file_subm
  #' @param add_structure
  #' 
  #' @return df 
  #' 
  hfr_metadata <- function(file_subm, 
                           add_structure = TRUE) {
      
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
    
    # return metadata only
    if (!add_structure) {
      return(df_meta)
    }
      
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
 
  # Processed raw files
    df_raws <- s3_objects(
        bucket = bkt_name,
        #prefix = "ddc/uat/raw/hfr/archive",
        prefix = s3_path("raw", "archive"),
        n = Inf,
        unpack_keys = TRUE
      ) %>% 
      filter(str_detect(str_to_lower(sys_data_object), ".xlsx$")) %>% 
      mutate(last_modified = ymd(as.Date(last_modified))) %>% 
      filter(last_modified == max(last_modified))
    
    df_raws %>% glimpse()
  
  # Processed files
    df_procs <- s3_objects(
        bucket = bkt_name,
        prefix = s3_path("processed", "incoming"),
        n = Inf,
        unpack_keys = TRUE
      ) %>% 
      filter(str_detect(str_to_lower(sys_data_object), ".csv$"),
             !str_detect(str_to_lower(sys_data_object), 
                         "^hfr_lambda_checks_.*._lambda.csv$")) %>% 
      mutate(last_modified = ymd(as.Date(last_modified))) %>% 
      filter(last_modified == max(last_modified))
    
  # Outputs
    df_outs <- s3_objects(
        bucket = bkt_name,
        prefix = "ddc/uat/processed/hfr/outgoing",
        n = Inf,
        unpack_keys = TRUE
      ) %>% 
      filter(str_detect(str_to_lower(sys_data_object), ".csv$"))
    
  # Detailed Errors
    key_errors <- df_outs %>% 
      filter(str_detect(str_to_lower(sys_data_object),
                        pattern = "^detailed_error_output_.*.csv$")) %>% 
      filter(last_modified == max(last_modified)) %>% 
      pull(key)
    
  # Submission Status
    key_subms <- df_outs %>% 
      filter(str_detect(str_to_lower(sys_data_object),
                        pattern = "^hfr_submission_status_.*.csv$")) %>% 
      filter(last_modified == max(last_modified)) %>% 
      pull(key)
  # Mechanism Status
    key_mechs <- df_outs %>% 
      filter(str_detect(str_to_lower(sys_data_object),
                        pattern = "^hfr_mechanism_submission_status_.*.csv$")) %>% 
      filter(last_modified == max(last_modified)) %>% 
      pull(key)
    
  # Tableau Output
    key_tbw <- df_outs %>% 
      filter(str_detect(str_to_lower(sys_data_object),
                        pattern = "^hfr_tableau_.*.csv$")) %>% 
      filter(last_modified == max(last_modified)) %>% 
      pull(key)
  
    
# DOWNLOAD ----

  # raws files
    df_raws %>% 
      pull(key) %>% 
      map(~s3_download(bkt_name, .,
                       filepath = file.path(dir_raws, basename(.))))
    
  # process files
    df_procs %>% 
      pull(key_proc) %>% 
      map(~s3_download(bkt_name, .,
                       filepath = file.path(dir_procs, basename(.))))
    
    
  # outputs (errors)
    c(key_errors, key_mechs, key_subms) %>% 
      map(~s3_download(bucket = bkt_name, 
                       object = .,
                       filepath = file.path(dir_outs, basename(.))))
    
  # outputs (tableau)
    key_tbw %>% 
      s3_download(bucket = bkt_name, 
                  object = .,
                  filepath = file.path(dir_outs, basename(.)))
  
# CHECKS ----
  
  # Metadata
    meta <- dir_raws %>% 
      list.files(pattern = ".xlsx$", full.names = TRUE) %>% 
      map_dfr(hfr_metadata)
    
    meta_sum <- meta %>% 
      group_by(filename, operating_unit_country, hfr_fy_and_period, template_type) %>% 
      summarise_at(all_of(c("rows", "rows_empty", "cols", "cols_empty")), 
                   sum, na.rm = TRUE) %>% 
      ungroup()
      
    
    meta_only <- dir_raws %>% 
      list.files(pattern = ".xlsx$", full.names = TRUE) %>% 
      map_dfr(hfr_metadata, add_structure = F)
  
  # No Ou or Period period
    meta %>% 
      filter(is.na(operating_unit_country) | is.na(hfr_fy_and_period)) %>% 
      prinf()
  
  # Empty Rows
    meta %>% 
      filter(!is.na(operating_unit_country),
             !is.na(hfr_fy_and_period),
             rows > 0,
             rows_empty > 0) %>% 
      prinf()
  
  # Empty Columns
    df_metadata %>% 
      filter(!is.na(operating_unit_country),
             !is.na(hfr_fy_and_period),
             rows > 0,
             rows_empty > 0) %>% 
      prinf()
  
  # Subms Status vs Meta
    df_status <- list.files(
        dir_outs, 
        basename(key_subms), 
        full.names = TRUE) %>%
      vroom() %>% 
      filter(processed_date %in% c(pdate, ymd(pdate) + 1))
    
    df_status %>% glimpse()
    
  # Check Submission status for failed files
    df_status %>% 
      filter(str_detect(status, "Entire file failed")) %>% 
      left_join(meta_only, by = c("file_name" = "filename")) %>% 
      select(-c(status:records_not_processed_due_to_errors)) %>% 
      view()
    
    df_status %>% 
      filter(str_detect(status, "Entire file failed")) %>% 
      left_join(meta_sum, by = c("file_name" = "filename")) %>% 
      select(file_name, records_not_processed_due_to_errors:last_col()) %>%
      rowwise() %>% 
      mutate(rows_check = records_not_processed_due_to_errors == rows) %>% 
      ungroup() %>% 
      view()
    
    df_status %>% 
      filter(str_detect(status, "Processing completed with errors")) %>% 
      left_join(meta_sum, by = c("file_name" = "filename")) %>% 
      select(file_name, records_successfully_processed:last_col()) %>%
      rowwise() %>% 
      mutate(rows_check = sum(records_successfully_processed, 
                              records_not_processed_due_to_errors,
                              na.rm = TRUE) == rows) %>% 
      ungroup() %>% 
      view()
    
  # Check detailed status for processed with errors
    df_errors <- list.files(
      dir_outs, 
      basename(key_errors), 
      full.names = TRUE) %>%
      vroom() %>% 
      filter(processed_date %in% c(pdate, ymd(pdate) + 1))
    
    df_errors %>% glimpse()
    
      
    
  
  