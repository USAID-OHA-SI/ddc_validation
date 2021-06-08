## PROJECT: ddc_validation
## AUTHOR:  B.Kagniniwa | USAID
## PURPOSE: Quick look at the outputs
## LICENSE: MIT
## DATE:    2020-06-02
## UPDATE:  2020-06-07

# LIBRARIES ----

  library(tidyverse)
  library(DBI)
  library(glue)
  library(glamr)
  library(janitor)
  library(lubridate)
  library(Wavelength)

# PARAMS ----

  # S3 bucket
  s3_bucket <- "gov-usaid"
  
  # Date - latest run
  pdate <- '2021-05-28'
  
  # Run 2021-05-10
  dir_raws <- paste0("./Data/ddc-run-", pdate)
  dir_procs <- file.path(dir_raws, "processed")
  dir_outs <- file.path(dir_raws, "outputs")
  
  
  db_host <- NULL
  db_port <- NULL
  
  db_user <- NULL
  db_pass <- NULL
  
  db_name <- ":memory:"

# FUNCTIONS ----

  #' @title Set service user/key values
  #' 
  #' @param service 
  #' @param user
  #' @param pwd
  #' 
  set_service <- function(service, user, 
                          pwd = NULL, 
                          keyr = NULL){
    
    if (is.null({{pwd}})) {
      keyring::key_set(service = {{service}},
                       username = {{user}},
                       keyring = {{keyr}})
    }
    else {
      keyring::key_set_with_value(service = {{service}},
                                  username = {{user}},
                                  password = {{pwd}},
                                  keyring = {{keyr}})
    }
  }
  
  
  #' @title Check service availability
  #' 
  #' @param service
  #' 
  check_service <- function(service) {
    {{service}} %in% keyring::key_list()$service
  }
  
  #' @title Service error message
  #' 
  #' @service
  #' 
  service_error <- function(service) {
    usethis::ui_oops({{service}})
    usethis::ui_stop("Service not available.")
  }
  
  
  #' @title Get account usernames
  #' 
  #' @param service
  #' 
  service_users <- function(service) {
    
    svc <- {{service}}
    
    if (!check_service(svc)) {
      usethis::ui_oops(sprintf("Service named '%s' is not available", svc))
      return(NULL)
    }
    
    # Return row=all, col=2
    keyring::key_list(svc)[,2]
  }
  
  #' Get account password
  #' @param service 
  #' 
  get_key <- function(service, user = NULL) {
    
    # Service name
    svc <- {{service}}
    
    if (!check_service(svc)) {
      usethis::ui_oops(sprintf("Service named '%s' is not available", svc))
      return(NULL)
    }
    
    # Service username
    if (is.null({{user}}) | !{{user}} %in% service_users(svc)) {
      usethis::ui_oops(sprintf("User '%s' is not available", user))
      return(NULL)
    }
    
    # Service password
    key <- keyring::key_get(service = svc, 
                            username = user)
    
    # value
    return(key)
  }
  
    
  
  #' @title Create db connection
  #' 
  #' @param server
  #' @param name
  #' 
  #' @param host
  #' @param port
  #'
  #' @param username
  #' @param password
  #' 
  connection <- function(server = 'sqlite', 
                         name = ":memory:",
                         host = NULL,
                         port = NULL,
                         username = NULL,
                         password = NULL) {
    
    # drivers
    #db_driver <- RMySQL::MySQL()
    # TODO: this is not working, find out why
    # db_driver <- case_when(
    #   server == 'sqlite' ~ RSQLite::SQLite(), #"SQLiteDriver",
    #   server == 'mysql' ~ RMySQL::MySQL(), #"MySQLDriver",
    #   server == 'postgres' ~ RPostgres::Postgres(), #"PgDriver",
    #   server == 'cassandra' ~ odbc::odbc(), #"OdbcDriver", #
    #   TRUE ~ odbc::odbc() #"OdbcDriver", #
    # )
    
    
    db_driver <- switch(server, 
                        'sqlite' = RSQLite::SQLite(), 
                        'mysql' = RMySQL::MySQL(), 
                        'postgres' = RPostgres::Postgres(), 
                        'cassandra' = odbc::odbc(), 
                        odbc::odbc())
    
    print(db_driver)
    
    # Special Case for sqlite
    if (server == 'sqlite') {
      conn <- DBI::dbConnect(drv = RSQLite::SQLite(), path = name)
      
      return(conn)
    }
    
    # host
    srv_host = ifelse(is.null(host), get_key('mysql', 'host'), host)
    
    # port
    srv_port = ifelse(is.null(port), as.integer(get_key('mysql', 'port')), port)
    
    # username / password
    user <- ifelse(is.null(username), get_key('mysql', 'username'), username)
    pass <- ifelse(is.null(password), get_key('mysql', 'password'), password)
    
    # Connection
    conn <- DBI::dbConnect(drv = db_driver, 
                           dbname = name,
                           host = srv_host, 
                           port = srv_port,
                           user = user, 
                           password = pass,
                           encoding = "latin1")
    
    return(conn)
  }
  
  #' @title Load data to SQLite DB
  #' 
  #' @param df
  #' @param connection
  #' @param tbl_name
  #' @param temporary
  #' @param overwrite
  #' 
  load_data <- function(df, connection, tbl_name, 
                        temporary = FALSE,
                        overwrite = TRUE) {
    
    dplyr::copy_to(connection, df, tbl_name, 
                   temporary = temporary,
                   overwrite = overwrite)
    
  }
  
  
  #' @title Describe Database
  #' 
  #' @param connection DB Connection
  #' @return DB Tables with column names and data types
  #' 
  db_describe <- function(connection) {
    conn %>% 
      DBI::dbListTables() %>% 
      str_subset("sqlite_", negate = TRUE) %>% 
      as_vector() %>% 
      map_dfr(function(.x) {
        
        stmt <- glue("SELECT * from {.x}")
        
        res <- dbSendQuery(conn, stmt)
        
        df <- res %>% 
          dbColumnInfo() %>% 
          as_tibble() %>% 
          mutate(table = .x) %>% 
          relocate(table, .before = 1)
        
        res %>% dbClearResult()
        
        return(df)
      }) 
  }
  
  
  #' @title Show update errors
  #' 
  #' @param err      Error message / object
  #' @param tbl_name Table Name
  #' 
  report_update_error <- function(err, tbl_name) {
    print(err)
    message(glue("Failed to updated table = {tbl_name}"))
  }
  
  
  #' @title Update HFR Ref Tables
  #' 
  #' @param connection  DB Connection
  #' @param df_ref      S3 Raw/Receiving objects
  #' @param bucket      S3 bucket name
  #' @param tables      List of tables to be updated
  #' 
  update_ref_tables <- function(connection, df_ref,
                                bucket = "gov-usaid",
                                tables = c("orghierachy", 
                                           "mechanisms",  
                                           "sitelist",   
                                           "targets")) {
    # Tables
    tbl_orgs <- "orghierachy"
    tbl_mechs <- "mechanisms"
    tbl_sites <- "sitelist"
    tbl_targets <- "targets"
    
    ## Orgs
    if (tbl_orgs %in% tables) {
      tryCatch(
        {
          df_ref %>% 
            filter(str_detect(key, "orghierarchy")) %>% 
            filter(last_modified == max(last_modified)) %>% 
            pull(key) %>% 
            s3_read_object(bucket = bucket, object = .) %>% 
            as_tibble() %>% 
            mutate_at(vars(ends_with("tude")), as.double) %>% 
            dplyr::copy_to(dest = connection, 
                           df = ., 
                           name = tbl_orgs, 
                           temporary = FALSE,
                           overwrite = TRUE)
        },
        error = function(err) {
          report_update_error(err, tbl_orgs)
        }
      )
    }
    
    ## mechs
    if (tbl_mechs %in% tables) {
      tryCatch(
        {
          df_ref %>% 
            filter(str_detect(key, "mechanisms")) %>% 
            filter(last_modified == max(last_modified)) %>% 
            pull(key) %>% 
            s3_read_object(bucket = bucket, object = .) %>% 
            dplyr::copy_to(dest = connection, 
                           df = ., 
                           name = tbl_mechs, 
                           temporary = FALSE,
                           overwrite = TRUE,
                           indexes = list("operatingunit", 
                                          "fundingagency", 
                                          "mech_code"))
        },
        error = function(err) {
          report_update_error(err, tbl_mechs)
        }
      )
    }
    
    ## sites
    if (tbl_sites %in% tables) {
      tryCatch(
        {
          df_ref %>% 
            filter(str_detect(key, "sitelist")) %>% 
            filter(last_modified == max(last_modified)) %>% 
            pull(key) %>% 
            s3_read_object(bucket = bucket, object = .) %>% 
            select(orgunituid, type, mech_code, indicator, 
                   expect_reporting, is_original, source) %>% 
            dplyr::copy_to(dest = connection, 
                           df = ., 
                           name = tbl_sites, 
                           temporary = FALSE,
                           overwrite = TRUE,
                           indexes = list("orgunituid", 
                                          "mech_code", 
                                          "indicator"))
        },
        error = function(err) {
          report_update_error(err, tbl_sites)
        }
      )
    }  
    
    ## targets
    if (tbl_targets %in% tables) {
      tryCatch(
        {
          df_ref %>% 
            filter(str_detect(key, "targets")) %>% 
            filter(last_modified == max(last_modified)) %>% 
            pull(key) %>% 
            s3_read_object(bucket = bucket, object = .) %>% 
            select(fy, psnuuid, mech_code, 
                   indicator, sex, agecoarse, 
                   mer_targets) %>% 
            mutate(mer_results = NA_integer_) %>% 
            dplyr::copy_to(conn, ., tbl_targets, 
                           temporary = FALSE,
                           overwrite = TRUE,
                           indexes = list("psnuuid", 
                                          "mech_code", 
                                          "indicator", 
                                          "sex", 
                                          "agecoarse"))
        },
        error = function(err) {
          report_update_error(err, tbl_targets)
        }
      )
    }  
  }
  
  
  #' @title Update HFR Error Tables
  #' 
  #' @param connection  DB Connection
  #' @param df_out      S3 Processed/Outgoing objects
  #' @param tables      List of tables to be updated
  #' 
  update_error_tables <- function(connection, df_out,
                                  bucket = "gov-usaid",
                                  tables = c("subms_status", 
                                             "mechs_status",  
                                             "subms_errors")) {
    # Tables
    tbl_subms_status <- "subms_status"
    tbl_mechs_status <- "mechs_status"
    tbl_subms_errors <- "subms_errors"
    
    ## Submission Status
    if (tbl_subms_status %in% tables) {
      tryCatch(
        {
          df_out %>% 
            filter(str_detect(str_to_lower(sys_data_object), 
                              "hfr_submission_status")) %>% 
            filter(last_modified == max(last_modified)) %>% 
            pull(key) %>% 
            s3_read_object(bucket = bucket, object = .) %>% 
            dplyr::copy_to(dest = connection, 
                           df = ., 
                           name = tbl_subms_status, 
                           temporary = FALSE,
                           overwrite = TRUE)
        },
        error = function(err) {
          report_update_error(err, tbl_subms_status)
        }
      )
    }
    
    # Submissions Mech Status
    if (tbl_mechs_status %in% tables) {
      tryCatch(
        {
          df_out %>% 
            filter(str_detect(str_to_lower(sys_data_object), 
                              "mechanism_detail")) %>% 
            filter(last_modified == max(last_modified)) %>% 
            pull(key) %>% 
            s3_read_object(bucket = bucket, object = .) %>% 
            dplyr::copy_to(dest = connection, 
                           df = ., 
                           name = tbl_mechs_status, 
                           temporary = FALSE,
                           overwrite = TRUE)
        },
        error = function(err) {
          report_update_error(err, tbl_mechs_status)
        }
      )
    }
    
    # Submission detailed errors
    if (tbl_subms_errors %in% tables) {
      tryCatch(
        {
          df_out %>% 
            filter(str_detect(str_to_lower(sys_data_object), 
                              "detailed_error")) %>% 
            filter(last_modified == max(last_modified)) %>% 
            pull(key) %>% 
            s3_read_object(bucket = bucket, object = .) %>% 
            dplyr::copy_to(dest = connection, 
                           df = ., 
                           name = tbl_subms_errors, 
                           temporary = FALSE, 
                           overwrite = TRUE)
        },
        error = function(err) {
          report_update_error(err, tbl_subms_errors)
        }
      )
    }
  }

  #' @title Check DDC Ouputs
  #' 
  #' @param response format of checks, options: checks, summary
  #' 
  check_outputs <- 
    function(filename, countryname, template_type, 
             fy, hfr_pd, sheet_name, proc_filename,
             response = "summary"){
  
    
    # Extra columns from processed files
    meta_cols <- c("sbmsn_id", "Row_number", "File_name")
    
    # Read raw data
    
    print(paste0(filename, " - ", sheet_name))
    
    df_raw <- readxl::read_excel(path = file.path(dir_raws, filename),
                                 sheet = sheet_name,
                                 skip = 1, 
                                 col_types = "text") %>% 
      select(-starts_with("..")) %>% 
      janitor::remove_empty(which = "rows", quiet = TRUE) %>% 
      janitor::remove_empty(which = "cols", quiet = TRUE)
    
    raw_cols <- names(df_raw)
    raw_ncols <- ncol(df_raw)
    raw_nrows <- nrow(df_raw)
    
    # Read proc data
    
    print(proc_filename)
    
    df_proc <- readr::read_csv(file = file.path(dir_procs, proc_filename),
                               skip = 1,
                               col_types = cols(.default = "c")) 
    
    # remove DDC Processing Info
    df_proc <- df_proc %>% 
      select(-last_col()) %>% # Note: this is a dynamic column (filename + checks)
      select(-meta_cols) 
    
    proc_cols <- names(df_proc)
    proc_ncols <- ncol(df_proc)
    proc_nrows <- nrow(df_proc)
    
    # Reshape tables
    if (template_type != "Long") {
      df_raw <- df_raw %>% Wavelength::hfr_gather()
      df_proc <- df_proc %>% Wavelength::hfr_gather()
    }
    
    # Raw dataset
    df_raw_sum <- df_raw %>% 
      Wavelength::hfr_fix_date() %>% 
      mutate(val = as.integer(val)) %>% 
      group_by(date, indicator) %>% 
      summarise(across(val, sum, na.rm = TRUE)) %>% 
      ungroup() %>% 
      mutate(type = "raw")
    
    # processed dataset
    df_proc_sum <- df_proc %>% 
      Wavelength::hfr_fix_date() %>% 
      mutate(val = as.integer(val)) %>% 
      group_by(date, indicator) %>% 
      summarise(across(val, sum, na.rm = TRUE)) %>% 
      ungroup() %>% 
      mutate(type = "processed")
    
    # output dataset
    sql_output <- glue("SELECT r.*, o.operatingunit, 
                         o.countryname, o.snu1, 
                         o.psnu, o.psnuuid
                         FROM {tbl_hfr_outputs} r
                         LEFT JOIN {tbl_orgs} o
                         ON r.orgunituid = o.orgunituid
                         WHERE countryname = '{countryname}'
                         AND fy = '{fy}' 
                         AND hfr_pd = '{hfr_pd}'")
    
    df_outputs <- tbl(conn, sql(sql_output)) %>%
      rename(val = value) %>%
      collect()
    
    #print(df_outputs %>% glimpse())
    
    df_out_sum <- df_outputs %>% 
      mutate(date = as.Date(date), 
             val = as.integer(val)) %>%
      group_by(date, indicator) %>% 
      summarise(across(val, sum, na.rm = TRUE)) %>% 
      ungroup() %>% 
      mutate(type = "output")
    
    match_proc_data <- all_equal(df_raw_sum, df_proc_sum)
    match_out_data <- all_equal(df_raw_sum, df_out_sum)
    
    # Checks
    
    if (response == "checks") {
      res <- tibble(
        raw_filename = filename,
        sheet_name = sheet_name,
        proc_filename = proc_filename,
        raw_nrows = raw_nrows,
        proc_nrows = proc_nrows,
        match_nrows = raw_nrows == proc_nrows,
        raw_ncols = raw_ncols,
        proc_ncols = proc_ncols,
        match_ncols = raw_ncols == proc_ncols,
        match_cols = identical(raw_cols, proc_cols),
        match_proc_data = match_proc_data,
        match_out_data = match_out_data
      )
      
      return(res)
    }
    
    
    # Summary
    
    if (response == "summary") {
      
      res <- df_raw_sum %>% 
        bind_rows(df_proc_sum) %>% 
        bind_rows(df_out_sum) %>% 
        pivot_wider(names_from = "indicator", values_from = "val") %>% 
        mutate(raw_filename = filename,
               proc_filename = proc_filename,
               sheet_name = sheet_name) %>% 
        relocate(raw_filename, proc_filename, sheet_name, .before = 1)
      
      return(res)
    }
    
    # other - all dfs
    return(c(df_raw, df_proc, df_out))
  }
  
# SETUP ----

  # Set new services
  #set_service(service = "mysql", user = "host", pwd = "localhost")
  #set_service(service = "sqlite", user = "db_hfr", pwd = "../../<path-to-your-dbs>/<db-filename>.db")

# CONNECTIONS ----

  ## MySQL Connection
  # conn <- DBI::dbConnect(drv = RMySQL::MySQL(),
  #                        dbname = get_key("mysql", "db_hfr"),
  #                        host = get_key("mysql", "host"),
  #                        port = as.integer(get_key("mysql", "port")),
  #                        user = get_key("mysql", "username"),
  #                        password = get_key("mysql", "password"))
  
  
  ## SQLite Connection
  # conn <- DBI::dbConnect(drv = RSQLite::SQLite(), 
  #                        dbname = "../../DATABASES/SQLite/hfrrr.sqlite")
  conn <- DBI::dbConnect(drv = RSQLite::SQLite(), 
                         dbname = get_key("sqlite", "db_hfr"))
  
  conn
  conn %>% summary()
  
  ## Disconnect
  #DBI::dbDisconnect(conn)

# EXPLORE DB ----

  # Create sample table from df
  
  #DBI::dbCreateTable(conn, "iris", iris)
  #DBI::dbWriteTable(conn, "iris", iris)
  
  # List db tables
  conn %>% DBI::dbListObjects()
  
  conn %>% DBI::dbListTables() 
  #conn %>% dplyr::db_list_tables()
  
  # Make sure table exists
  conn %>% DBI::dbExistsTable('mtcars')
  conn %>% DBI::dbExistsTable('iris')
  
  # List out table columns
  conn %>% DBI::dbListFields('mtcars')
  conn %>% dplyr::tbl('mtcars') %>% colnames()
  
  # Show SQL Query
  conn %>% dplyr::tbl('mtcars') %>% show_query()
  
  conn %>% 
    dplyr::tbl('mtcars') %>% 
    filter(cyl > 4) %>% 
    show_query()
  
  conn %>% 
    dplyr::tbl('mtcars') %>% 
    count(name, cyl) %>% 
    arrange(desc(cyl), name) %>% 
    show_query() 
  
  conn %>% 
    dplyr::tbl('mtcars') %>% 
    explain()
  
  conn %>% 
    tbl('mtcars') %>% 
    glimpse()
  
  conn %>% 
    db_describe() %>% 
    prinf()


# LAOD DATA ----

  # Tables
  
  # TBL Inputs
  tbl_orgs <- "orghierachy"
  tbl_mechs <- "mechanisms"
  tbl_sites <- "sitelist"
  tbl_targets <- "targets"
  
  tbls_input <- c(tbl_orgs, tbl_mechs, tbl_sites, tbl_targets)
  
  # TBL Error Outputs
  tbl_subms_status <- "subms_status"
  tbl_mechs_status <- "mechs_status"
  tbl_subms_errors <- "subms_errors"
  
  tbls_errors <- c(tbl_subms_status, tbl_mechs_status, tbl_subms_errors)
  
  # TBL Tbw output
  tbl_hfr_outputs <- "hfr_data"
  
  # Check existing tables
  conn %>% 
    db_describe() %>% 
    prinf()
  
  # S3 DATIM Ref tables
  df_ref <- s3_objects(
    bucket = s3_bucket, 
    prefix = "ddc/uat/raw/hfr/receiving/",
    n = Inf,
    unpack_keys = TRUE
  ) 
  
  # Update input tables [solve transactions issues]
  update_ref_tables(connection = conn,
                    df_ref = df_ref,
                    bucket = s3_bucket,
                    tables = tbls_input)
  
  ## Orgs
  tbl(conn, tbl_orgs) %>% glimpse()
  
  tbl(conn, tbl_orgs) %>% 
    filter(operatingunit == 'Nigeria') %>% 
    collect() %>%
    head() %>% 
    prinf()
  
  ## mechs
  tbl(conn, tbl_mechs) %>% glimpse()
  
  ## sites
  tbl(conn, tbl_sites) %>% glimpse()
  
  ## targets
  tbl(conn, tbl_targets) %>% glimpse()
  
  
  # DDC/HFR Outputs
  df_out <- s3_objects(
    bucket = s3_bucket, 
    prefix = "ddc/uat/processed/hfr/outgoing/",
    n = Inf,
    unpack_keys = TRUE
  )
  
  # Update error output tables 
  update_error_tables(connection = conn,
                      df_out = df_out,
                      bucket = s3_bucket,
                      tables = tbls_errors)
  
  ## Submissions status
  tbl(conn, tbl_subms_status) %>% glimpse()
  
  ## Mechanism details
  tbl(conn, tbl_mechs_status) %>% glimpse()
  
  ## Submissions Detailed Errors
  tbl(conn, tbl_subms_errors) %>% glimpse()
  
  ## HFR Outputs
  df_out %>% 
    filter(str_detect(str_to_lower(sys_data_object), "tableau_output")) %>% 
    filter(last_modified == max(last_modified)) %>% 
    pull(key) %>% 
    s3_read_object(s3_bucket, .) %>% 
    select(fy, date, hfr_pd, orgunituid, mech_code, 
           indicator, agecoarse, sex, otherdisaggregate, value = val) %>% 
    dplyr::copy_to(conn, ., tbl_hfr_outputs, 
                   temporary = FALSE, 
                   overwrite = TRUE,
                   indexes = list("hfr_pd", "orgunituid", "mech_code", 
                                  "indicator", "agecoarse", "sex"))
  
  tbl(conn, tbl_hfr_outputs) %>% glimpse()

# Validations ----
  
  # List of valid countries
  tbl(conn, tbl_orgs) %>% glimpse()
  
  countries <- tbl(conn, tbl_orgs) %>%
    filter(operatingunit != 6) %>% 
    distinct(operatingunit, countryname) %>% 
    collect() 
  
  # latest submission status
  tbl(conn, tbl_subms_status) %>% glimpse()
  
  tbl(conn, tbl_subms_status) %>% 
    collect() %>% 
    filter(str_detect(status, "errors")) %>% # Not available in SQL
    view()
  
  # Number of files by run
  tbl(conn, tbl_subms_status) %>% 
    distinct(processed_date, file_name) %>% 
    count(processed_date) %>% 
    arrange(desc(processed_date)) %>% 
    collect() %>% 
    prinf()
  
  # Subms status for latest run
  df_subms_status = tbl(conn, tbl_subms_status) %>% 
    filter(processed_date == max(processed_date)) %>% 
    collect() 
  
  # Metadata from latest subm files
  df_meta <- dir_raws %>% 
    list.files(pattern = ".xlsx$", full.names = TRUE) %>% 
    as_vector() %>% 
    map_dfr(hfr_metadata) 
  
  df_meta <- df_meta %>% 
    mutate(countryname = if_else(str_detect(operating_unit_country, "\\/"),
                                  str_extract(operating_unit_country, "(?<=\\/).*"),
                                  operating_unit_country),
           fy = if_else(!is.na(hfr_fy_and_period),
                   paste0("20", str_sub(hfr_fy_and_period, 3, 4)),
                   hfr_fy_and_period),
           hfr_pd = if_else(!is.na(hfr_fy_and_period),
                            str_sub(hfr_fy_and_period, 6, 8),
                            hfr_fy_and_period),
           hfr_pd = match(hfr_pd, month.abb) + 3,
           hfr_pd = if_else(hfr_pd > 12, hfr_pd - 12, hfr_pd),
           hfr_pd = str_pad(hfr_pd, width = 2, side = "left", pad = "0")
    ) %>% 
    relocate(countryname, .after = operating_unit_country) %>% 
    relocate(fy, hfr_pd, .after = hfr_fy_and_period) 
  
  # Cross Check metadata against subms statuses
  df_meta_checks <- df_subms_status %>% 
    left_join(df_meta, by = c("file_name" = "filename")) %>% 
    rename(filename = file_name) %>% 
    relocate(filename, .before = 1) %>% 
    mutate(
      cntry_check = countryname.x == countryname.y,
      cntry_match1 = countryname.x %in% countries$countryname,
      cntry_match2 = countryname.y %in% countries$countryname,
      fy_check = fy.x == fy.y,
      hfr_pd_check = hfr_pd.x == hfr_pd.y
    ) 

  # Double check metadata for these?
  df_meta_checks %>% 
    filter(
      cntry_check == FALSE |
      cntry_match1 == FALSE |
      cntry_match2 == FALSE |
      fy_check == FALSE |
      hfr_pd_check == FALSE
    ) %>% 
    select(filename, countryname.x, countryname.y, 
           ends_with("_match1"), # Countryname from Subm Status
           ends_with("_match2"), # Countryname from Metadata
           ends_with("_check")) %>% view()
  
  # Extract metadata from processed files
  df_proc_files <- dir_procs %>% 
    list.files(pattern = ".csv$", full.names = TRUE) %>% 
    map(basename) %>% 
    as_vector() %>% 
    as_tibble() %>% 
    rename(proc_filename = value) %>% 
    filter(
      str_detect(proc_filename, 
                 pattern = "^hfr_lambda_checks_.*", 
                 negate = TRUE),
      str_detect(proc_filename, 
                 pattern = ".*_meta.csv$", 
                 negate = TRUE)
    ) %>% 
    mutate(
      raw_filename = str_replace(proc_filename, "(?<=\\d{8}\\s\\d{6}).*", ".xlsx"),
      meta = str_extract(proc_filename, "(?<=\\d{8}\\s\\d{6}_).*(?=.csv)"),
      sheet_name = str_extract(proc_filename, "(?<=\\d{8}\\s\\d{6}_).*(?=_Wide.csv|_Long.csv|_Limited.csv)"),
      template_type = str_extract(proc_filename, "(?<=_)[^_]+(?=.csv)"),
      template_type = if_else(template_type == "Limited", 
                              "Wide - Limited", template_type)
    ) %>% 
    relocate(raw_filename, .before = 1)
  
  # Check proc files metadata against info in meta worksheet
  df_proc_checks <- df_meta %>% 
    rename(sheet_name = sheet) %>% 
    right_join(df_proc_files, 
               by = c("filename" = "raw_filename",
                      "template_type" = "template_type",
                      "sheet_name" = 'sheet_name')) 
  
  df_proc_checks %>% filter(is.na(meta))
  
  # Track data accross raw, processed and output files
  ddc_checks <- df_proc_checks %>% 
    filter(!is.na(meta)) %>% 
    #filter(filename == 'HFR_FY21_Apr_Zimbabwe_20210517 - Brilliant Nkomo 20210526 032430.xlsx') %>% 
    select(filename, countryname, template_type, 
           fy, hfr_pd, sheet_name, proc_filename) %>% 
    pmap_dfr(check_outputs, response = "checks")
  
  ddc_checks
  
  ddc_summary <- df_proc_checks %>% 
    filter(!is.na(meta)) %>% 
    #filter(filename == 'HFR_FY21_Apr_Zimbabwe_20210517 - Brilliant Nkomo 20210526 032430.xlsx') %>% 
    select(filename, countryname, template_type, 
           fy, hfr_pd, sheet_name, proc_filename) %>% 
    pmap_dfr(check_outputs, response = "summary")
  
  ddc_summary
  

  
  
    
  
  