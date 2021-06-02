## PROJECT: ddc_validation
## AUTHOR:  B.Kagniniwa | USAID
## PURPOSE: Quick look at the outputs
## LICENSE: MIT
## DATE:    2020-06-02

# LIBRARIES ----

library(tidyverse)
library(DBI)
library(odbc)
#library(dbplyr)
#library(RSQLite)
#library(RMySQL)
library(glue)
library(glamr)
library(janitor)
library(lubridate)


# PARAMS ----

  s3_bucket <- "gov-usaid"
  
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
  #' @param tables      List of tables to be updated
  #' 
  update_ref_tables <- function(connection, df_ref,
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
            s3_read_object(s3_bucket, .) %>% 
            as_tibble() %>% 
            mutate_at(vars(ends_with("tude")), as.double) %>% 
            dplyr::copy_to(connection, ., tbl_orgs, 
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
            s3_read_object(s3_bucket, .) %>% 
            dplyr::copy_to(conn, ., tbl_mechs, 
                           temporary = FALSE,
                           overwrite = TRUE)
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
            s3_read_object(s3_bucket, .) %>% 
            select(orgunituid, type, mech_code, indicator, 
                   expect_reporting, is_original, source) %>% 
            dplyr::copy_to(conn, ., tbl_sites, 
                           temporary = FALSE,
                           overwrite = TRUE)
        },
        error = function(err) {
          report_update_error(err, tbl_sites)
        }
      )
    }  
    
    ## targets
    if (tbl_sites %in% tables) {
      tryCatch(
        {
          df_ref %>% 
            filter(str_detect(key, "targets")) %>% 
            filter(last_modified == max(last_modified)) %>% 
            pull(key) %>% 
            s3_read_object(s3_bucket, .) %>% 
            select(fy, psnuuid, mech_code, 
                   indicator, sex, agecoarse, 
                   mer_targets) %>% 
            mutate(mer_results = NA_integer_) %>% 
            dplyr::copy_to(conn, ., tbl_targets, 
                           temporary = FALSE,
                           overwrite = TRUE)
        },
        error = function(err) {
          report_update_error(err, tbl_sites)
        }
      )
    }  
  }

# SETUP ----

  #set_service(service = "mysql", user = "host", pwd = "localhost")


# CONNECTIONS ----

  ## MySQL Connection
  # conn <- DBI::dbConnect(drv = RMySQL::MySQL(),
  #                        dbname = get_key("mysql", "db_hfr"),
  #                        host = get_key("mysql", "host"),
  #                        port = as.integer(get_key("mysql", "port")),
  #                        user = get_key("mysql", "username"),
  #                        password = get_key("mysql", "password"))
  
  
  ## SQLite Connection
  conn <- DBI::dbConnect(drv = RSQLite::SQLite(), 
                         dbname = get_key("sqlite", "db_hfr"))
  
  ## SQLite Connection with dbplyr
  # conn <- DBI::dbConnect(drv = RSQLite::SQLite(), 
  #                        dbname = "../../DATABASES/SQLite/hfrrr.sqlite")
  
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
  
  conn %>% DBI::dbExistsTable('mtcars')
  #conn %>% dplyr::db_has_table('mtcars')
  
  conn %>% DBI::dbListFields('mtcars')
  conn %>% dplyr::tbl('mtcars') %>% colnames()
  
  conn %>% dplyr::tbl('mtcars') %>% show_query()
  
  conn %>% 
    dplyr::tbl('mtcars') %>% 
    filter(cyl > 4) %>% show_query()
  
  conn %>% 
    dplyr::tbl('mtcars') %>% 
    count(name, cyl) %>% 
    arrange(desc(cyl), name) %>% 
    show_query() 
  
  conn %>% dplyr::tbl('mtcars') %>% explain()
  
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
  
  # TBL Outputs
  tbl_subms_status <- "subms_status"
  tbl_mechs_status <- "mechs_status"
  tbl_subms_errors <- "subms_errors"
  tbl_hfr_data <- "hfr_data"
  
  tbls_output <- c(tbl_subms_status, tbl_mechs_status, 
                   tbl_subms_errors, tbl_hfr_data)
  
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
                    tables = tbls_input)
  
  ## Orgs
  df_ref %>% 
    filter(str_detect(key, "orghierarchy")) %>% 
    filter(last_modified == max(last_modified)) %>% 
    pull(key) %>% 
    s3_read_object(s3_bucket, .) %>% 
    as_tibble() %>% 
    mutate_at(vars(ends_with("tude")), as.double) %>% 
    dplyr::copy_to(conn, ., tbl_orgs, 
                   temporary = FALSE,
                   overwrite = TRUE)
  
  tbl(conn, tbl_orgs) %>% glimpse()
  
  tbl(conn, tbl_orgs) %>% 
    filter(operatingunit == 'Nigeria') %>% 
    collect() %>%
    head() %>% 
    prinf()
  
  ## mechs
  df_ref %>% 
    filter(str_detect(key, "mechanisms")) %>% 
    filter(last_modified == max(last_modified)) %>% 
    pull(key) %>% 
    s3_read_object(s3_bucket, .) %>% 
    dplyr::copy_to(dest = conn, 
                   df = ., 
                   name = tbl_mechs, 
                   temporary = FALSE,
                   overwrite = TRUE,
                   indexes = list("operatingunit", "fundingagency", "mech_code"))
  
  tbl(conn, tbl_mechs) %>% glimpse()
  
  ## sites
  df_ref %>% 
    filter(str_detect(key, "sitelist")) %>% 
    filter(last_modified == max(last_modified)) %>% 
    pull(key) %>% 
    s3_read_object(s3_bucket, .) %>% 
    select(orgunituid, type, mech_code, indicator, 
           expect_reporting, is_original, source) %>% 
    dplyr::copy_to(conn, ., tbl_sites, 
                   temporary = FALSE,
                   overwrite = TRUE,
                   indexes = list("orgunituid", "mech_code", "indicator"))
  
  tbl(conn, tbl_sites) %>% glimpse()
  
  ## targets
  df_ref %>% 
    filter(str_detect(key, "targets")) %>% 
    filter(last_modified == max(last_modified)) %>% 
    pull(key) %>% 
    s3_read_object(s3_bucket, .) %>% 
    select(fy, psnuuid, mech_code, 
           indicator, sex, agecoarse, 
           mer_targets) %>% 
    mutate(across(starts_with("mer_"), as.integer)) %>% 
    dplyr::copy_to(conn, ., tbl_targets, 
                   temporary = FALSE,
                   overwrite = TRUE,
                   indexes = list("psnuuid", "mech_code", 
                                  "indicator", "sex", "agecoarse"))
  
  tbl(conn, tbl_targets) %>% glimpse()
  
  
  # DDC/HFR Outputs
  df_out <- s3_objects(
    bucket = s3_bucket, 
    prefix = "ddc/uat/processed/hfr/outgoing/",
    n = Inf,
    unpack_keys = TRUE
  )
  
  ## Submissions status
  df_out %>% 
    filter(str_detect(str_to_lower(sys_data_object), "hfr_submission_status")) %>% 
    filter(last_modified == max(last_modified)) %>% 
    pull(key) %>% 
    s3_read_object(s3_bucket, .) %>% 
    dplyr::copy_to(conn, ., tbl_subms_status, temporary = FALSE)
  
  tbl(conn, tbl_subms_status) %>% 
    glimpse()
  
  ## Mechanism details
  df_out %>% 
    filter(str_detect(str_to_lower(sys_data_object), "mechanism_detail")) %>% 
    filter(last_modified == max(last_modified)) %>% 
    pull(key) %>% 
    s3_read_object(s3_bucket, .) %>% 
    dplyr::copy_to(conn, ., tbl_mechs_status, temporary = FALSE)
  
  tbl(conn, tbl_mechs_status) %>% 
    glimpse()
  
  ## Submissions Detailed Errors
  df_out %>% 
    filter(str_detect(str_to_lower(sys_data_object), "detailed_error")) %>% 
    filter(last_modified == max(last_modified)) %>% 
    pull(key) %>% 
    s3_read_object(s3_bucket, .) %>% 
    dplyr::copy_to(conn, ., tbl_subms_errors, 
                   temporary = FALSE, 
                   overwrite = TRUE)
  
  tbl(conn, tbl_subms_errors) %>% glimpse()
  
  ## Output
  df_out %>% 
    filter(str_detect(str_to_lower(sys_data_object), "tableau_output")) %>% 
    filter(last_modified == max(last_modified)) %>% 
    pull(key) %>% 
    s3_read_object(s3_bucket, .) %>% 
    select(fy, date, hfr_pd, orgunituid, mech_code, 
           indicator, agecoarse, sex, otherdisaggregate, value = val) %>% 
    dplyr::copy_to(conn, ., tbl_hfr_data, 
                   temporary = FALSE, 
                   overwrite = TRUE,
                   indexes = list("hfr_pd", "orgunituid", "mech_code", 
                                  "indicator", "agecoarse", "sex"))
  
  tbl(conn, tbl_hfr_data) %>% glimpse()

