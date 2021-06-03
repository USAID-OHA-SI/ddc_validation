## PROJECT: ddc_validation
## AUTHOR:  B.Kagniniwa | USAID
## PURPOSE: Quick look at the outputs
## LICENSE: MIT
## DATE:    2020-06-02

# LIBRARIES ----

  library(tidyverse)
  library(DBI)
  library(odbc)
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
  #conn %>% dplyr::db_has_table('mtcars')
  
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

