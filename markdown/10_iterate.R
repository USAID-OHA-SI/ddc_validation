## PROJECT:  HFR
## AUTHOR:   A.CHAFETZ | USAID
## PURPOSE:  iterate error reports
## LICENSE:  MIT
## DATE:     2020-11-19
## UPDATED:  2020-11-21


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
library(googlesheets4)

# GLOBAL VARIABLES --------------------------------------------------------

   #Gdrive error report file
    gdrive_ddc_errors <- "1fXQx74mj2VzkUxOKocxKqd8uuWGKDjUZwM7pEzt9V3A"
    gdrive_ddc_errors_fldr <- "1_jFJOi7fMJ3tABEbRoMWgQjdQBBC4h_c"
    ddc_errors_file <- "Extract_Error_2020-11-18_05_37.csv"
    
  #Gdrive report folder 
    gdrive_fldr <- "1UESgXMSNqQs4VlE7gicU0PQLnykvKB9s"

    

# AUTHENTICATE ------------------------------------------------------------
  
    #google sheets
      gs4_auth()
    
    #google drive
      drive_auth()
    

# IMPORT ------------------------------------------------------------------

  #download error report
      import_drivefile(gdrive_ddc_errors_fldr, 
                       filename =ddc_errors_file,
                       zip = FALSE)
      
  #DDC error reports
    # df_err <- read_sheet(as_sheets_id(gdrive_ddc_errors))
    df_err <- read_csv(file.path("Data", ddc_errors_file), col_types = c(.default = "c"))
  
      
  #remove submitter from file name
    df_err <- df_err %>% 
      mutate(file_name = str_remove(file_name, " - .*"))


# ITERATE -----------------------------------------------------------------

  #using error_report.Rmd
    
  #pull distinct files with errors from the error report to iterate over
    filename <- df_err %>% 
      filter(validation_result == "Error",
             str_detect(file_name, "FY21")) %>% 
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
            input = here("markdown","error_reports.Rmd"))


# UPLOAD ------------------------------------------------------------------



printed_reports <- list.files(here("markdown"), "docx", full.names = TRUE)

walk(.x = printed_reports,
     .f = ~ drive_upload(.x, 
                        path = as_id(gdrive_fldr), 
                        name = str_remove(basename(.x), ".docx"),
                        type = "document",
                        overwrite = TRUE)
     )




