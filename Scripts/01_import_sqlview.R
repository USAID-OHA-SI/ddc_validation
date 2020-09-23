library(tidyverse)
library(googledrive)
library(glamr)


sqlview_fldr <- "1SgZkdG5uu-Syy6DYsNbTrzDqUmK4fSgF"
email <- ""

drive_auth(email)

(file <- drive_ls(as_id(sqlview_fldr), "2020.12") %>% pull(name))

import_drivefile(sqlview_fldr, file)
