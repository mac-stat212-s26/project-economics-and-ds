## Last updated: April 24nd, 2026
## By David Rios

# ───────────────────────────────────────────────────────

## This is the code where we used census API via httr2 to pull public ABS data

## This is our data import file where we are extracting the ABS data set for the year 2018
## 2018 is the only year for which this data is publicly available; hence, our focus on this year.

## uploading libraries
library(httr2)
library(dplyr)
library(tidyr)
library(purrr)
library(janitor)

us_census_data_api_key <- Sys.getenv("API_KEY")
base_url <- "https://api.census.gov"

# this is a generic ABS fetcher

fetch_abs <- function(year, module, vars, geo = "state:*") {

  resp <- request(base_url) |>
    req_url_path_append(paste0("data/", year, "/", module)) |>
    req_url_query(
      get   = paste(vars, collapse = ","),
      `for` = geo,
      key   = us_census_data_api_key
    ) |>
    req_error(is_error = \(r) FALSE) |>
    req_perform()

  if (resp_status(resp) != 200) {
    message("Skipping ", year, "/", module, " — HTTP ", resp_status(resp))
    return(NULL)
  }

  resp |>
    resp_body_json(simplifyVector = TRUE) |>
    row_to_names(1) |>
    as_tibble() |>
    mutate(year = as.integer(year), .before = 1)
}

# 1. Pull 2018 technology data
# NOTE: we ran into troubles here and learned that
# each code variable has to have a _LABEL companion in the API

tech_2018_raw <- fetch_abs(
  year   = 2018,
  module = "abstcb",
  vars   = c(
    # outcomes
    "FIRMPDEMP", "EMP", "RCPPDEMP", "PAYANN",
    # technology category codes + their labels
    "TECHUSE",    "TECHUSE_LABEL",
    "TECHSELL",   "TECHSELL_LABEL",
    "IMPACTWF_U", "IMPACTWF_U_LABEL",
    "IMPACTWK_U", "IMPACTWK_U_LABEL",
    "FACTORS_U",  "FACTORS_U_LABEL",
    "MOTUSETECH", "MOTUSETECH_LABEL",
    # identifiers
    "NAICS2017", "NSFSZFI"
  )
)

# Inspecting what we got
glimpse(tech_2018_raw)

# key checks
tech_2018_raw |> count(TECHUSE_LABEL)    |> print(n = 50)
tech_2018_raw |> count(TECHSELL_LABEL)   |> print(n = 50)
tech_2018_raw |> count(IMPACTWF_U_LABEL) |> print(n = 50)
tech_2018_raw |> count(FACTORS_U_LABEL)  |> print(n = 50)
tech_2018_raw |> count(MOTUSETECH_LABEL) |> print(n = 50)


# 3. Pulling company summary panel (abscs) 2017–2021
# We use a slightly different variable list because 'abscs' sometimes uses different naming conventions for NAICS in early years.

cs_panel_raw <- map(
  2017:2021,
  \(yr) {
    message("Fetching year: ", yr)
    # Note: In some years of ABSCS, the variable is just 'NAICS2017'
    current_vars <- c("FIRMPDEMP", "EMP", "PAYANN", "RCPPDEMP", "NAICS2017")

    fetch_abs(
      year   = yr,
      module = "abscs",
      vars   = current_vars,
      geo    = "state:*" # This fetches all states
    )
  }
) |>
  list_rbind()

## Checking
if (!is.null(cs_panel_raw)) {
  cs_panel_raw |>
    filter(NAICS2017 == "00") |>
    mutate(EMP = as.numeric(EMP)) |>
    glimpse()
}


# Saving everything for easy access later on, as .csv
dir.create("data", showWarnings = FALSE)

write.csv(tech_2018_raw, "data/tech_2018_raw.csv", row.names = FALSE)
write.csv(cs_panel_raw,  "data/cs_panel_raw.csv",  row.names = FALSE)

## Note: the company size clean csv file is a state-by-sector panel dataset across 2017–2021.
## We aggregated by the first two digits of the NAICS2017 codebook.

