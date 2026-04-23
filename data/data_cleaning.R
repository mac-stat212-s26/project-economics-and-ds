## Last updated: April 22nd, 2026
## By David Rios

# ───────────────────────────────────────────────────────

# This is our cleaning file
# tech_2018_raw and cs_panel_raw are single-year cross-sections;
# cs_panel_raw covers 2017-2021. The 2018 AI snapshot is joined onto the multi-year panel at the end.

library(dplyr)
library(tidyr)
library(stringr)
library(janitor)
library(readr)
library(here)

# Load raw data
tech_2018_raw <- readr::read_csv(
  here::here("data", "tech_2018_raw.csv"))

cs_panel_raw <- readr::read_csv(
  here::here("data", "cs_panel_raw.csv"))

# ── 1. Clean technology data

tech_2018_clean <- tech_2018_raw |>
  # avoid double counting
  filter(TECHUSE_LABEL != "All firms") |>
  # FIX: rename NAICS2017 → naics_sector BEFORE the case_when that references it
  rename(naics_sector = NAICS2017) |>
  mutate(
    tech_type = str_extract(TECHUSE_LABEL, "^[^:]+"),
    use_level = str_extract(TECHUSE_LABEL, "(?<=: ).+"),
    # numeric conversion
    across(c(FIRMPDEMP, EMP, RCPPDEMP, PAYANN), as.numeric),
    state = as.character(state)
  ) |>
  # Remove built-in totals that cause double counting
  filter(!use_level %in% c("Total Reporting", "Total use")) |>
  # Normalize text
  mutate(
    tech_type = str_to_lower(str_trim(tech_type))
  ) |>
  # usage classification
  mutate(
    used_tech = case_when(
      use_level %in% c("High use", "Moderate use", "Low use") ~ "Used",
      use_level == "Did not use"                               ~ "Did not use",
      use_level == "Tested, but did not use in production or service" ~ "Tested only",
      use_level == "Don't know"                                ~ "Unknown",
      TRUE ~ NA_character_
    )
  ) |>
  mutate(
    naics_sector = case_when(
      naics_sector %in% c("31","32","33","31-33") ~ "31-33",
      naics_sector %in% c("44","45","44-45")      ~ "44-45",
      naics_sector %in% c("48","49","48-49")      ~ "48-49",
      TRUE ~ naics_sector
    )
  ) |>
  select(
    year, state, naics_sector,
    tech_type, use_level, used_tech,
    n_firms  = FIRMPDEMP,
    n_emp    = EMP,
    revenue  = RCPPDEMP,
    payroll  = PAYANN
  )

# ── 2. AI-specific summary by state × NAICS

# Produces a 2018 cross-sectional AI adoption profile per state-industry cell.

ai_adoption_2018 <- tech_2018_clean |>
  filter(tech_type == "artificial intelligence",
  naics_sector != "00") |>
  select(state, naics_sector, use_level, n_firms) |>
  pivot_wider(
    names_from  = use_level,
    values_from = n_firms,
    names_prefix = "ai_firms_"
  ) |>
  clean_names() |>
  mutate(
    ai_firms_any_use =
      coalesce(ai_firms_high_use,     0) +
      coalesce(ai_firms_moderate_use, 0) +
      coalesce(ai_firms_low_use,      0),

    ai_firms_total =
      ai_firms_any_use +
      coalesce(ai_firms_did_not_use, 0),
    ai_adoption_rate = if_else(
      ai_firms_total > 0,
      ai_firms_any_use / ai_firms_total,
      NA_real_
    )
  )


# ── 3. Comparison table: all tech types by state
tech_adoption_summary <- tech_2018_clean |>
  filter(used_tech == "Used") |>
  group_by(state, naics_sector, tech_type) |>
  summarise(
    firms_using = sum(n_firms, na.rm = TRUE),
    emp_using   = sum(n_emp,   na.rm = TRUE),
    .groups = "drop"
  ) |>
  pivot_wider(
    names_from  = tech_type,
    values_from = c(firms_using, emp_using),
    names_sep   = "_"
  ) |>
  clean_names()



# ── 3b. Dimensional sub-tables from tech_2018_raw
# Why did firms adopt AI?
ai_motivations <- tech_2018_raw |>
  filter(
    MOTUSETECH_LABEL != "All firms",
    str_starts(MOTUSETECH_LABEL, "Artificial Intelligence")) |>
  rename(naics_sector = NAICS2017) |>
  mutate(
    motivation = str_extract(MOTUSETECH_LABEL, "(?<=: ).+"),
    across(c(FIRMPDEMP, EMP), as.numeric)
  ) |>
  select(state, naics_sector, motivation, n_firms = FIRMPDEMP, n_emp = EMP) |>
  filter(!str_detect(motivation, "Total Reporting"))

# What barriers did firms face?
ai_barriers <- tech_2018_raw |>
  filter(
    FACTORS_U_LABEL != "All firms",
    str_starts(FACTORS_U_LABEL, "Artificial Intelligence")
  ) |>
  rename(naics_sector = NAICS2017) |>
  mutate(
    barrier = str_extract(FACTORS_U_LABEL, "(?<=: ).+"),
    across(c(FIRMPDEMP, EMP), as.numeric)
  ) |>
  select(state, naics_sector, barrier, n_firms = FIRMPDEMP, n_emp = EMP) |>
  filter(!str_detect(barrier, "Total Reporting"))

# How did AI impact the workforce?
ai_workforce_impact <- tech_2018_raw |>
  filter(
    IMPACTWF_U_LABEL != "All firms",
    str_starts(IMPACTWF_U_LABEL, "Artificial Intelligence")
  ) |>
  rename(naics_sector = NAICS2017) |>
  mutate(
    impact = str_extract(IMPACTWF_U_LABEL, "(?<=: ).+"),
    across(c(FIRMPDEMP, EMP), as.numeric)
  ) |>
  select(state, naics_sector, impact, n_firms = FIRMPDEMP, n_emp = EMP) |>
  filter(!str_detect(impact, "Total Reporting"))

# Did firms sell/produce AI?
ai_production <- tech_2018_raw |>
  filter(
    TECHSELL_LABEL != "All firms",
    str_starts(TECHSELL_LABEL, "Artificial Intelligence")
  ) |>
  rename(naics_sector = NAICS2017) |>
  mutate(
    produced = str_extract(TECHSELL_LABEL, "(?<=: ).+"),
    across(c(FIRMPDEMP, EMP), as.numeric)
  ) |>
  select(state, naics_sector, produced, n_firms = FIRMPDEMP, n_emp = EMP) |>
  filter(!str_detect(produced, "Total Reporting"))

# Save dimensional sub-tables
write_csv(ai_motivations,     here::here("data", "ai_motivations.csv"))
write_csv(ai_barriers,        here::here("data", "ai_barriers.csv"))
write_csv(ai_workforce_impact,here::here("data", "ai_workforce_impact.csv"))
write_csv(ai_production,      here::here("data", "ai_production.csv"))


# ── 4. Clean company summary panel
cs_panel_clean <- readr::read_csv(
  here::here("data", "cs_panel_clean.csv")) |>
  mutate(
    state        = as.character(state),
    naics_sector = as.character(naics_sector),
    year         = as.integer(year),
    across(c(n_firms, n_emp, payroll, revenue), as.numeric)  # ← use the already-renamed names
  ) |>
  filter(!(n_firms == 0 & n_emp == 0 & payroll == 0)) |>    # ← same fix here
  mutate(
    payroll_per_emp = if_else(n_emp > 0, (payroll * 1000) / n_emp, NA_real_)
  ) |>
  filter(
    naics_sector %in% c(
      "11","21","22","23","31-33","42","44-45",
      "48-49","51","52","53","54","55","56",
      "61","62","71","72","81","92")
  )

if (interactive()) count(cs_panel_clean, year)


# ── 5. Merge: attach 2018 AI snapshot to multi-year panel

#because the median is zero we are computing threshold BEFORE
median_threshold <- ai_adoption_2018 |>
  filter(ai_adoption_rate > 0) |>
  pull(ai_adoption_rate) |>
  median(na.rm = TRUE)

merged_panel <- cs_panel_clean |>
  left_join(ai_adoption_2018, by = c("state", "naics_sector")) |>
  mutate(
    high_ai = case_when(
      is.na(ai_adoption_rate) | ai_adoption_rate == 0 ~ "No AI data",
      ai_adoption_rate >= median_threshold             ~ "High AI adoption",
      ai_adoption_rate <  median_threshold             ~ "Low AI adoption"
    )
  )



if (interactive()) {
  merged_panel |> count(year, high_ai)

  merged_panel |>
    group_by(year, high_ai) |>
    summarise(
      total_firms         = sum(n_firms,          na.rm = TRUE),
      total_emp           = sum(n_emp,            na.rm = TRUE),
      avg_payroll_per_emp = mean(payroll_per_emp, na.rm = TRUE),
      .groups = "drop"
    )
}

## MEDIAN =  0.03568342
median_threshold


# ── 6. Save all cleaned outputs
write_csv(tech_2018_clean,      here::here("data", "tech_2018_clean.csv"))
write_csv(ai_adoption_2018,     here::here("data", "ai_adoption_2018.csv"))
write_csv(tech_adoption_summary,here::here("data", "tech_adoption_summary.csv"))
write_csv(cs_panel_clean,       here::here("data", "cs_panel_clean.csv"))
write_csv(merged_panel,         here::here("data", "merged_panel.csv"))
