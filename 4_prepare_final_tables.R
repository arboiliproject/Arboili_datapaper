# Load required packages
require(tidyverse)
require(data.table)

########## Load and prepare Google Trends data
FUs <- read.csv("data/br_federative_units.csv", colClasses = "factor")

# Identify latest Google Trends extraction folder
# gt_extraction_dirs <- substr(list.dirs("data/GT/"), 9, 19)
# gt_extraction_dirs <- gt_extraction_dirs[nchar(gt_extraction_dirs) == 10]
# last_gt_extraction_dir <- sort(gt_extraction_dirs)[length(gt_extraction_dirs)]
# dir_path <- paste0("./data/GT/", last_gt_extraction_dir)
dir_path <- paste0("./data/GT/query_results")

# Read GT time series data
gt_ts <- fread(paste0(dir_path, "/query_result.csv"))

# Clean and prepare GT data
gt_ts_final <- gt_ts %>%
  select(-all_of(c("sys_time", "time"))) %>%
  mutate(
    geo = substr(geo, 4, 5),
    geo = as.character(geo),
    geo = ifelse(geo == "", "BR", geo),
    location = factor(geo),
    date = as.Date(date),
    topic = factor(keyword)
  ) %>%
  select(date, location, topic, value) %>%
  complete(date, location, topic, fill = list(value = 0))

fwrite(gt_ts_final, file = "data/dash_data/GT_dataset.csv")

########## Load and process arbovirus data
dn_ts <- fread("data/SINAN/SINAN_dengue_cases.csv.gz", 
               select = c("ew_symptom_onset", "final_classification", "state_abbrev", "case_count"),
               colClasses = c("ew_symptom_onset" = "Date", "final_classification" = "numeric", 
                              "state_abbrev" = "factor", "case_count" = "numeric"))

ck_ts <- fread("data/SINAN/SINAN_chik_cases.csv.gz", 
               select = c("ew_symptom_onset", "final_classification", "state_abbrev", "case_count"),
               colClasses = c("ew_symptom_onset" = "Date", "final_classification" = "numeric", 
                              "state_abbrev" = "factor", "case_count" = "numeric"))

# Process dengue and chikungunya
dn_ts <- dn_ts %>%
  filter(final_classification != 5 | is.na(final_classification)) %>%
  complete(ew_symptom_onset, state_abbrev) %>%
  group_by(ew_symptom_onset, state_abbrev, .add = TRUE) %>%
  summarise(case_count = sum(case_count, na.rm = TRUE)) %>%
  ungroup() %>%
  rename(dengue_cases = case_count) %>%
  drop_na(ew_symptom_onset)

ck_ts <- ck_ts %>%
  filter(final_classification != 5 | is.na(final_classification)) %>%
  complete(ew_symptom_onset, state_abbrev) %>%
  group_by(ew_symptom_onset, state_abbrev, .add = TRUE) %>%
  summarise(case_count = sum(case_count, na.rm = TRUE)) %>%
  ungroup() %>%
  rename(chik_cases = case_count)

arbo_ts <- dn_ts %>%
  full_join(ck_ts, by = c("ew_symptom_onset", "state_abbrev")) %>%
  left_join(FUs %>% select(CODE, ABBREVIATION), by = c("state_abbrev" = "CODE")) %>%
  select(-state_abbrev) %>%
  rename(location = ABBREVIATION)

########## Load and process SARI (SIVEP-Gripe) data
sari_ts <- fread("data/SIVEP/SIVEP_cases.csv.gz",
                 select = c("ew_symptom_onset", "final_classification_new", "state_abbrev", "case_count"),
                 colClasses = c("ew_symptom_onset" = "Date", "final_classification_new" = "numeric", 
                                "state_abbrev" = "factor", "case_count" = "numeric")) %>%
  mutate(ew_symptom_onset = as.Date(ew_symptom_onset))

# Build disaggregated SARI datasets
all_sari_ts <- sari_ts %>%
  complete(ew_symptom_onset, state_abbrev) %>%
  group_by(ew_symptom_onset, state_abbrev, .add = TRUE) %>%
  summarise(case_count = sum(case_count, na.rm = TRUE)) %>%
  ungroup() %>%
  rename(sari_cases = case_count, location = state_abbrev)

covid_ts <- sari_ts %>%
  filter(final_classification_new %in% c(5, 0)) %>%
  complete(ew_symptom_onset, state_abbrev) %>%
  group_by(ew_symptom_onset, state_abbrev, .add = TRUE) %>%
  summarise(case_count = sum(case_count, na.rm = TRUE)) %>%
  ungroup() %>%
  rename(covid_cases = case_count, location = state_abbrev)

influenza_ts <- sari_ts %>%
  filter(final_classification_new %in% c(1, 0)) %>%
  complete(ew_symptom_onset, state_abbrev) %>%
  group_by(ew_symptom_onset, state_abbrev, .add = TRUE) %>%
  summarise(case_count = sum(case_count, na.rm = TRUE)) %>%
  ungroup() %>%
  rename(flu_cases = case_count, location = state_abbrev)

other_ts <- sari_ts %>%
  filter(final_classification_new %in% c(2, 4)) %>%
  complete(ew_symptom_onset, state_abbrev) %>%
  group_by(ew_symptom_onset, state_abbrev, .add = TRUE) %>%
  summarise(case_count = sum(case_count, na.rm = TRUE)) %>%
  ungroup() %>%
  rename(other_sari_cases = case_count, location = state_abbrev)

ignored_sari_ts <- sari_ts %>%
  filter(is.na(final_classification_new)) %>%
  complete(ew_symptom_onset, state_abbrev) %>%
  group_by(ew_symptom_onset, state_abbrev, .add = TRUE) %>%
  summarise(case_count = sum(case_count, na.rm = TRUE)) %>%
  ungroup() %>%
  rename(na_sari_cases = case_count, location = state_abbrev)

# Combine all SARI subtypes
all_sari_ts <- all_sari_ts %>%
  left_join(covid_ts, by = c("ew_symptom_onset", "location")) %>%
  left_join(influenza_ts, by = c("ew_symptom_onset", "location")) %>%
  left_join(other_ts, by = c("ew_symptom_onset", "location")) %>%
  left_join(ignored_sari_ts, by = c("ew_symptom_onset", "location"))

########## Merge all disease data
dis_ts <- arbo_ts %>%
  mutate(ew_symptom_onset = as.Date(ew_symptom_onset)) %>%
  replace_na(list(chik_cases = 0)) %>%
  mutate(arbo_cases = dengue_cases + chik_cases) %>%
  full_join(all_sari_ts, by = c("ew_symptom_onset", "location"))

# Create Brazil-wide aggregation
dis_br_ts <- dis_ts %>%
  group_by(ew_symptom_onset) %>%
  summarise(across(.cols = contains("case"), .fns = ~ sum(., na.rm = TRUE))) %>%
  ungroup()

# Add Brazil-wide row to the dataset
dis_ts <- dis_ts %>%
  bind_rows(dis_br_ts) %>%
  mutate(location = as.character(location)) %>%
  mutate(location = ifelse(is.na(location), "BR", location)) %>%
  relocate(ew_symptom_onset, location)

# Rename final column names
colnames(dis_ts) <- c("ew_symptom_onset", "location", "dengue_cases", "chik_cases", "arbo_cases",
                      "sari_cases", "covid_cases", "flu_cases", "other_sari_cases", "na_sari_cases")

# Save output file
fwrite(dis_ts, file = "data/dash_data/ArboILI_disease_table.csv")
