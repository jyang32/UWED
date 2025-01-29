#### AUTHORS ####

# Lauren Woyczynski
# Lucas Owen
# Isaiah Wright
# June Yang

#Purpose: This script:
#   reads in addresses geocoded to lat/long
#   assigns each address to a census block group
#   matches addresses to voter data
#   predicts race based on last name and geography
#   saves voter file with geographic info and race predictions to *TBD*

#### SET LIBRARY PATH ####

#pull packages from the project site_libs - also install packages here if needed
# .libPaths('~/win/project/UWED/Master Team Folder/Code/site_libs/')

source("Keys.R")
source("00_helper_fns.R")

#### LOAD PACKAGES ####
## R version 4.1.3 ##

library(tidyverse)
library(tidycensus)
library(tigris)
library(sf)
library(sp)
library(janitor)
library(wru)
library(fs)
library(reader)
library(data.table)



#### READ IN ADDRESS W GPS COORD DATA ####

res <- readRDS("~/win/project/UWED/Master Team Folder/Data/Outputs/all_geocoded_addresses_Jan2025_batch.RDS")

#Convert to spatial pts object
res <- st_as_sf(res, coords = c("Longitude", "Latitude"))

#Read in most recent block group shapefile from ACS - using a random variable that I then drop
#Read cache files
#options(tigris_use_cache = TRUE)

bg <- get_acs(geography = 'block group',
              variables = 'B19013_001',
              state = state.abb, #WA?
              geometry = TRUE,
              key = census_api_key) %>%
    select(-variable, -estimate, -moe)

# Read in tribal boundaries shapefile
## Updated into 2024 shapefile

tribe_shp <- read_sf('~/win/project/UWED/Master Team Folder/Data/Inputs/Outside/tl_2024_us_aiannh/tl_2024_us_aiannh.shp')
#Note! There is an additional column from 2024 tribal shape file, "GEOIDFQ", which results in a mismatch between old and new Imputed files

wa <- states() %>% filter(NAME == "Washington")
st_crs(tribe_shp) <- st_crs(wa)
tribe_shp <- st_crop(tribe_shp, wa) %>% select(!GEOID)

#Make sure they have the same coordinate system
st_crs(res) <- st_crs(bg)
st_crs(tribe_shp) <- st_crs(bg)
sf_use_s2(FALSE)

#Do the overlay, separate geo information into columns
# Preprocess the `bg` dataset to split the NAME column into parts
bg <- bg %>%
    separate(
        NAME,
        into = c("Block Group", "Tract", "County", "State"),
        sep = "; ",
        remove = TRUE
    )

# Combine `bg` with tribal boundaries
## This step is to include additional attributes from the tribal shp,
## Additional rows will be added due to one block group may contain multiple tribes
bg_combined <- st_join(bg, tribe_shp, join = st_intersects)

# Perform a single spatial join with `res` (3 minutes in total from line 72)
res_loc <- st_join(res, bg_combined, join = st_intersects)

#Save all addresses located within census and tribal geographies

# st_write(res_loc, "~/win/project/UWED/Master Team Folder/Data/Outputs/all_address_census_tribe_joined_01272025.shp", append = F)

#### READ ISSUANCES ####
# Specify the file path - only read in new files
## Change into "R:/Project/" when using Windows
issuances_path <- "~/win/project/UWED/Master Team Folder/Data/Inputs/SoS/Ballot_Issuances/"

old_files <- read_csv(paste0(issuances_path,"current_file_names.csv"))

issuances_txt_files <- list.files(issuances_path, pattern = "\\.txt$", full.names = FALSE)

new_files <- issuances_txt_files[!issuances_txt_files %in% old_files$issuances_txt_files]

# run once, can be optimized
new_files_full <- paste0(issuances_path, new_files)

###

# Read all files, convert columns to character, and combine into one data frame
issuances <- bind_rows(lapply(new_files_full, function(file) {
    read_delim(file, delim = "|", na = "", col_types = cols(), quote = "") %>%
        mutate(across(everything(), as.character))
}))



### BEFORE COLLAPSING ADDRESS DATA, IDENTIFY OBS WITH ADDRESS MISSINGNESS ###
issuances <- issuances %>%
    mutate(Any_NA = ifelse(rowSums(is.na(select(., Address, City, State, Zip))) > 0, 1, 0))

table(issuances$Any_NA)
#7/20/24 - 10% of issuances have a missing address
#12/11/24 - .5% of issuances have some part of address missing (most of them international)
#1/27/25 - .1% of issuances have some part of address missing (most of them international)

# collapse address data
issuances <- issuances %>%
    unite(col = "Address", Address, City, State, Zip, sep = ', ', remove = F) %>%
    mutate(Address = str_squish(Address))


#now remove obs with missingness from res_loc

res_loc <- res_loc %>%
    anti_join(issuances %>% filter(Any_NA == 1), by = "Address")

#Specify the file path
#Windows user: "R:/Project/UWED/

vrdb_path <- "~/win/project/UWED/Master Team Folder/Data/Inputs/SoS/VRDB/"

old_files <- read_csv("~/win/project/UWED/Master Team Folder/Data/Inputs/SoS/VRDB/current_file_names.csv")

vrdb_txt_files <- list.files(vrdb_path, pattern = "\\.txt$")
vrdb_txt_files <- vrdb_txt_files[grepl("VRDB", vrdb_txt_files)]

new_files <- vrdb_txt_files[!vrdb_txt_files %in% old_files$vrdb_txt_files]

#### READ VRDB ####
# Initialize a list to store file names and guessed delimiters
delimiters_list <- vector("list", length(vrdb_txt_files))

# Loop over the files and guess delimiters
for (i in seq_along(vrdb_txt_files)) {
    guessed_delimiter <- guessDelimiter(paste0(vrdb_path, vrdb_txt_files[i]))

    # Store results in the list
    delimiters_list[[i]] <- list(FileName = basename(vrdb_txt_files[i]), GuessedDelimiter = guessed_delimiter)
}

# Convert the list to a data frame
delimiters_df <- do.call(rbind, lapply(delimiters_list, as.data.frame))

# Replace " " delimiters with "\t"
delimiters_df$GuessedDelimiter[delimiters_df$GuessedDelimiter == " "] <- "\t"

# Handle special case: Ensure the delimiter for the 4th file is "|", if there are 16 rows
## ??? Don't understand what this mean
if (nrow(delimiters_df) == 16) {
    delimiters_df$GuessedDelimiter[4] <- "|"
}
### Loading VRDB Files ###

# Define base path for VRDB files
vrdb_base_path <- "~/win/project/UWED/Master Team Folder/Data/Inputs/SoS/VRDB/"

# Read and process all files in one step
vrdb <- bind_rows(lapply(new_files, function(file_name) {
    # Find guessed delimiter or use default "|"
    guessed_delimiter <- delimiters_df$GuessedDelimiter[delimiters_df$FileName == file_name]
    guessed_delimiter <- ifelse(length(guessed_delimiter) == 0, "|", guessed_delimiter)

    # Read the data from the file
    vrdb_df <- read.table(paste0(vrdb_base_path, file_name),
                          sep = guessed_delimiter, header = TRUE, na.strings = "", fill = TRUE, quote = "", comment.char = "")

    # Extract year from file name and add as a column
    vrdb_df <- vrdb_df %>%
        mutate(Year = substr(file_name, 1, 4))

    # Convert all columns to character
    vrdb_df <- data.frame(lapply(vrdb_df, as.character), stringsAsFactors = FALSE)

    return(vrdb_df)
}))

#Put together address variable to match the geocoded data
vrdb <- vrdb %>%
    unite(., col = "Address",  RegStNum, RegStPreDirection, RegStName,  RegStType, RegStPostDirection, na.rm=TRUE, sep = " ") %>%
    mutate(Address = str_squish(Address))

### BEFORE COLLAPSING ADDRESS DATA, IDENTIFY OBS WITH ADDRESS MISSINGNESS ###
vrdb <- vrdb %>%
    mutate(Any_NA = ifelse(rowSums(is.na(select(., Address, RegCity, RegState, RegZipCode))) > 0, 1, 0))

table(vrdb$Any_NA)
# Jan 2025 batch has extremely small proportion of missing addresses (46 out of 5553k)

#collapse address
vrdb <- vrdb %>%
    unite(col = "Address", Address, RegCity, RegState, RegZipCode, sep = ', ', remove = F)


#now remove obs with missingness from res_loc

res_loc <- res_loc %>%
    anti_join(vrdb %>% filter(Any_NA == 1), by = "Address")

#Unique values from issuances and vrdb file on YEAR, NAME, VOTER ID, ADDRESS
#Look for duplicate voter IDs where one has an address and one does not, keep the one with an address

setDT(vrdb)
vrdb <- vrdb[, .(
    First.Name = FName,
    Last.Name = LName,
    Voter.ID = StateVoterID,
    Year,
    Address,
    State = RegState
)]

setDT(issuances)

# Rename and select columns to match vrdb
issuances <- issuances[, .(
    First.Name = `First Name`,
    Last.Name = `Last Name`,
    #Voter.ID = paste0("WA", `Voter ID`),  # Prefix Voter ID with "WA" Why?
    Voter.ID = `Voter ID`,
    Year = substr(Election, nchar(Election) - 3, nchar(Election)), # Extract Year from Election
    Address,
    State
)]

# Combine data tables
combined_df <- rbindlist( # Prefix of WA was dropped?
    list(vrdb, issuances),
    use.names = TRUE,
    fill = TRUE
)

# Remove duplicates
distinct_df <- unique(combined_df)
# JY question: what keys identify a unique voter? What if one voter has different addresses?
# distinct_df[order(-First.Name)][1:15]


# JY - no code optimization was performed starting this line and below
#### RACE IMPUTATION ####

#Merge and filter to the names with geocoded matches (should be all with a full sample)
#Change variable names for wru requirements

# Warning message:
# In left_join(., res_loc, by = "Address") :
    #Detected an unexpected many-to-many relationship between `x` and `y`.
# Row 228703 of `x` matches multiple rows in `y`.
# Row 1789323 of `y` matches multiple rows in `x`.
# If a many-to-many relationship is expected, set `relationship = "many-to-many"` to silence this warning.
states <- tibble(state.abb = state.abb, State = state.name)

geo_df <- distinct_df %>%
    rename(State.Abb = State) %>%
    left_join(res_loc, by = "Address") %>%
    rename(surname = Last.Name) %>%
    mutate(county = substr(GEOID, 3, 5),
           tract = substr(GEOID, 6, 11),
           block_group = substr(GEOID, 12, 12),
           Year = as.numeric(Year)) %>%
    mutate(State = str_trim(State)) %>%
    left_join(states) %>%
    rename(state = state.abb)

geo_df <- geo_df %>%
    filter(!str_detect(surname, "[^ -~]")) #remove any with non-ASCII characters

# #### REMOVE PREVIOUSLY IMPUTED INSTANCES ####
# # Read in previously imputed instances
# # Read as data.table for faster processing

previous_imputed <- setDT(readRDS("~/win/project/UWED/Master Team Folder/Data/Outputs/all_imputed.RDS"))

previous <- previous_imputed[
    , .(First.Name, surname, Voter.ID, Year, Address, State)
]

setDT(geo_df)

# Set keys for fast joins
setkey(previous, First.Name, surname, Voter.ID, Year, Address, State)

# Perform the anti-join
geo_df <- geo_df[!previous, on = .(First.Name, surname, Voter.ID, Year, Address, State)]

#### BRING IN CENSUS DATA ####

#Pull census data, save to folder if there is a new year to pull
#census_data <- get_census_data(key = key, states=state.abb, census.geo = "block_group", year = '2010')
#saveRDS(census_data, "R:/Project/UWED/Master Team Folder/Data/Inputs/Outside/census_data_for_imputation/census_2010.RDS")

census_data_2010 <-readRDS("~/win/project/UWED/Master Team Folder/Data/Inputs/Outside/census_data_for_imputation/census_2010.RDS")
census_data_2020 <-readRDS("~/win/project/UWED/Master Team Folder/Data/Inputs/Outside/census_data_for_imputation/census_2020.RDS")

# Impute race at the lowest geography, block group
# Separated out to refer to the closest census
# Years 2006-2015 should refer to 2010
# Years 2016-202+ should refer to 2020

geo_df_2010_bg <- geo_df %>%
    filter(Year %in% c(2006:2015)) %>%
    filter(!is.na(block_group)) %>%
    filter(State.Abb=="WA")
geo_df_2020_bg <- geo_df %>%
    filter(Year > 2015) %>%
    filter(!is.na(block_group)) %>%
    filter(State.Abb=="WA")


preds_2010_bg <- predict_race(voter.file=geo_df_2010_bg,
                              census.geo="block_group",
                              census.key=key,
                              year = '2020',
                              census.data = census_data_2020)


preds_2020_bg <- predict_race(voter.file=geo_df_2020_bg,
                              census.geo="block_group",
                              census.key=key,
                              year = '2020',
                              census.data = census_data_2020)
# JY 10282025: 122617 (9.5%) individuals' last names were not matched.

#Then, impute race for those who don't have tract predictions, but have county

geo_df_2010_cty <- geo_df %>%
    filter(Year %in% c(2006:2015)) %>%
    filter(is.na(block_group) & !is.na(county)) %>%
    filter(State.Abb=="WA")
geo_df_2020_cty <- geo_df %>%
    filter(Year > 2015) %>%
    filter(is.na(block_group) & !is.na(county)) %>%
    filter(State.Abb=="WA")

preds_2010_cty <- predict_race(voter.file=geo_df_2010_cty,
                               census.geo="county",
                               census.key=key,
                               year = '2010',
                               census.data = census_data_2010)

preds_2020_cty <- predict_race(voter.file=geo_df_2020_cty,
                               census.geo="county",
                               census.key=key,
                               year = '2020',
                               census.data = census_data_2020)

#Then, do just name for those remaining

geo_df_2010_name <- geo_df %>%
    filter(Year %in% c(2006:2015)) %>%
    filter((is.na(block_group) & is.na(county)) | State.Abb!="WA")
geo_df_2020_name <- geo_df %>%
    filter(Year > 2015) %>%
    filter((is.na(block_group) & is.na(county)) | State.Abb!="WA")

preds_2010_name <- predict_race(voter.file=geo_df_2010_name,
                                census.key=key,
                                surname.only = TRUE,
                                #surname.year = '2010', #this option appears to not exist
                                census.data = census_data_2010)

preds_2020_name <- predict_race(voter.file=geo_df_2020_name,
                                census.key=key,
                                surname.only = TRUE,
                                #surname.year = '2020', #this option appears to not exist
                                census.data = census_data_2020)

# JY 01282025: 32418 (10.3%) individuals' last names were not matched.

#### SAVE DATA ####

# Once the geocoding and imputation has been done successfully, save the text file that names all the files that have already been geocoded and imputed
write_csv(data.frame(issuances_txt_files), "~/win/project/UWED/Master Team Folder/Data/Inputs/SoS/Ballot_Issuances/current_file_names.csv")
write_csv(data.frame(vrdb_txt_files), "~/win/project/UWED/Master Team Folder/Data/Inputs/SoS/VRDB/current_file_names.csv")

#bind imputed race files that are in the environment
environment <- ls()
to_bind <- c("preds_2010_bg", "preds_2020_bg",
             "preds_2010_cty", "preds_2020_cty",
             "preds_2010_name", "preds_2020_name")
to_bind <- to_bind[to_bind %in% environment]
new_preds <- do.call(bind_rows, mget(to_bind)) # before binding with the previous_imputed

# Have to harmonize the columns before appending RDS and fst files
## Due to differences in 2018 and 2024 tribal shape files
## Removing the GEOIDFQ column and turn into data.table for faster processing

new_preds <- setDT(new_preds %>%
    select(-(GEOIDFQ)))

all_preds <- rbindlist(list(previous_imputed, new_preds),
                       use.names = TRUE,
                       fill = TRUE)

saveRDS(all_preds, "~/win/project/UWED/Master Team Folder/Data/Outputs/all_imputed_01282025.RDS")


