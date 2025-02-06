#### AUTHORS ####

# Lauren Woyczynski
# Lucas Owen
# Isaiah Wright
# June Yang

#### SET LIBRARY PATH ####

#pull packages from the site_libs - also install packages here
.libPaths('R:/Project/UWED/Master Team Folder/Code/site_libs/')

#### LOAD PACKAGES ####

library(dplyr)
library(stringr)
library(lubridate)
library(tidyverse)

source("00_helper_fns.R")

#### LOAD BALLOT ISSUANCES FILES ####

#LOAD BALLOT ISSUANCES DATASETS

# Specify the file path
issuances_path <- "R:/Project/UWED/Master Team Folder/Data/Inputs/SoS/Ballot_Issuances"

# Get the names of all TXT files in the specified file path
issuances_txt_files <- list.files(issuances_path, pattern = "\\.txt$", full.names = TRUE)

# Initialize an empty list to store the data frames
issuances_list <- list()

# Loop over the file names
for (i in 1:length(issuances_txt_files)) {
    # Read the data from the file and create data frame
    issuances_df <- read.table(issuances_txt_files[i], sep="|", header=T, na.strings = "", fill=T, quote = "")

    # Add the data frame to the list
    issuances_list[[i]] <- issuances_df
}

# Convert every variable in each data frame to a character
issuances_list <- lapply(issuances_list, function(df) {
    data.frame(lapply(df, as.character), stringsAsFactors = FALSE)
})

# rbind all data frames in the list
ballot_issuances <- bind_rows(issuances_list)


#### VRDB GUESS DELIMITER ####

# Specify the file path
vrdb_path <- "R:/Project/UWED/Master Team Folder/Data/Inputs/SoS/VRDB"

# Get the names of all TXT files in the specified file path
vrdb_txt_files <- list.files(vrdb_path, pattern = "\\.txt$", full.names = TRUE)

# Initialize a data frame to store file names and their guessed delimiters
delimiters_df <- data.frame(FileName = character(), GuessedDelimiter = character(), stringsAsFactors = FALSE)

# Loop over the files and apply the guessDelimiter function
for (file_path in vrdb_txt_files) {
    guessed_delimiter <- guessDelimiter(file_path)

    # Add the file name and its guessed delimiter to the data frame
    delimiters_df <- rbind(delimiters_df, data.frame(FileName = basename(file_path), GuessedDelimiter = guessed_delimiter))
}

delimiters_df$GuessedDelimiter[delimiters_df$GuessedDelimiter==" "] <- "\t"
if(nrow(delimiters_df)>=16){delimiters_df$GuessedDelimiter[4] <- "|"}


#### Loading VRDB Files ####

# Specify the file path
vrdb_path <- "R:/Project/UWED/Master Team Folder/Data/Inputs/SoS/VRDB/"

vrdb_txt_files <- list.files(vrdb_path, pattern = "\\.txt$")
vrdb_txt_files <- vrdb_txt_files[grepl("VRDB", vrdb_txt_files)]

# Initialize an empty list to store the data frames
vrdb_list <- list()

#load the files
# Loop over the file names
for (i in 1:length(vrdb_txt_files)) {
    # Extract the file name
    file_name <- vrdb_txt_files[i]

    # Find the guessed delimiter for the current file
    guessed_delimiter <- delimiters_df$GuessedDelimiter[delimiters_df$FileName == file_name]

    # Check if a guessed delimiter was found, if not, default to "|"
    if (length(guessed_delimiter) == 0) {
        guessed_delimiter <- "|"
    }

    # Read the data from the file using the guessed delimiter
    vrdb_df <- read.table(paste0(vrdb_path, vrdb_txt_files[i]),
                          sep = guessed_delimiter, header = TRUE, na.strings = "", fill = TRUE, quote = "")

    # Filter for "Active" in the StatusCode column, removing NAs
    vrdb_df <- vrdb_df[!is.na(vrdb_df$StatusCode) & vrdb_df$StatusCode == "Active", ]

    # Add the data frame to the list
    vrdb_list[[i]] <- vrdb_df
}

# Convert every variable in each data frame to a character
vrdb_list <- lapply(vrdb_list, function(df) {
    data.frame(lapply(df, as.character), stringsAsFactors = FALSE)
})

#create year variable for each dataframe
# Function to extract and format date
extract_and_format_date <- function(file_names) {
    # Extract the date portion from the file name
    dates <- gsub("^(\\d{6,8})_VRDB_Extract\\.txt$", "\\1", file_names)

    # Check if day is missing (length is 6), then append '01' for the day
    dates <- ifelse(nchar(dates) == 6, paste0(dates, "01"), dates)

    # Convert to Date object
    as.Date(dates, format = ifelse(nchar(dates) == 8, "%Y%m%d", "%Y%m"))
}

# Apply the function to the vector of file names
formatted_dates <- extract_and_format_date(vrdb_txt_files)
formatted_dates
#apply to vrdb_list
for(i in seq_along(vrdb_list)) {
    vrdb_list[[i]]$Date <- formatted_dates[i]
}

# rbind all data frames in the list
vrdb <- bind_rows(vrdb_list)

#create year
library(lubridate)
vrdb$Year <- year(vrdb$Date)

#Put together address variable to match the geocoded data
vrdb <- vrdb %>%
    unite(., col = "Address",  RegStNum, RegStPreDirection, RegStName,  RegStType, RegStPostDirection, na.rm=TRUE, sep = " ") %>%
    mutate(Address = str_squish(Address)) %>%
    unite(col = "Address", Address, RegCity, RegState, RegZipCode, sep = ', ', remove = F)

rm(issuances_list, issuances_df, vrdb_df, vrdb_list)
gc()

#### LOAD REJECTIONS AND CURES FILES ####

# Specify the file path
rej_path <- "R:/Project/UWED/Master Team Folder/Data/Inputs/SoS/Rejections_Cures/"

# Get the names of all TXT files in the specified file path
rej_txt_files <- list.files(rej_path, pattern = "\\.txt$", full.names = TRUE)

# Initialize an empty list to store the data frames
rej_list <- list()

# Loop over the file names
for (i in 1:length(rej_txt_files)) {
    # Read the data from the file and create data frame
    rej_df <- read.table(rej_txt_files[i], sep="|", header=T, na.strings = "", fill=T, quote = "")

    # Add the data frame to the list
    rej_list[[i]] <- rej_df
}

# Convert every variable in each data frame to a character
rej_list <- lapply(rej_list, function(df) {
    data.frame(lapply(df, as.character), stringsAsFactors = FALSE)
})

# rbind all data frames in the list
rej <- bind_rows(rej_list)

rm(rej_df, rej_list)
gc()



#### LOAD SIGNATURE UPDATE FILES ####

# Specify the file path
sig_path <- "R:/Project/UWED/Master Team Folder/Data/Inputs/SoS/Signature_Update_Requests/"

# Get the names of all csv files in the specified file path
sig_files <- list.files(sig_path, pattern = "\\.csv$", full.names = FALSE)

# Subset the sig_files vector to include files with "Notices" in the name
sig_files <- sig_files[grepl("Notices", sig_files)]

# Initialize an empty list to store the data frames
sig_list <- list()

#load the files
# Loop over the file names
for (i in 1:length(sig_files)) {
    # Read the data from the file and create data frame
    sig_df <- read.csv(paste0(sig_path, sig_files[i]))

    # Add the data frame to the list
    sig_list[[i]] <- sig_df
}

# Convert every variable in each data frame to a character
sig_list <- lapply(sig_list, function(df) {
    data.frame(lapply(df, as.character), stringsAsFactors = FALSE)
})

# rbind all data frames in the list
sig <- bind_rows(sig_list)


rm(sig_df, sig_list)
gc()

#### LOAD RACE IMPUTATION RESULTS ####

race_imputation <- readRDS("R:/Project/UWED/Master Team Folder/Data/Outputs/all_imputed_01282025.RDS")

#add WA as prefix to voter IDs where missing
race_imputation$Voter.ID <- ifelse(!grepl("^WA", race_imputation$Voter.ID),
                                   sprintf("WA%09d", as.numeric(sub("^WA", "", race_imputation$Voter.ID))),
                                   race_imputation$Voter.ID)

race_imputation$geometry <- as.character(race_imputation$geometry)

race_imputation <- race_imputation %>%
    distinct(surname, Voter.ID, Year, Address, .keep_all=T)


#### CREATING ELECTORAL COMPETITIVENESS MEASURE ####

elec <- read.csv("R:/Project/UWED/Master Team Folder/Data/Inputs/Outside/county presidential results for competitiveness/countypres_2000-2020.csv", stringsAsFactors = F)

#set fips to 5-digit character
elec$county_fips <- sprintf("%05d", as.numeric(elec$county_fips))

#set mode to TOTAL
elec <- elec[elec$mode=="TOTAL",]

#subset to just democrat and republican
elec <- elec[elec$party %in% c("DEMOCRAT", "REPUBLICAN"),]

#remove NA fips
elec <- elec %>%
    filter(!grepl("NA", county_fips))

#subset to counties that have two observations per election (none that have only one, just some weird NA values)
elec <- elec %>%
    group_by(county_fips, year) %>%
    mutate(num_rows = n())
unique(elec$num_rows) #looks like we're good

#for each county-year combination, calculate competitiveness as absolute percent difference, using binary dem/rep votes
comp <- elec %>%
    group_by(county_fips, year) %>%  #for each county and year
    mutate(bin_percent = candidatevotes / sum(candidatevotes)) %>% #create binary percent for each candidate
    mutate(bin_competitiveness = max(unique(bin_percent)) - min(unique(bin_percent))) %>% #calculate binary competitiveness
    ungroup() %>%
    distinct(county_fips, year, .keep_all = T) %>% #subset to one observation per county
    select(county_fips, year, state_po, county_name, bin_competitiveness) #select the variables we care about

#convert to wide format
comp_wide <- comp %>%
    pivot_wider(names_from = year, values_from = bin_competitiveness,
                names_prefix = "bin_competitiveness_")

write.csv(comp_wide, "R:/Project/UWED/Master Team Folder/Data/Inputs/Outside/county presidential results for competitiveness/county_pres_competitiveness_2000-2020.csv", row.names=F)

comp <- comp_wide

#### LOADING USDA URBAN INFLUENCE CODES ####

urb1 <- read.csv("R:/Project/UWED/Master Team Folder/Data/Inputs/Outside/usda urban influence codes/UrbanInfluenceCodes2013.csv", stringsAsFactors = F)

#set fips to 5-digit character
urb1$FIPS <- sprintf("%05d", urb1$FIPS)

#how many counties?
length(unique(urb1$FIPS))

#select variables
colnames(urb1)
urb1 <- urb1[,colnames(urb1) %in% c("State", "County_Name", "Population_2010", "UIC_2013", "Description")]
colnames(urb1)[5] <- "UIC_2013_description"

#remove commas from population
urb1$Population_2010 <- as.numeric(gsub(",", "", urb1$Population_2010))


#### LOADING RURAL URBAN CONTINUUM CODES ####

urb2 <- read.csv("R:/Project/UWED/Master Team Folder/Data/Inputs/Outside/rural urban continuum codes/ruralurbancodes2013.csv", stringsAsFactors = F)

#set fips to 5-digit character
urb2$FIPS <- sprintf("%05d", urb2$FIPS)

#select variables
colnames(urb2)
urb2 <- urb2[,colnames(urb2) %in% c("State", "County_Name", "RUCC_2013", "Description")]
colnames(urb2)[4] <- "RUCC_2013_description"

#remove nas
urb2 <- urb2[!is.na(urb2$RUCC_2013),]



#### CLEAN VARIABLES FOR BALLOT ISSUANCES FILES ####

#record cleaned and uncleaned vars, record unclean values of vars to be cleaned
vars_issuances <- colnames(ballot_issuances)
vars_issuances_cleaned <- c("County", "Gender", "Election", "Ballot.Status", "Challenge.Reason", "Return.Method")
vars_issuances_uncleaned <- vars_issuances[!vars_issuances %in% vars_issuances_cleaned]
vars_issuances_cleaned <- as.data.frame(vars_issuances_cleaned)
colnames(vars_issuances_cleaned)[1] <- "Cleaned_Variable"
vars_issuances_cleaned$Pre_Cleaned_Values <- NA_character_
vars_issuances_cleaned$Cleaned_Values <- NA_character_
for(i in 1:nrow(vars_issuances_cleaned)){
    unique_values <- sort(unique(ballot_issuances[,vars_issuances_cleaned$Cleaned_Variable[i]]))
    vars_issuances_cleaned$Pre_Cleaned_Values[i] <- paste(unique_values, collapse = ", ")
}

#for each of the below variables, we determine normal values for that variable, and code any non-normal value as NA

#County
counties <- c("Kitsap", "Snohomish", "Lewis", "San Juan", "Whatcom", "Clark",
              "Walla Walla", "Spokane", "Island", "Cowlitz", "Whitman", "Douglas",
              "Benton", "Kittitas", "Franklin", "Mason", "Pierce", "Grant",
              "Klickitat", "Thurston", "Skagit", "Chelan", "Stevens", "Pacific",
              "Grays Harbor", "Ferry", "King", "Okanogan", "Lincoln", "Asotin",
              "Clallam", "Skamania", "Yakima", "Jefferson", "Adams", "Pend Oreille",
              "Columbia", "Wahkiakum", "Garfield")
ballot_issuances$County[!ballot_issuances$County %in% counties] <- NA_character_

#Gender
genders <- c("M", "F", "O", "U")
ballot_issuances$Gender[!ballot_issuances$Gender %in% genders] <- NA_character_

#Election
#replace spaces in election names. necessary for later when using these to name dataframes
ballot_issuances$Election <- gsub(" ", "_", ballot_issuances$Election, fixed = TRUE)
#sets election to only values with a four digit number at the end
ballot_issuances$Election <- ifelse(grepl("\\d{4}$", ballot_issuances$Election), ballot_issuances$Election, NA)

#Ballot.Status
standard_statuses <- c("Accepted", "Invalid", "Queued", "Received", "Rejected", "Sent", "Suspended", "Undeliverable")
ballot_issuances$Ballot.Status[!ballot_issuances$Ballot.Status %in% standard_statuses] <- NA_character_
ballot_issuances$Ballot.Status[ballot_issuances$Challenge.Reason=="Undeliverable"] <- "Undeliverable"

#Challenge.Reason
challenge_reasons <- c("Too Late", "Signature Does Not Match", "Undeliverable", "No Signature on File", "Invalid",
                       "Unsigned", "ID Required", "Id Required", "Canvassing Board", "Empty Envelope", "HOLD", "Ballot Style Change",
                       "Witness Signature Missing", "Other than Voter", "Deceased", "Marked Moved", "Power of Attorney",
                       "Other Than Voter", "No Signature on File", "Voter Name Change", "Review", "No Party", "Both Parties",
                       "No Party, No Sig", "Modified", "Advanced Special Ballot", "Hold", "Power Of Attorney")
ballot_issuances$Challenge.Reason[!ballot_issuances$Challenge.Reason %in% challenge_reasons] <- NA_character_
#correcting spellings
ballot_issuances$Challenge.Reason[ballot_issuances$Challenge.Reason=="Power Of Attorney"] <- "Power of Attorney"
ballot_issuances$Challenge.Reason[ballot_issuances$Challenge.Reason=="HOLD"] <- "Hold"
ballot_issuances$Challenge.Reason[ballot_issuances$Challenge.Reason=="Id Required"] <- "ID Required"
ballot_issuances$Challenge.Reason[ballot_issuances$Challenge.Reason=="Other Than Voter"] <- "Other than Voter"

#Return.Method
return_methods <- c("Email", "Drop Box", "Mail", "In Person", "Non-Standard Mail", "Non-Standard Dropbox", "Fax")
ballot_issuances$Return.Method[!ballot_issuances$Return.Method %in% return_methods] <- NA_character_



#record clean values of cleaned vars
for(i in 1:nrow(vars_issuances_cleaned)){
    unique_values <- sort(unique(ballot_issuances[,vars_issuances_cleaned$Cleaned_Variable[i]]))
    vars_issuances_cleaned$Cleaned_Values[i] <- paste(unique_values, collapse = ", ")
}

#### CREATE VARIABLES FOR BALLOT ISSUANCES FILES ####

#create year and election type variable
ballot_issuances$election_type <- NA_character_
ballot_issuances$Year <- NA_real_
#extract four digit number from Election variable to create year variable
ballot_issuances$Year <- str_extract(ballot_issuances$Election, "\\d{4}")
# Assign "Primary" to ballot_issuances$election_type where ballot_issuances$Election has first word "Primary"
ballot_issuances$election_type[grepl("^Primary", ballot_issuances$Election)] <- "Primary"
# Assign "General" to ballot_issuances$election_type where ballot_issuances$Election has first word "General"
ballot_issuances$election_type[grepl("^General", ballot_issuances$Election)] <- "General"
#Presidential_Primary
ballot_issuances$election_type[grepl("^Presidential_Primary", ballot_issuances$Election)] <- "Presidential_Primary"
#Special
ballot_issuances$election_type[grepl("^Special", ballot_issuances$Election)] <- "Special"
#Conservation
ballot_issuances$election_type[grepl("^Conservation", ballot_issuances$Election)] <- "Conservation"

#create Challenge.Reason_clean
ballot_issuances$Challenge_Reason_clean <- NA_character_
ballot_issuances$Challenge_Reason_clean[ballot_issuances$Challenge.Reason=="Too Late"] <- "Too Late"
ballot_issuances$Challenge_Reason_clean[ballot_issuances$Challenge.Reason=="Signature Does Not Match"] <- "Signature Does Not Match"
ballot_issuances$Challenge_Reason_clean[ballot_issuances$Challenge.Reason=="Unsigned"] <- "Unsigned"

#create rejection count variable
ballot_issuances <- ballot_issuances %>%
    group_by(Voter.ID) %>%
    mutate(Rejections = sum(Ballot.Status=="Rejected", na.rm=T))
ballot_issuances$Rejections[is.na(ballot_issuances$Rejections)] <- 0 #set NAs to zero
#new variable for none, one, or two or more
ballot_issuances$Rejection_cat <- NA_character_
ballot_issuances$Rejection_cat[ballot_issuances$Rejections==0] <- "None"
ballot_issuances$Rejection_cat[ballot_issuances$Rejections==1] <- "One"
ballot_issuances$Rejection_cat[ballot_issuances$Rejections>1] <- "Two or More"

#fill in DOB where missing
dob_na <- ballot_issuances[is.na(ballot_issuances$DOB),]
dob_not_na <- ballot_issuances[!is.na(ballot_issuances$DOB),]
dob_not_na <- dob_not_na %>%
    distinct(Voter.ID, .keep_all=T) %>%
    select(Voter.ID, DOB)
sum(dob_na$Voter.ID %in% dob_not_na$Voter.ID)/nrow(dob_na)
ballot_issuances <- ballot_issuances %>%
    left_join(dob_not_na, by = "Voter.ID", suffix = c(".na", ".not_na")) %>%
    mutate(DOB = ifelse(is.na(DOB.na), DOB.not_na, DOB.na)) %>%
    select(-DOB.na, -DOB.not_na)

#fill in DOB for 2023 where only year is available
dob_na <- ballot_issuances[nchar(ballot_issuances$DOB) == 4,]
dob_not_na <- ballot_issuances[nchar(ballot_issuances$DOB) != 4,]
dob_not_na <- dob_not_na %>%
    distinct(Voter.ID, .keep_all=T) %>%
    select(Voter.ID, DOB)
sum(dob_na$Voter.ID %in% dob_not_na$Voter.ID)/nrow(dob_na)
ballot_issuances <- ballot_issuances %>%
    left_join(dob_not_na, by = "Voter.ID", suffix = c(".na", ".not_na")) %>%
    mutate(DOB = ifelse(nchar(DOB.na) == 4, DOB.not_na, DOB.na)) %>%
    select(-DOB.na, -DOB.not_na)

#if we cant match from a previous year, then assume birthdate is Jan 1 for entries that only have a year
nrow(ballot_issuances %>% filter(nchar(DOB) == 4))
ballot_issuances <- ballot_issuances %>%
    mutate(DOB = case_when(
        nchar(DOB) == 4 ~ paste0(DOB, '-01-01'),
        TRUE ~ DOB
    ))


#calculate age
ballot_issuances$DOB <- as.Date(ballot_issuances$DOB)
ballot_issuances$Election_date <- NA_character_
# create election date
# Processing

ballot_issuances <- ballot_issuances %>%
    mutate(Election_date = lubridate::mdy(substr(Election, nchar(Election)-10, nchar(Election))))

ballot_issuances <- ballot_issuances %>%
    mutate(Age = as.numeric(Election_date-DOB)/365.25)

#create age, categorical variable
ballot_issuances$Age_cat <- NA_character_
ballot_issuances$Age_cat[ballot_issuances$Age<26] <- "25 and below"
ballot_issuances$Age_cat[ballot_issuances$Age>=26 & ballot_issuances$Age<36] <- "26-35"
ballot_issuances$Age_cat[ballot_issuances$Age>=36 & ballot_issuances$Age<46] <- "36-45"
ballot_issuances$Age_cat[ballot_issuances$Age>=46 & ballot_issuances$Age<56] <- "46-55"
ballot_issuances$Age_cat[ballot_issuances$Age>=56 & ballot_issuances$Age<66] <- "56-65"
ballot_issuances$Age_cat[ballot_issuances$Age>=66] <- "66+"


ballot_issuances$Voter_Election <- paste0(ballot_issuances$Voter.ID, ballot_issuances$Election)
#some voters have more than one ballot in an election. need to correct this

#create new variable for if voter_election has a ballot with non-NA received date.
#if so, put that voter election in ballot_issuances1
ballot_issuances <- ballot_issuances %>%
    group_by(Voter_Election) %>%
    mutate(Received_Date_not_NA = if_else(any(!is.na(Received.Date)), 1, 0)) %>%
    ungroup()
ballot_issuances1 <- ballot_issuances[ballot_issuances$Received_Date_not_NA==1,]
ballot_issuances2 <- ballot_issuances[ballot_issuances$Received_Date_not_NA==0,]


#for values where the Voter_Election group has a non-NA Received_Date value, take the obs with the max date
ballot_issuances1 <- ballot_issuances1[!is.na(ballot_issuances1$Received.Date),]
ballot_issuances1 <- ballot_issuances1 %>%
    group_by(Voter_Election) %>%
    filter(Received.Date == max(Received.Date)) %>%
    ungroup()

#for Voter_Election groups with all NA Received_Date values, do something different...

#separate NA Received_Date ballots out into status, and remove duplicates within Voter_Election groups
unique(ballot_issuances2$Ballot_Status)
ballot_issuances2_a <- ballot_issuances2[ballot_issuances2$Ballot.Status=="Accepted",]
ballot_issuances2_a <- ballot_issuances2_a %>%
    distinct(Voter_Election, .keep_all=T)
ballot_issuances2_r <- ballot_issuances2[ballot_issuances2$Ballot.Status=="Rejected",]
ballot_issuances2_r <- ballot_issuances2_r %>%
    distinct(Voter_Election, .keep_all=T)
ballot_issuances2_i <- ballot_issuances2[ballot_issuances2$Ballot.Status=="Invalid",]
ballot_issuances2_i <- ballot_issuances2_i %>%
    distinct(Voter_Election, .keep_all=T)
ballot_issuances2_sus <- ballot_issuances2[ballot_issuances2$Ballot.Status=="Suspended",]
ballot_issuances2_sus <- ballot_issuances2_sus %>%
    distinct(Voter_Election, .keep_all=T)
ballot_issuances2_remaining <- ballot_issuances2[!ballot_issuances2$Ballot.Status %in% c("Accepted", "Rejected", "Invalid","Suspended"),]
ballot_issuances2_remaining <- ballot_issuances2_remaining %>%
    distinct(Voter_Election, .keep_all=T)

#rbind in the order of priority for having in the final dataset
ballot_issuances_final <- rbind(ballot_issuances1, ballot_issuances2_a, ballot_issuances2_r, ballot_issuances2_i, ballot_issuances2_sus, ballot_issuances2_remaining)

#remove duplicate voter_elections once again
#n count is 28,637,935 before doing so
ballot_issuances_final <- ballot_issuances_final %>%
    distinct(Voter_Election, .keep_all=T)
#n count is 28,429,981 afterwards

#switch ballot_issuances_final back to ballot_issuances, remove all others
ballot_issuances <- ballot_issuances_final
rm(ballot_issuances_final, ballot_issuances1, ballot_issuances2, ballot_issuances2_a, ballot_issuances2_i, ballot_issuances2_r, ballot_issuances2_sus, ballot_issuances2_remaining)
gc()

#create voted
ballot_issuances$Voted <- 0
ballot_issuances$Voted[ballot_issuances$Ballot.Status %in% c("Accepted", "Received", "Invalid", "Rejected")] <- 1

#create rejected
ballot_issuances$Rejected <- NA_real_
ballot_issuances$Rejected[ballot_issuances$Voted==1] <- 0
ballot_issuances$Rejected[ballot_issuances$Ballot.Status=="Rejected"] <- 1

#create late
ballot_issuances$Late <- NA_real_
ballot_issuances$Late[ballot_issuances$Ballot.Status=="Rejected"] <- 0
ballot_issuances$Late[ballot_issuances$Challenge.Reason=="Too Late"] <- 1

#create signature issue
ballot_issuances$Sig_issue <- NA_real_
ballot_issuances$Sig_issue[ballot_issuances$Ballot.Status=="Rejected"] <- 0
ballot_issuances$Sig_issue[ballot_issuances$Challenge_Reason_clean %in% c("Signature Does Not Match", "Unsigned")] <- 1


### CALCULATE BALLOT RECEIPT TIMING RELATIVE TO ELECTION ###

#convert to date class

ballot_issuances$Received.Date <- as.POSIXct(ballot_issuances$Received.Date, format="%Y-%m-%d %H:%M:%S")
ballot_issuances$Election_date <- paste(ballot_issuances$Election_date, "00:00:00")
ballot_issuances$Election_date <- as.POSIXct(ballot_issuances$Election_date, format="%Y-%m-%d %H:%M:%S")

#received to election
ballot_issuances$received_to_election <- ballot_issuances$Received.Date - ballot_issuances$Election_date
#set to hours
units(ballot_issuances$received_to_election) <- "hours"

#### MERGE IN COMPETITIVENESS, RURAL/URBAN CODES ####

#convert county variables to match
ballot_issuances$County <- toupper(ballot_issuances$County) #all caps
urb1$County_Name <- toupper(urb1$County_Name) #all caps
urb2$County_Name <- toupper(urb2$County_Name) #all caps
#remove "COUNTY" from County_Name in urb1 and urb2
urb1$County_Name <- gsub(" COUNTY", "", urb1$County_Name)
urb2$County_Name <- gsub(" COUNTY", "", urb2$County_Name)

#subset each of comp, urb1, and urb2 to just washington state
comp <- comp[comp$state_po=="WA",]
urb1 <- urb1[urb1$State=="WA",]
urb2 <- urb2[urb2$State=="WA",]

#check to make sure names are consistent with comp, urb1, and urb2
counties <- sort(unique(ballot_issuances$County))
sum(counties==sort(unique(comp$county_name)))==39 #should be true
sum(counties==sort(unique(urb1$County_Name)))==39 #should be true
sum(counties==sort(unique(urb2$County_Name)))==39 #should be true

#looks like we're in the clear

#create rural/urban variable
urb2$RUCC_2013_description <- sapply(strsplit(urb2$RUCC_2013_description, " "), '[', 1)
urb1$UIC_2013_description <- sapply(strsplit(urb1$UIC_2013_description, " "), '[', 1)
unique(urb1$UIC_2013_description)
urb1$UIC_2013_description[urb1$UIC_2013_description=="Small-in"] <- "Small Metro"
urb1$UIC_2013_description[urb1$UIC_2013_description=="Large-in"] <- "Large Metro"

### MAKE SURE EACH COUNTY HAS A FIPS CODE ###
# county_codes <- ballot_issuances[,c("County", "county_fips")] %>%
#     distinct(county_fips, .keep_all=T)
# county_codes <- county_codes[!is.na(county_codes$county_fips),]
# ballot_issuances <- ballot_issuances[,colnames(ballot_issuances)[!colnames(ballot_issuances) %in% c("county_fips")]]
# ballot_issuances <- left_join(ballot_issuances, county_codes)

#once we are, join on state and county name
ballot_issuances <- left_join(ballot_issuances, comp, by=c("State"="state_po", "County"="county_name"))
ballot_issuances <- left_join(ballot_issuances, urb1, by=c("State"="State", "County"="County_Name"))
ballot_issuances <- left_join(ballot_issuances, urb2, by=c("State"="State", "County"="County_Name"))

#### CLEAN VRDB FILES ####

#record cleaned and uncleaned vars, record unclean values of vars to be cleaned
vars_vrdb <- colnames(vrdb)
vars_vrdb_cleaned <- c("NameSuffix", "Gender")
vars_vrdb_uncleaned <- vars_vrdb[!vars_vrdb %in% vars_vrdb_cleaned]
vars_vrdb_cleaned <- as.data.frame(vars_vrdb_cleaned)
colnames(vars_vrdb_cleaned)[1] <- "Cleaned_Variable"
vars_vrdb_cleaned$Pre_Cleaned_Values <- NA_character_
vars_vrdb_cleaned$Cleaned_Values <- NA_character_
for(i in 1:nrow(vars_vrdb_cleaned)){
    unique_values <- sort(unique(vrdb[,vars_vrdb_cleaned$Cleaned_Variable[i]]))
    vars_vrdb_cleaned$Pre_Cleaned_Values[i] <- paste(unique_values, collapse = ", ")
}




#clean the variables we can clean

#NameSuffix
# Convert to uppercase
vrdb$NameSuffix <- toupper(vrdb$NameSuffix)
# Standardize common suffixes
standardize_suffix <- function(suffix) {
    if (grepl("^JR\\.?$", suffix)) {
        return("JR")
    } else if (grepl("^SR\\.?$", suffix)) {
        return("SR")
    } else if (grepl("^I{1,3}\\.?$", suffix)) { # Matches I, II, III
        return(sub("\\.", "", suffix)) # Remove period
    } else if (grepl("^[IVX]+\\.?$", suffix)) { # Matches IV, V, ... Roman numerals
        return(sub("\\.", "", suffix)) # Remove period
    } else {
        return(suffix)
    }
}
vrdb$NameSuffix <- sapply(vrdb$NameSuffix, standardize_suffix)
suffixes <- c("III", "JR", "II", "SR", "IV", "V", "2ND", "3RD", "MD", "I", "VII", "VI",
              "DDS", "IX", "4TH", "VII", "DC", "DVM", "5TH", "XI", "XIII", "XII", "6TH", "X")
vrdb$NameSuffix[!vrdb$NameSuffix %in% suffixes] <- NA_character_

#Gender
genders <- c("M", "F", "O", "U")
vrdb$Gender[!vrdb$Gender %in% genders] <- NA_character_



#record clean values of cleaned vars
for(i in 1:nrow(vars_vrdb_cleaned)){
    unique_values <- sort(unique(vrdb[,vars_vrdb_cleaned$Cleaned_Variable[i]]))
    vars_vrdb_cleaned$Cleaned_Values[i] <- paste(unique_values, collapse = ", ")
}

#### CLEAN REJECTIONS AND CURES FILES ####

#record cleaned and uncleaned vars, record unclean values of vars to be cleaned
vars_rej <- colnames(rej)
vars_rej_cleaned <- c("ElectionDate", "County", "Gender", "Ballot.Status", "Challenge.Reason", "State", "Return.Method", "NoticeName")
vars_rej_uncleaned <- vars_rej[!vars_rej %in% vars_rej_cleaned]
vars_rej_cleaned <- as.data.frame(vars_rej_cleaned)
colnames(vars_rej_cleaned)[1] <- "Cleaned_Variable"
vars_rej_cleaned$Pre_Cleaned_Values <- NA_character_
vars_rej_cleaned$Cleaned_Values <- NA_character_
for(i in 1:nrow(vars_rej_cleaned)){
    unique_values <- sort(unique(rej[,vars_rej_cleaned$Cleaned_Variable[i]]))
    vars_rej_cleaned$Pre_Cleaned_Values[i] <- paste(unique_values, collapse = ", ")
}




rej$ElectionDate <- as.POSIXct(rej$ElectionDate, format="%Y-%m-%d %H:%M:%S")

colnames(rej)

#County
counties <- c("Kitsap", "Snohomish", "Lewis", "San Juan", "Whatcom", "Clark",
              "Walla Walla", "Spokane", "Island", "Cowlitz", "Whitman", "Douglas",
              "Benton", "Kittitas", "Franklin", "Mason", "Pierce", "Grant",
              "Klickitat", "Thurston", "Skagit", "Chelan", "Stevens", "Pacific",
              "Grays Harbor", "Ferry", "King", "Okanogan", "Lincoln", "Asotin",
              "Clallam", "Skamania", "Yakima", "Jefferson", "Adams", "Pend Oreille",
              "Columbia", "Wahkiakum", "Garfield")
rej$County[!rej$County %in% counties] <- NA_character_

#Gender
genders <- c("M", "F", "O", "U")
rej$Gender[!rej$Gender %in% genders] <- NA_character_

#Ballot.Status
standard_statuses <- c("Accepted", "Invalid", "Queued", "Received", "Rejected", "Sent", "Suspended", "Undeliverable")
rej$Ballot.Status[!rej$Ballot.Status %in% standard_statuses] <- NA_character_

#Challenge.Reason
challenge_reasons <- c("Too Late", "Signature Does Not Match", "Undeliverable", "No Signature on File", "Invalid",
                       "Unsigned", "ID Required", "Id Required", "Canvassing Board", "Empty Envelope", "HOLD", "Ballot Style Change",
                       "Witness Signature Missing", "Other than Voter", "Deceased", "Marked Moved", "Power of Attorney",
                       "Other Than Voter", "No Signature on File", "Voter Name Change", "Review", "No Party", "Both Parties",
                       "No Party, No Sig", "Modified", "Advanced Special Ballot", "Hold", "Power Of Attorney")
rej$Challenge.Reason[!rej$Challenge.Reason %in% challenge_reasons] <- NA_character_
#correcting spellings
rej$Challenge.Reason[rej$Challenge.Reason=="Power Of Attorney"] <- "Power of Attorney"
rej$Challenge.Reason[rej$Challenge.Reason=="HOLD"] <- "Hold"
rej$Challenge.Reason[rej$Challenge.Reason=="Id Required"] <- "ID Required"
rej$Challenge.Reason[rej$Challenge.Reason=="Other Than Voter"] <- "Other than Voter"

#State
# Set to NA if not exactly two letters
rej$State[!grepl("^[A-Za-z]{2}$", rej$State)] <- NA_character_

#Return.Method
return_methods <- c("Email", "Drop Box", "Mail", "In Person", "Non-Standard Mail", "Non-Standard Dropbox", "Fax")
rej$Return.Method[!rej$Return.Method %in% return_methods] <- NA_character_

#NoticeName
# For "Notice of Signature Does Not Match"
signature_not_match_pattern <- "Signature Does Not Match|Signature No Match"
rej$NoticeName[grepl(signature_not_match_pattern, rej$NoticeName, ignore.case = TRUE)] <- "Notice of Signature Does Not Match"
# For "Notice of Unsigned"
unsigned_pattern <- "Unsigned|No Signature"
rej$NoticeName[grepl(unsigned_pattern, rej$NoticeName, ignore.case = TRUE)] <- "Notice of Unsigned"


#record clean values of cleaned vars
for(i in 1:nrow(vars_rej_cleaned)){
    unique_values <- sort(unique(rej[,vars_rej_cleaned$Cleaned_Variable[i]]))
    vars_rej_cleaned$Cleaned_Values[i] <- paste(unique_values, collapse = ", ")
}



#### CREATE VARIABLES FOR REJECTIONS AND CURES FILES ####

#create curable definition
curable <- unique(rej$Challenge.Reason[!is.na(rej$NoticeSent)])

#create cure column
rej$cure <- NA_character_
rej$cure[rej$Ballot.Status=="Rejected"] <- "uncured"
rej$cure[rej$Ballot.Status=="Accepted" & is.na(rej$NoticeSent)] <- "cured_no_notice"
rej$cure[rej$Ballot.Status=="Accepted" & !is.na(rej$NoticeSent)] <- "cured_w_notice"

#join age and age_cat using ballot ID
dobs <- ballot_issuances[!is.na(ballot_issuances$DOB),] %>%
    select(Voter.ID, DOB) %>%
    distinct(Voter.ID, .keep_all=T)

rej <- left_join(rej, dobs)

rej$Age <- NA_real_
rej$Age <- as.numeric(as.Date(rej$ElectionDate, format="%Y-%m-%d") - rej$DOB) / 365
#Age_cat
rej$Age_cat <- NA_character_
rej$Age_cat[rej$Age<26] <- "25 and below"
rej$Age_cat[rej$Age>=26 & rej$Age<36] <- "26-35"
rej$Age_cat[rej$Age>=36 & rej$Age<46] <- "36-45"
rej$Age_cat[rej$Age>=46 & rej$Age<56] <- "46-55"
rej$Age_cat[rej$Age>=56 & rej$Age<66] <- "56-65"
rej$Age_cat[rej$Age>=66] <- "66+"

rej$Voter_Election <- paste0(rej$Voter.ID, rej$ElectionDate)
length(unique(rej$Voter_Election))/nrow(rej)

rej$Received.Date <- as.Date(rej$Received.Date, format = "%Y-%m-%d %H:%M:%S")

rej <- rej %>%
    group_by(Voter_Election) %>%
    filter(Received.Date == max(Received.Date))

rej <- rej %>%
    distinct(Voter_Election, .keep_all=T)

#create binary outcome vars

rej$Late <- 0
rej$Late[rej$Challenge.Reason=="Too Late"] <- 1
rej$Sig.Issue <- 0
rej$Sig.Issue[rej$Challenge.Reason %in% c("Unsigned", "Signature Does Not Match")] <- 1




#### REJECTIONS AND CURES - CALCULATE CURE LETTER TIMING RELATIVE TO ELECTION ####

#convert to date class

rej$Received.Date <- as.POSIXct(rej$Received.Date, format="%Y-%m-%d %H:%M:%S")
rej$NoticeSent <- as.POSIXct(rej$NoticeSent, format="%Y-%m-%d %H:%M:%OS")
rej$ElectionDate <- as.POSIXct(rej$ElectionDate, format="%Y-%m-%d %H:%M:%S")

#calculate time between notice and election
rej$notice_to_election <- rej$NoticeSent - rej$ElectionDate
#calculate time between received and notice
rej$received_to_notice <- rej$NoticeSent - rej$Received.Date
#received to election
rej$received_to_election <- rej$Received.Date - rej$ElectionDate

#convert to hours
# Convert minutes to hours
units(rej$received_to_notice) <- "hours"
#convert seconds to hours
units(rej$received_to_election) <- "hours"
#set negative received_to_notice values to NA (there's only 3)
rej$received_to_notice[rej$received_to_notice<0 & !is.na(rej$received_to_notice)] <- as.difftime(NA_real_, units="hours")

#check duplicate ids
# rej_subset <- rej %>%
#   group_by(Ballot.ID) %>%
#   filter(n() > 1) %>%
#   ungroup()

#duplicates exist because multiple notice letters were sent. pick the first letter for each ballot
rej <- rej %>%
    group_by(Ballot.ID) %>%
    slice_min(NoticeSent, n = 1) %>%
    ungroup()

#then remove duplicates further
rej <- rej %>%
    distinct(Ballot.ID, .keep_all=T)





curable <- unique(rej$Challenge.Reason[!is.na(rej$NoticeSent)])




#### JOIN NOTICE LETTER VARIABLES FROM REJECTIONS AND CURES TO MAIN DATA FRAME ####

colnames(rej)
rej_for_joining <- rej[,c("Ballot.ID", "NoticeName", "NoticeSent")]
colnames(rej_for_joining)[2] <- "Ballot_Sig_Issue_NoticeName"
colnames(rej_for_joining)[3] <- "Ballot_Sig_Issue_NoticeSent"

#join to ballot_issuances
ballot_issuances <- left_join(ballot_issuances, rej_for_joining, by=c("Ballot.ID"="Ballot.ID"))


#### CLEAN SIGNATURE UPDATE FILES ####


#record cleaned and uncleaned vars, record unclean values of vars to be cleaned
vars_sig <- colnames(sig)
vars_sig_cleaned <- c("Type")
vars_sig_uncleaned <- vars_sig[!vars_sig %in% vars_sig_cleaned]
vars_sig_cleaned <- as.data.frame(vars_sig_cleaned)
colnames(vars_sig_cleaned)[1] <- "Cleaned_Variable"
vars_sig_cleaned$Pre_Cleaned_Values <- NA_character_
vars_sig_cleaned$Cleaned_Values <- NA_character_
for(i in 1:nrow(vars_sig_cleaned)){
    unique_values <- sort(unique(sig[,vars_sig_cleaned$Cleaned_Variable[i]]))
    vars_sig_cleaned$Pre_Cleaned_Values[i] <- paste(unique_values, collapse = ", ")
}



#convert DateSent to date class
sig$DateSent <- as.Date(sig$DateSent, format = "%m/%d/%Y")

sig$VoterID <- as.character(sig$VoterID)

colnames(sig)

sig$Type <- toupper(sig$Type)



#record clean values of cleaned vars
for(i in 1:nrow(vars_sig_cleaned)){
    unique_values <- sort(unique(sig[,vars_sig_cleaned$Cleaned_Variable[i]]))
    vars_sig_cleaned$Cleaned_Values[i] <- paste(unique_values, collapse = ", ")
}

#### JOIN RACE IMPUTATION RESULTS ####

#create year
ballot_issuances$Year <- str_extract(ballot_issuances$Election, "\\d{4}$")
rej$Year <- substr(rej$ElectionDate, 1, 4)
#year already created for vrdb

#create address
ballot_issuances$Full_Address <- paste(ballot_issuances$Address, ballot_issuances$City,
                                       ballot_issuances$State, ballot_issuances$Zip,
                                       sep=", ")
rej$Full_Address <- paste(rej$Address, rej$City,
                                       rej$State, rej$ZIP,
                                       sep=", ")

#add WA prefix to voter ID for ballot_issuances and rej
ballot_issuances$Voter.ID <- paste0("WA", ballot_issuances$Voter.ID)
rej$Voter.ID <- paste0("WA", rej$Voter.ID)

#join to rej, ballot_issuances, and vrdb

race_imputation$Year <- as.character(race_imputation$Year)
race_imputation$geometry <- as.character(race_imputation$geometry)
race_imputation <- race_imputation[,c("surname", "Voter.ID", "Year", "Address", "GEOID", "Block.Group", "Tract", "geometry", "county", "tract", "block_group", "pred.whi", "pred.bla", "pred.his", "pred.asi", "pred.oth")]

rej <- left_join(rej, race_imputation, by=c("Last.Name"="surname", "Voter.ID"="Voter.ID", "Year"="Year",
                                            "Full_Address"="Address"))
ballot_issuances <- left_join(ballot_issuances, race_imputation, by=c("Last.Name"="surname", "Voter.ID"="Voter.ID", "Year"="Year",
                                            "Full_Address"="Address"))

vrdb$Year <- as.character(vrdb$Year)
# Apply proper formatting to rows that do not start with 'WA'
vrdb$StateVoterID <- ifelse(
    !grepl("^WA", vrdb$StateVoterID), # Condition to check rows that do not start with 'WA'
    sprintf("WA%09d", as.numeric(sub("^WA", "", vrdb$StateVoterID))), # Apply formatting
    vrdb$StateVoterID # Keep the original value if it starts with 'WA'
)

vrdb <- vrdb %>%
    distinct(LName, StateVoterID, Year, Address, .keep_all=T)
race_imputation <- race_imputation %>%
    distinct(surname, Voter.ID, Year, Address, .keep_all=T)
vrdb <- left_join(vrdb, race_imputation, by=c("LName"="surname", "StateVoterID"="Voter.ID", "Year"="Year",
                                                                      "Address"="Address"))
vrdb$geometry[vrdb$geometry=="c(NA, NA)"] <- NA_character_


#### SAVE ALL FILES AS RDS ####

saveRDS(ballot_issuances, "R:/Project/UWED/Master Team Folder/Data/Outputs/Ballot_Issuances_clean_01312025.rds")
saveRDS(vrdb, "R:/Project/UWED/Master Team Folder/Data/Outputs/VRDB_clean_01312025.rds")
saveRDS(rej, "R:/Project/UWED/Master Team Folder/Data/Outputs/Rejections_and_Cures_clean_01312025.rds")
saveRDS(sig, "R:/Project/UWED/Master Team Folder/Data/Outputs/Signature_Update_Requests_clean_01312025.rds")

#save diagnostics of cleaned variables
write.csv(vars_issuances_cleaned, "R:/Project/UWED/Master Team Folder/Data/Outputs/Cleaning Diagnostics/Ballot_Issuances_Cleaned_Vars_01312025.csv", row.names = F)
write.csv(vars_vrdb_cleaned, "R:/Project/UWED/Master Team Folder/Data/Outputs/Cleaning Diagnostics/VRDB_Cleaned_Vars_01312025.csv", row.names = F)
write.csv(vars_rej_cleaned, "R:/Project/UWED/Master Team Folder/Data/Outputs/Cleaning Diagnostics/Rejections_and_Cures_Cleaned_Vars_01312025.csv", row.names = F)
write.csv(vars_sig_cleaned, "R:/Project/UWED/Master Team Folder/Data/Outputs/Cleaning Diagnostics/Signature_Update_Requests_Cleaned_Vars_01312025.csv", row.names = F)



#### DOCUMENTATION FOR UNCLEANED VARIABLES ####

library(data.table)

# Function to process each dataset with data.table
process_data_dt <- function(data, vars_uncleaned, output_file_path) {
    # Convert data to data.table
    setDT(data)

    # Create a list to store results
    results <- vector("list", length(vars_uncleaned))

    # Loop through columns and process
    for (column_name in vars_uncleaned) {
        unique_values <- unique(data[[column_name]])

        if (length(unique_values) > 100) {
            sampled_values <- sample(unique_values, size = 100, replace = FALSE)
        } else {
            sampled_values <- unique_values
        }

        sampled_values_str <- paste(sampled_values, collapse = ", ")
        results[[column_name]] <- paste(column_name, sampled_values_str, sep = "\n")
    }

    # Write results to file
    write(unlist(results), file = output_file_path)
}

# Apply the function to each dataset
process_data_dt(as.data.table(ballot_issuances), vars_issuances_uncleaned, "R:/Project/UWED/Master Team Folder/Data/Outputs/Cleaning Diagnostics/Ballot_Issuances_Sample_Values_of_Uncleaned_Vars_01312025.txt")
process_data_dt(as.data.table(vrdb), vars_vrdb_uncleaned, "R:/Project/UWED/Master Team Folder/Data/Outputs/Cleaning Diagnostics/VRDB_Sample_Values_of_Uncleaned_Vars_01312025.txt")
process_data_dt(as.data.table(rej), vars_rej_uncleaned, "R:/Project/UWED/Master Team Folder/Data/Outputs/Cleaning Diagnostics/Rejections_and_Cures_Sample_Values_of_Uncleaned_Vars_01312025.txt")
process_data_dt(as.data.table(sig), vars_sig_uncleaned, "R:/Project/UWED/Master Team Folder/Data/Outputs/Cleaning Diagnostics/Signature_Update_Requests_Sample_Values_of_Uncleaned_Vars_01312025.txt")
