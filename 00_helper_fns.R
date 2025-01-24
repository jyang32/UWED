#' Helper functions for UWED pipeline
#' 

# GUESS DELIMITER for .txt files####

guessDelimiter <- function(file_path, num_lines = 50) {
  # Read the first few lines of the file
  lines <- readLines(file_path, n = num_lines)
  
  # Define a list of possible delimiters
  possible_delimiters <- c(",", "\t", ";", "|", " ")
  
  # Initialize a vector to count delimiter occurrences
  delimiter_counts <- numeric(length(possible_delimiters))
  
  for (i in seq_along(possible_delimiters)) {
    # Count how many times each delimiter appears in the first few lines
    delimiter_counts[i] <- sum(sapply(lines, function(line) sum(str_count(line, fixed(possible_delimiters[i])))))
  }
  
  # Find the delimiter with the highest count
  guessed_delimiter <- possible_delimiters[which.max(delimiter_counts)]
  
  return(guessed_delimiter)
}

# Remove observations based on pattern ####
#remove those with missing data
# Define the patterns to remove
# Create a function to check for the patterns
remove_observations <- function(address) {
  patterns <- c("NA ,", "NA,", ", NA")
  for (pattern in patterns) {
    if (grepl(pattern, address)) {
      return(FALSE)
    }
  }
  return(TRUE)
}

# ASCII Characters ####
# Function to check if a string contains non-ASCII characters
contains_non_ascii <- function(s) {
  any(charToRaw(s) > 127)
}