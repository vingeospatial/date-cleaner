# Load necessary libraries
library(data.table)    # For fast data manipulation
library(lubridate)     # For flexible date-time parsing
library(stringr)       # For string handling and regex operations
library(fasttime)      # For very fast POSIXct date parsing
library(progress)      # For progress bars
library(crayon)        # For colored terminal output (used for better visibility)

# Define the main function to clean one or more date columns in a data frame
clean_date_columns <- function(data, date_columns, tz = "Africa/Nairobi", batch_size = 1e6, verbose = TRUE) {
  
  # Convert date_columns argument to a character vector to ensure consistent handling
  date_columns <- as.character(date_columns)
  
  # Check if all specified date columns exist in the data
  missing_columns <- date_columns[!date_columns %in% names(data)]
  
  # Stop execution and show an error if some columns are missing
  if (length(missing_columns) > 0) {
    stop("Error: Specified date column(s) not found in the data frame: ", 
         paste(missing_columns, collapse = ", "))
  }
  
  # Convert the data frame to a data.table for faster processing
  dt <- as.data.table(data)
  
  # Define helper function to identify the format pattern of each date string
  identify_pattern <- function(date) {
    if (is.na(date) || date == "") return("NA or empty")               # Handle empty or NA entries
    if (!grepl("[0-9]", date)) return("Non-numeric text")              # Detect if text has no digits
    
    # Check and return specific date patterns using regex
    if (grepl("^\\d{4}-\\d{2}-\\d{2}$", date)) return("YYYY-MM-DD")
    if (grepl("^\\d{1,2}/\\d{1,2}/\\d{2}$", date)) return("MM/DD/YY or DD/MM/YY")
    if (grepl("^\\d{1,2}-\\d{1,2}-\\d{4}$", date)) return("MM-DD-YYYY or DD-MM-YYYY")
    if (grepl("^\\d{2}-[A-Za-z]{3}-\\d{4}$", date)) return("DD-MMM-YYYY")
    if (grepl("^\\d{8}$", date)) return("YYYYMMDD")
    if (grepl("^\\d{4}/\\d{2}/\\d{2}$", date)) return("YYYY/MM/DD")
    if (grepl("^\\d{2}/\\d{2}/\\d{4}$", date)) return("MM/DD/YYYY or DD/MM/YYYY")
    if (grepl("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$", date)) return("YYYY-MM-DD HH:MM:SS")
    if (grepl("^\\d{1,2}/\\d{1,2}/\\d{4} \\d{1,2}:\\d{2}$", date)) return("MM/DD/YYYY HH:MM")
    if (grepl("^\\d{1,2}/\\d{1,2}/\\d{4} \\d{1,2}:\\d{2}:\\d{2}$", date)) return("MM/DD/YYYY HH:MM:SS")
    if (grepl("^\\d{1,2}-\\d{1,2}-\\d{4} \\d{1,2}:\\d{2}[AP]M$", date)) return("MM-DD-YYYY H:MMAM/PM")
    if (grepl("^\\d{8} \\d{6}$", date)) return("YYYYMMDD HHMMSS")
    if (grepl("^\\d{2}-[A-Za-z]{3}-\\d{4} \\d{2}:\\d{2}$", date)) return("DD-MMM-YYYY HH:MM")
    
    # Handle all remaining cases
    if (grepl("\\d", date)) return("Invalid numeric date")
    return("Other")
  }
  
  # Define helper function to preprocess each date string before parsing
  preprocess_date <- function(date) {
    if (is.na(date) || date == "") return(NA_character_)              # Replace blanks with NA
    if (grepl(re_hhmm, date)) return(paste0(date, ":00"))             # Append seconds (:00) to HH:MM patterns
    if (grepl(re_ampm, date)) return(gsub("-", "/", date))            # Replace dashes with slashes in AM/PM formats
    return(date)                                                      # Otherwise return unchanged
  }
  
  # Define supported date-time formats for lubridate::parse_date_time()
  datetime_formats <- c(
    "Y-m-d HMS", "m/d/y HM", "m/d/y HMS", "m/d/Y HM", "m/d/Y HMS",
    "m-d-Y HM p", "d/m/y HM", "d/m/y HMS", "d/m/Y HM", "d/m/Y HMS",
    "Y/m/d HM", "Y/m/d HMS", "dBy HM", "Ymd HMS", "Y-m-d", "m/d/y",
    "m/d/Y", "d/m/Y", "dBy", "Ymd"
  )
  
  # Precompile regex patterns for efficiency
  re_hhmm <- "^\\d{1,2}/\\d{1,2}/\\d{4} \\d{1,2}:\\d{2}$|^\\d{2}-[A-Za-z]{3}-\\d{4} \\d{2}:\\d{2}$" # Detects HH:MM
  re_ampm <- "^\\d{1,2}-\\d{1,2}-\\d{4} \\d{1,2}:\\d{2}[AP]M$"                                       # Detects AM/PM
  re_invalid_month <- "^\\d{4}/13/\\d{2}"                                                            # Detects invalid month (13)
  re_valid_time <- "\\d{2}:[0-5][0-9]:[0-5][0-9]"                                                    # Detects valid HH:MM:SS
  re_hhmm_only <- "\\d{2}:\\d{2}"                                                                    # Detects HH:MM only
  re_non_numeric <- "^[a-zA-Z]+$"                                                                    # Detects pure text
  re_simple_format <- "^\\d{4}-\\d{2}-\\d{2}( \\d{2}:\\d{2}:\\d{2})?$"                               # Detects simple ISO formats
  
  # Define internal worker function to process a single date column
  process_column <- function(date_column, dt, batch_size, verbose, tz) {
    
    # Print a green message "processing......" for visibility
    if (interactive()) {
      cat(crayon::green("processing......\n"))
    }
    
    # Extract the current column as a character vector
    date_vec <- as.character(dt[[date_column]])
    n_rows <- length(date_vec)  # Total number of rows
    
    # Identify and summarize initial date format patterns
    patterns <- vapply(date_vec, identify_pattern, character(1), USE.NAMES = FALSE)
    initial_patterns <- data.table(
      pattern = patterns,
      original = date_vec
    )[, .(count = .N, examples = paste(head(original, min(3, .N)), collapse = ", ")), by = pattern]
    
    # Print pattern summary if verbose is TRUE
    if (verbose) {
      cat("\nProcessing column:", date_column, "\n")
      cat("Initial patterns in", date_column, ":\n")
      print(initial_patterns)
      cat("\n")
    }
    
    # Initialize empty vectors for parsed dates and out-of-range flags
    parsed_dates <- rep(NA_POSIXct_, n_rows)
    out_of_range <- rep(NA, n_rows)
    
    # Split data into batches for memory efficiency
    batches <- seq(1, n_rows, by = batch_size)
    if (batches[length(batches)] < n_rows) batches <- c(batches, n_rows + 1)
    num_batches <- length(batches) - 1
    
    # Create progress bar (only in interactive mode)
    pb <- NULL
    if (interactive()) {
      pb <- progress_bar$new(
        format = paste("Processing", date_column, "[:bar] :percent eta: :eta"),
        total = num_batches,
        clear = FALSE,
        width = 60
      )
      pb$tick(0)
    }
    
    # Initialize lists to collect failed and final pattern summaries
    failed_list <- list()
    final_patterns_list <- list()
    
    # Loop through each batch
    for (i in seq_len(num_batches)) {
      if (!is.null(pb)) pb$tick()  # Update progress bar
      
      # Define batch boundaries
      start_idx <- batches[i]
      end_idx <- min(batches[i + 1] - 1, n_rows)
      batch <- date_vec[start_idx:end_idx]  # Extract current batch
      
      # Preprocess each date string
      processed <- vapply(batch, preprocess_date, character(1), USE.NAMES = FALSE)
      processed[grepl(re_non_numeric, processed)] <- NA_character_  # Replace non-numeric text with NA
      
      # Parse simple ISO-like formats quickly using fasttime::fastPOSIXct
      is_simple <- grepl(re_simple_format, processed, perl = TRUE)
      parsed_dates[start_idx:end_idx][is_simple] <- suppressWarnings(
        fastPOSIXct(processed[is_simple], tz = tz)
      )
      
      # Use lubridate for complex date formats
      to_parse <- !is_simple & !is.na(processed)
      if (any(to_parse)) {
        parsed_dates[start_idx:end_idx][to_parse] <- suppressWarnings(
          parse_date_time(processed[to_parse], orders = datetime_formats, tz = tz)
        )
      }
      
      # Correct invalid month “13” to “01”
      invalid_month <- grepl(re_invalid_month, processed, perl = TRUE)
      if (any(invalid_month)) {
        parsed_dates[start_idx:end_idx][invalid_month] <- suppressWarnings(
          as.POSIXct(gsub("/13/", "/01/", processed[invalid_month]), format = "%Y/%m/%d %H:%M:%S", tz = tz)
        )
      }
      
      # Set invalid time-only strings to NA
      invalid_time <- grepl(re_hhmm_only, processed, perl = TRUE) & !grepl(re_valid_time, processed, perl = TRUE)
      parsed_dates[start_idx:end_idx][invalid_time] <- NA_POSIXct_
      
      # Collect failed parses for inspection
      batch_failed <- data.table(
        original = processed,
        cleaned = parsed_dates[start_idx:end_idx]
      )[is.na(cleaned)]
      if (nrow(batch_failed) > 0) {
        failed_list[[i]] <- batch_failed
      }
      
      # Summarize final patterns for this batch
      batch_patterns <- data.table(
        original = processed,
        pattern = fcase(
          is.na(processed), "NA or empty",
          grepl("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$", processed, perl = TRUE), "YYYY-MM-DD HH:MM:SS",
          grepl("^\\d{1,2}/\\d{1,2}/\\d{4} \\d{1,2}:\\d{2}$", processed, perl = TRUE), "MM/DD/YYYY HH:MM",
          grepl("^\\d{1,2}/\\d{1,2}/\\d{4} \\d{1,2}:\\d{2}:\\d{2}$", processed, perl = TRUE), "MM/DD/YYYY HH:MM:SS",
          grepl("^\\d{1,2}-\\d{1,2}-\\d{4} \\d{1,2}:\\d{2}[AP]M$", processed, perl = TRUE), "MM-DD-YYYY H:MMAM/PM",
          grepl("^\\d{2}-[A-Za-z]{3}-\\d{4} \\d{2}:\\d{2}$", processed, perl = TRUE), "DD-MMM-YYYY HH:MM",
          grepl("^\\d{8} \\d{6}$", processed, perl = TRUE), "YYYYMMDD HHMMSS",
          grepl("\\d", processed, perl = TRUE), "Invalid numeric date",
          default = "Non-numeric text"
        )
      )[, .(count = .N), by = pattern]
      final_patterns_list[[i]] <- batch_patterns
    }
    
    # Combine all failed parses from batches into one table
    failed_parses <- if (length(failed_list) > 0) {
      rbindlist(failed_list)[, .(count = .N), by = original][order(-count)]
    } else {
      data.table(original = character(0), count = integer(0))
    }
    
    # Combine all final pattern summaries from batches
    final_patterns <- rbindlist(final_patterns_list)[
      , .(count = sum(count), examples = paste(head(dt[[date_column]][.I], min(3, .N)), collapse = ", ")), 
      by = pattern
    ]
    
    # Convert parsed dates to POSIXct safely
    parsed_dates <- suppressWarnings(as.POSIXct(parsed_dates, tz = tz))
    
    # Flag out-of-range dates (before 1900 or after 2025)
    out_of_range <- parsed_dates < as.POSIXct("1900-01-01 00:00:00", tz = tz) |
      parsed_dates > as.POSIXct("2025-12-31 23:59:59", tz = tz)
    
    # Add cleaned columns to the original data.table
    dt[, (paste0("cleaned_", date_column)) := parsed_dates]
    dt[, (paste0("out_of_range_", date_column)) := out_of_range]
    dt[, (date_column) := date_vec]
    
    # Print summary statistics and failed examples
    if (verbose) {
      cat("Summary of cleaned datetimes in", paste0("cleaned_", date_column), ":\n")
      print(summary(parsed_dates))
      cat("Number of NAs:", sum(is.na(parsed_dates)), "\n\n")
      cat("Remaining unparsed entries in", date_column, ":\n")
      print(head(failed_parses, 10))
      cat("\nFinal patterns in", date_column, ":\n")
      print(final_patterns)
      cat("\n")
    }
    
    # Return all relevant objects
    return(list(dt = dt, initial_patterns = initial_patterns, failed_parses = failed_parses, final_patterns = final_patterns))
  }
  
  # Create progress bar for column-level processing
  num_columns <- length(date_columns)
  pb_columns <- NULL
  if (interactive()) {
    pb_columns <- progress_bar$new(
      format = "Processing columns [:bar] :percent eta: :eta",
      total = num_columns,
      clear = FALSE,
      width = 60
    )
    pb_columns$tick(0)
  }
  
  # Process each column sequentially
  for (col in date_columns) {
    if (!is.null(pb_columns)) pb_columns$tick()
    result <- suppressWarnings(process_column(col, dt, batch_size, verbose, tz))
    dt <- result$dt
  }
  
  # Stop the column progress bar when done
  if (!is.null(pb_columns)) pb_columns$terminate()
  
  # Return the cleaned data.table as a regular data.frame
  return(as.data.frame(dt))
}

# # Example usage of the function (commented out for distribution)
# covid_raw <- read.csv("data/coronavirus_dataset.csv", stringsAsFactors = FALSE)  # Load CSV
# cleaned_data_multiple <- clean_date_columns(covid_raw, c("Last.Update", "ObservationDate"), tz = "Africa/Nairobi")  # Clean two columns
# print(head(cleaned_data_multiple, 10))  # Print first 10 rows of cleaned dataset