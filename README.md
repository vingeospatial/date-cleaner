# Date Cleaner: Clean Mixed Date Formats in R

## Overview
The `clean_date_columns` function is an R utility for cleaning and standardizing date columns with mixed formats and timestamps in a data frame. It handles various date formats, converts them to POSIXct, flags out-of-range dates, and provides detailed summaries of the cleaning process. Optimized for performance with `data.table` and `fasttime`, it supports batch processing for large datasets.

## Features
- Processes single or multiple date columns.
- Recognizes formats like `YYYY-MM-DD`, `MM/DD/YYYY`, `DD-MMM-YYYY`, and timestamps (e.g., `YYYY-MM-DD HH:MM:SS`).
- Handles `NA`, empty strings, and non-numeric text.
- Corrects invalid months (e.g., `13` to `01`).
- Flags dates outside 1900–2025.
- Provides verbose summaries: initial patterns, cleaned datetimes, unparsed entries, and final patterns.
- Uses progress bars for interactive feedback.

## Dependencies
- R (>= 3.5.0)
- Packages: `data.table`, `lubridate`, `stringr`, `fasttime`, `progress`, `crayon`

## Installation
### Option 1: Source Directly
1. Clone or download the repository:
   ```bash
   git clone https://github.com/your-username/date-cleaner.git
   ```
2. Source the script in R:
   ```R
   source("path/to/date-cleaner/clean_date_columns.R")
   ```

### Option 2: Install as a Package (Optional)
If packaged (future enhancement), install using `devtools`:
```R
devtools::install_github("your-username/date-cleaner")
library(datecleaner)
```

## Usage
### Basic Example
```R
# Load your data
data <- data.frame(
  date_col = c("2020-03-23 23:23:20", "3/8/20 5:31", "01-22-2020 5:00PM", NA, "", "Invalid Date")
)

# Clean a single date column
cleaned_data <- clean_date_columns(data, "date_col", tz = "Africa/Nairobi")

# Clean multiple date columns
cleaned_data <- clean_date_columns(data, c("date_col1", "date_col2"), tz = "Africa/Nairobi", batch_size = 1e6, verbose = TRUE)
```

### Parameters
- `data`: Input data frame containing the date columns.
- `date_columns`: Character vector of column names to clean (single or multiple).
- `tz`: Timezone for output POSIXct dates (default: "Africa/Nairobi").
- `batch_size`: Number of rows to process per batch (default: 1e6).
- `verbose`: Logical; if `TRUE`, prints summaries (default: `TRUE`).

### Output
- A data frame with original columns plus:
  - `cleaned_<column>`: Standardized POSIXct dates for each input column.
  - `out_of_range_<column>`: Logical flag for dates outside 1900–2025.
- Summaries (if `verbose = TRUE`):
  - Initial patterns: Distribution of date formats.
  - Cleaned datetimes: Summary statistics of parsed dates.
  - Unparsed entries: List of entries that failed parsing.
  - Final patterns: Distribution after preprocessing.

## Example Output
For a column with mixed formats:
```
Processing column: date_col
Initial patterns in date_col:
   pattern                    count examples
1: YYYY-MM-DD HH:MM:SS        1     2020-03-23 23:23:20
2: MM/DD/YY HH:MM             1     3/8/20 5:31
3: MM-DD-YYYY H:MMAM/PM       1     01-22-2020 5:00PM
4: NA or empty                2     NA, 
5: Non-numeric text           1     Invalid Date

Summary of cleaned datetimes in cleaned_date_col:
   Min.               1st Qu.            Median             Mean               3rd Qu.            Max.               NA's
2020-01-22 17:00:00 2020-03-08 05:31:00 2020-03-08 05:31:00 2020-03-11 14:38:13 2020-03-23 23:23:20 2020-03-23 23:23:20 3
Number of NAs: 3
```

## Notes
- **Ambiguous Formats**: Formats like `MM/DD/YY` may be interpreted as `MM/DD/YY` or `DD/MM/YY`. Adjust `datetime_formats` in the script if your data follows a specific convention.
- **Performance**: Batch processing (`batch_size`) optimizes memory for large datasets.
- **NA Handling**: `NA`, empty strings, and non-numeric text are converted to `NA` in the cleaned columns.

## Contributing
Contributions are welcome! Please submit issues or pull requests to the GitHub repository.

## License
MIT License