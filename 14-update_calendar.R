#####################################
# LONDON cycle hire - Fill Calendar #
#####################################

# load packages -------------------------------------------------------------------------------------------------------
lapply(c('data.table', 'lubridate'), require, char = TRUE)

# helpers -------------------------------------------------------------------------------------------------------------
days_of_week <- data.table(
    day_num = 1:7,
    day_txt = c('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'),
    day_pref = c('Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun')
)
months_of_year <- data.table(
    month_num  = 1:12,
    month_txt  = c(
        'January', 'February', 'March', 'April', 'May', 'June', 
        'July', 'August', 'September', 'October', 'November', 'December'
    ),
    month_pref = c('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec')
)
get_day_suff <- Vectorize(function(x){
    switch(as.character(x),
        '1' =, '21' =, '31' = 'st',
        '2' =, '22' = 'nd',
        '3' =, '23' = 'rd',
        'th'
    )
})
to_date_cuts <- data.table(
    cuts = c(0, 7,  15,  30,  60,  90, 120, 180, 360, 365, 372, 380, 395, 425, 455, 485, 545, 725, 730), 
    labels = c(1:9, 11:19, 99)
)

holydays <- data.table(
  
)

# set boundary dates --------------------------------------------------------------------------------------------------
dbc = dbConnect(MySQL(), group = 'dataOps', dbname = 'cycle_hire_ldn')
date_start <- as.Date(as.character(dbGetQuery(dbc, "SELECT MIN(start_day) FROM hires")), '%Y%m%d')
date_end <- as.Date(as.character(dbGetQuery(dbc, "SELECT MAX(start_day) FROM hires")), '%Y%m%d')
dbDisconnect(dbc)

# create primary key --------------------------------------------------------------------------------------------------
calendar <- data.table(datefield = seq(as.Date(date_start, origin = '1970-01-01'), date_end, by = 'days'))
calendar <- calendar[order(-datefield)]

# DAYS ----------------------------------------------------------------------------------------------------------------
calendar[, `:=`( 
    day_long = format(datefield, '%A'),
    day_short = format(datefield, '%a'),
    day_of_week = format(datefield, '%u'),
    day_of_month = format(datefield, '%e'),
    day_of_quarter = qday(datefield),
    day_of_quarters = as.numeric(gsub('\\.', '', quarter(datefield, with_year = TRUE))) * 100 + qday(datefield),
    day_of_year = as.numeric(format(datefield, '%j')),
    day_of_years = format(datefield, '%Y%j'),
    day_last_month = gsub('-', '', datefield - months(1)),
    day_last_quarter = NA,
    day_last_year = gsub('-', '', datefield - years(1)),
    first_day_curr_month = gsub('-', '', rollback(datefield, roll_to_first = TRUE)),
    last_day_curr_month = gsub('-', '', ceiling_date(datefield, unit = 'month') - 1),
    last_day_prev_month = gsub('-', '', rollback(datefield)),
    d0 = gsub('-', '', datefield),
    d1 = format(datefield, '%d %b'),
    d2 = format(datefield, '%d/%m/%y'),
    d3 = format(datefield, '%d-%m-%y'),
    d4 = format(datefield, '%d-%b-%y'),
    d5 = format(datefield, '%d %b %y'),
    d6 = format(datefield, '%a, %d %b'),
    d7 = format(datefield, '%a, %d %b %y'),
    d8 = format(datefield, '%d%m%Y'),
    d9 = format(datefield, '%d%m%y')
)]
calendar[, `:=`( 
    day_suff = get_day_suff(day_of_week),
    is_weekday = as.numeric(day_of_week <= 5),
    days_past := 1:.N
)]
calendar[, is_holiday := NA]
calendar[, to_date := as.numeric( cut( days_past, c(to_date_cuts$cuts, Inf), to_date_cuts$labels ) ) ]

# WEEK ----------------------------------------------------------------------------------------------------------------
calendar[, `:=`( 
    week_of_year = format(datefield, '%V'), 
    week_of_years = format(datefield, '%G%V'), 
#    week_of_month = format(datefield, '%e'), 
    w0 = datefield - days(as.numeric(day_of_week) -  1)
)]
calendar[, `:=`( 
    w1 = format(w0, '%d %b'),
    w2 = format(w0, '%d/%m/%y'),
    w3 = format(w0, '%d-%m-%y'),
    w4 = format(w0, '%d-%b-%y'),
    w5 = format(w0, '%d %b %y')
)]
calendar[, w0 := gsub('-', '', w0)]
calendar <- calendar[unique(calendar[, .(w0)])[, weeks_past := 1:.N], on = 'w0']

# MONTH ---------------------------------------------------------------------------------------------------------------
calendar[, `:=`( 
    month_long = format(datefield, '%B'), 
    month_short = format(datefield, '%b'),
    month_of_year = format(datefield, '%m'), 
    m0 = format(datefield, '%Y%m'),
    m2 = format(datefield, '%m/%y'),
    m3 = format(datefield, '%m-%y'),
    m4 = format(datefield, '%b-%y'),
    m5 = format(datefield, '%b %y'),
    m8 = format(datefield, '%m%Y'),
    m9 = format(datefield, '%m%y')
)]
calendar <- calendar[unique(calendar[, .(m0)])[, months_past := 1:.N], on = 'm0']

# QUARTER -------------------------------------------------------------------------------------------------------------
calendar[, `:=`( 

)]

# YEAR ----------------------------------------------------------------------------------------------------------------
calendar[, `:=`( 
    y0 = year(datefield)
)]

# SAVE TO DATABASE ----------------------------------------------------------------------------------------------------
dbc = dbConnect(MySQL(), group = 'dataOps', dbname = 'cycle_hire_ldn')
dbWriteTable(dbc, 'calendar', calendar, row.names = FALSE, append = TRUE)
dbDisconnect(dbc)

# clean and exit  -----------------------------------------------------------------------------------------------------
rm(list = ls())
gc()

