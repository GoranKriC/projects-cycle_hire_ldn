###########################################################################################
# London cycle hire - data set conversion to fst format for quicker loading in Shiny apps
###########################################################################################

get.fst.names <- function(app.name){
    dbc <- dbConnect(MySQL(), group = 'dataOps', dbname = 'common')
    strSQL <- paste0("SELECT path FROM paths WHERE name = 'dataframes' AND system = '", ifelse(grepl('linux', R.version$os), 'linux', 'win'), "'")
    data.path <- dbGetQuery(dbc, strSQL)
    data.path <- paste0(data.path, 'fst', '/', app.name, '/')
    dt_names <- dbGetQuery(dbc, paste0("SELECT dataset FROM fst_shiny WHERE appname = '", app.name, "'"))
    dbDisconnect(dbc)
    return( list(data.path, dt_names) )
}

dt2fst <- function(app.name){
    lapply(c('fst', 'RMySQL'), require, character.only = TRUE)
    y <- get.fst.names(app.name)
    dbc <- dbConnect(MySQL(), group = 'dataOps', dbname = app.name)
    for(dtn in unlist(y[[2]])){
        print(paste0('Reading ', dtn, '...'))
        dt <- dbReadTable(dbc, dtn)
        print(paste0('Writing ', dtn, '...'))
        write.fst(dt, paste0(y[[1]], dtn, '.fst'), 100 )
    }
    dbDisconnect(dbc)
}


### How  to read back fst files in the app as data.tables
# lapply(c('fst', 'RMySQL'), require, character.only = TRUE)
# app.name <- 'cycle_hire_ldn'
# y <- get.fst.names(app.name)
# dbc <- dbConnect(MySQL(), group = 'dataOps', dbname = app.name)
# for(dtn in unlist(y[[2]])){
#     print(paste0('Reading ', dtn, '...'))
#     assign(dtn, read.fst(paste0(y[[1]], dtn, '.fst'), as.data.table = TRUE) )
# }
# dbDisconnect(dbc)
