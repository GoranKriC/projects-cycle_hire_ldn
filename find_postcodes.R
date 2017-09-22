###############################################################################
# LONDON cycle hire - Shiny app to check station postcode with its neighbours
###############################################################################
lapply(c('data.table', 'DT', 'leaflet', 'RMySQL', 'shiny'), require, character.only = TRUE)

# Retrieve db name
dbc = dbConnect(MySQL(), group = 'dataOps', dbname = 'common')
db_name <- dbGetQuery(dbc, "SELECT db_name FROM common.cycle_hires WHERE scheme_id = 1")[[1]]
dbDisconnect(dbc)

pal <- colorFactor(c('navy', 'red'), domain = c('station', 'neighbour') )

strSQL <- "
    SELECT * 
    FROM stations 
    WHERE is_active AND area != 'void'
    ORDER BY postcode
"
dbc = dbConnect(MySQL(), group = 'dataOps', dbname = db_name)
stations <- data.table(dbGetQuery(dbc, strSQL) )
dbDisconnect(dbc)

ui <- fluidPage(
        dataTableOutput('st_tbl'), hr(),
        textInput('txtPostcode', 'POSTCODE:'),
        fluidRow(
            column(2, actionButton('btnUpdate', 'update station') ),
            column(2, actionButton('btnUpdateOA', 'update OAs') )
        ), hr(),
        leafletOutput('st_map', height = '600px')
)

server <- function(input, output, session) {
    
    y <- stations[, .(
            station_id, 
            address, 
            place, 
            area, 
            started = first_hire, 
            docks
        )][order(area, place)]

    output$st_tbl <- renderDataTable({
        datatable(y,
            rownames = FALSE,
            selection = 'single',
            class = 'cell-border stripe hover nowrap',
            extensions = 'Scroller',
            escape = FALSE,
            options = list(
                scrollX = TRUE,
                scrollY = 300,
                scroller = TRUE,
                searchHighlight = TRUE,
                initComplete = JS(
                    "function(settings, json) {",
                    "$(this.api().table().header()).css({'background-color': '#238443', 'color': '#fff'});",
                    "}"
                ),
                dom = 'frtip'
            )
        )
    })
    
    selID <- reactive({
        if(length(input$st_tbl_rows_selected ) == 0) return(NULL)
        y[input$st_tbl_rows_selected, station_id]
    })
    
    output$st_map <- renderLeaflet({
        if(length(input$st_tbl_rows_selected ) == 0) return(NULL)
        st_lon = stations[station_id == selID(), x_lon]
        st_lat = stations[station_id == selID(), y_lat]
        locations <- data.frame(postcode = stations[station_id == selID(), postcode], type = 'station', lon = st_lon, lat = st_lat, distance = 0 )
        strSQL <- paste("
            SELECT postcode, 'neighbour' AS type, x_lon AS lon, y_lat AS lat,
                (3959 * ACOS ( 
                    COS ( RADIANS(", st_lat, ") ) * COS( RADIANS( y_lat ) )
                    * COS( RADIANS( x_lon ) - RADIANS(", st_lon, ") )
                    + SIN ( RADIANS(", st_lat, ") )
                    * SIN( RADIANS( y_lat ) )
                )) AS distance
            FROM geo_postcodes 
            WHERE is_active
            HAVING distance < 0.1 
            ORDER BY distance 
            LIMIT 20;
        ")
        dbc = dbConnect(MySQL(), group = 'dataOps', dbname = db_name)
        tmp <- dbGetQuery(dbc,  strSQL)
        dbDisconnect(dbc)
        locations <- rbind(locations, tmp)
        mp <- locations %>% 
                    leaflet() %>% 
                    addProviderTiles("OpenStreetMap.BlackAndWhite") %>%
                    setView(lng = st_lon, lat = st_lat, zoom = 18) %>%
                    addCircleMarkers(
                        radius = ~ifelse(type == 'station', 10, 6),
                        color = ~pal(type),
                        stroke = TRUE, 
                        fillOpacity = 0.8,
                        popup = ~postcode
                    )
        mp
    })
    
    observeEvent(
        input$btnUpdate,
        {
            if(length(input$st_tbl_rows_selected ) == 0) return(NULL)
            dbc = dbConnect(MySQL(), group = 'dataOps', dbname = db_name)
            dbSendQuery(dbc, paste('UPDATE stations SET postcode = "', input$txtPostcode, '" WHERE station_id = ', selID(), sep = '') )
            dbDisconnect(dbc)
            updateTextInput(session, 'txtPostcode', value = '')
        }
    )
    
    observeEvent(
        input$btnUpdateOA,
        {
            dbc = dbConnect(MySQL(), group = 'dataOps', dbname = db_name)
            dbSendQuery(dbc, "UPDATE stations st JOIN geo_postcodes pc ON pc.postcode = st.postcode SET st.OA = pc.OA")
            dbDisconnect(dbc)
        }
    )
    
}

shinyApp(ui = ui, server = server)
