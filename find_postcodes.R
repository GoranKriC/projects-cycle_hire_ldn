lapply(c('data.table', 'DT', 'leaflet', 'RMySQL', 'shiny'), require, character.only = TRUE)
db_conn = dbConnect(MySQL(), group = 'homeserver', dbname = 'londonCycleHire')
pal <- colorFactor(c('navy', 'red'), domain = c('station', 'neighbour') )

strSQL <- "
    SELECT * 
    FROM stations 
    WHERE is_active AND lat > 0 -- AND OA_id = '0'
    ORDER BY postcode
"
stations <- data.table(dbGetQuery(db_conn, strSQL) )
dbDisconnect(db_conn)

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
    
    y <- stations[, .(station_id, id = paste('<a href="http://maps.google.com/maps?z=18&t=m&q=loc:', lat, ',', long, '" target="_blank">', station_id, '</a>', sep = ''), address, place, area, started = first_hire, docks) ][order(area, place)]

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
                columnDefs = list( list(targets = 0, visible = FALSE) ),
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
        st_lon = stations[station_id == selID(), long]
        st_lat = stations[station_id == selID(), lat]
        locations <- data.frame(postcode = stations[station_id == selID(), postcode], type = 'station', lon = st_lon, lat = st_lat, distance = 0 )
        db_conn = dbConnect(MySQL(), group = 'homeserver', dbname = 'geographyUK')
        strSQL <- paste("
            SELECT postcode, 'neighbour' AS type, X_lon AS lon, Y_lat AS lat,
                (3959 * ACOS ( 
                    COS ( RADIANS(", st_lat, ") ) * COS( RADIANS( Y_lat ) )
                    * COS( RADIANS( X_lon ) - RADIANS(", st_lon, ") )
                    + SIN ( RADIANS(", st_lat, ") )
                    * SIN( RADIANS( Y_lat ) )
                )) AS distance
            FROM postcodes 
            WHERE is_active
            HAVING distance < 0.1 
            ORDER BY distance 
            LIMIT 20;
        ")
        tmp <- dbGetQuery(db_conn,  strSQL)
        dbDisconnect(db_conn)
        locations <- rbind(locations, tmp)
        mp <- locations %>% leaflet() %>% addProviderTiles("OpenStreetMap.BlackAndWhite") %>%
            setView(lng = st_lon, lat = st_lat, zoom = 18) %>%
            addCircleMarkers(
                radius = ~ifelse(type == 'station', 10, 6),
                color = ~pal(type),
                stroke = TRUE, 
                fillOpacity = 0.8,
                popup = ~postcode
            )
    })
    
    observeEvent(
        input$btnUpdate,
        {
            if(length(input$st_tbl_rows_selected ) == 0) return(NULL)
            db_conn = dbConnect(MySQL(), group = 'homeserver', dbname = 'londonCycleHire')
            dbSendQuery(db_conn, paste('UPDATE stations SET postcode = "', input$txtPostcode, '" WHERE station_id = ', selID(), sep = '') )
            dbDisconnect(db_conn)
            updateTextInput(session, 'txtPostcode', value = '')
        }
    )
    
    observeEvent(
        input$btnUpdateOA,
        {
            db_conn = dbConnect(MySQL(), group = 'homeserver', dbname = 'londonCycleHire')
            dbSendQuery(db_conn, "UPDATE stations st JOIN geography.postcodes pc ON pc.postcode = st.postcode SET st.OA_id = pc.OA_id")
            dbDisconnect(db_conn)
        }
    )
    
}

shinyApp(ui = ui, server = server)
