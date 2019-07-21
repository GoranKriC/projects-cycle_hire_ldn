#########################################################################
# LONDON Cycle Hire - 42. Map Areas
#########################################################################

## Load packages ------------------------------------------------------------------------------------------------------
pkg <- c('data.table')
invisible(lapply(pkg, require, char = TRUE))

## Set variables ------------------------------------------------------------------------------------------------------
shp.path <- '/usr/local/share/data/boundaries/shp/ldn'
data.path <- '/usr/local/share/data/dataframes/'
proj.wgs <- '+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0'
ldn_centre <- c(-2.421976, 53.825564)
ldn_bounds <- c(-8.3, 49.9, 1.8, 59.0 )
tiles.list <- as.list(maptiles[, url])
names(tiles.list) <- maptiles[, name]
tile.ini <- tiles.list$CartoDB.Positron
loca.map <- c('LSOA', 'MSOA', 'LAD', 'PCS', 'PCD', 'PCA')
loca.ini <- 'PCS'
pal.ini <- 'Blues' # c("#CDC673", "#90EE90", "#20B2AA")

# icons for hospitals: green = nhs, red = private
hsp.icons <- awesomeIcons(
    icon = 'h-square',
    library = 'fa',
    squareMarker = TRUE,
    markerColor = sapply(centres$type, function(x) if(x == 1){ "lightgreen" } else { "lightred" }),
    iconColor = 'white'
)

# list of classification methods, to be used with classInt and ColorBrewer packages 
class.methods <- c(
#    'Fixed' = 'fixed',                  # need an additional argument fixedBreaks that lists the n+1 values to be used
    'Equal Intervals' = 'equal',        # the range of the variable is divided into n part of equal space
    'Quantiles' = 'quantile',           # each class contains (more or less) the same amount of values
#    'Pretty Integers' = 'pretty',       # sequence of about ‘n+1’ equally spaced ‘round’ values which cover the range of the values in ‘x’. The values are chosen so that they are 1, 2 or 5 times a power of 10.
    'Natural Breaks' = 'jenks',         # seeks to reduce the variance within classes and maximize the variance between classes
    'Hierarchical Cluster' = 'hclust',  # Cluster with short distance
    'K-means Cluster' = 'kmeans'        # Cluster with low variance and similar size
)

# Read boundaries as shapefiles from files in www directory
# boundaries <- lapply(loca.map, function(x) readOGR(shp.path, x))
# names(boundaries) <- loca.map
# for(m in loca.map){
#     boundaries[[m]] <- merge(boundaries[[m]], areas[, .(ons_id, nhs_id, name)], by.x = 'id', by.y = 'ons_id')
#     boundaries[[m]] <- merge(boundaries[[m]], centres[get(audit) == 1, .(H = .N), .(ons_id = get(paste0(m, '_ons')))], by.x = 'id', by.y = 'ons_id')
# }
# Read boundaries as unique list from rds shared rep
boundaries <- readRDS(paste0(data.path, 'boundaries.rds'))
for(m in loca.map){
    boundaries[[m]] <- merge(boundaries[[m]], centres[get(audit) == 1, .(H = .N), .(ons_id = get(paste0(m, '_ons')))], by.x = 'id', by.y = 'ons_id')
}

# Determines the text intervals for the colours in the map legend
get.legend.colnames <- function(bnd, mtc.type, lbl.brks, ncols) {
    if(mtc.type == 1){
        lbl.brks <- format(round(lbl.brks, 0), big.mark = ',')
    } else if(mtc.type == 2){
        lbl.brks <- format(round(100*lbl.brks, 2), nsmall = 2)
    } else {
        lbl.brks <- format(round(lbl.brks, 1), nsmall = 1)
    }
    lbl.text <- sapply(2:ncols,
        function(x)
            paste0(
                lbl.brks[x-1], ' \u2264 n < ', lbl.brks[x],
                ' (', length(bnd$Y[bnd$Y >= as.numeric(gsub(',', '', lbl.brks[x-1])) & bnd$Y < as.numeric(gsub(',', '', lbl.brks[x])) ] ), ')'
            )
    )
    lbl.text <- c(lbl.text,
        paste0(
            lbl.brks[ncols], ' \u2264 n \u2264 ', lbl.brks[ncols + 1],
            ' (', length(bnd$Y[bnd$Y >= as.numeric(gsub(',', '', lbl.brks[ncols])) & bnd$Y <= as.numeric(gsub(',', '', lbl.brks[ncols + 1])) ] ), ')'
        )
    )
}


## Define functions ---------------------------------------------------------------------------------------------------



## Load lookups -------------------------------------------------------------------------------------------------------
