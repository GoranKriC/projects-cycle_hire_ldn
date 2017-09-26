* \>\>\> Thanks for visiting :-) This project is much work in progress at the moment <<< *

# London Cycle Hire

The data has been processed to remove trips that:
 - are taken to/from any of *test* stations 
 - were below 60 seconds in length (potentially false starts or users trying to re-dock a bike to ensure it's secure).

Milage estimates are calculated using an assumed speed of 7.456 miles per hour, up to two hours. Trips over two hours max-out at 14.9 miles.

### Scripts sequence
1. Set up 
    1. sql tables
    1. 
    1. 
1. ETL
    1. update
    1. load data
1. EDA
    1. find postcode.  
       When checking for the *best* postcode, the choice is made against *active* postcode, so the final choice could be different from what found on other web services, like Google Maps (see, for example, station 821, for which GM report a *terminated* postcode **SW118NR**, instead of the currently active **SW118NR**) 
    1. 

### Credits

 - Cycle Data.
   - Core datasets at [TFL](http://cycling.data.tfl.gov.uk/)
   - Live data at [TFL API](https://api.tfl.gov.uk/bikepoint)
   - Text to display: **Santander Cycles data supplied at (time) on (date) by Transport for London**
   - Use the Santander Cycles logo to represent the scheme on all applications and services
   - Use this cycle [pushpin icon](http://tfl.gov.uk/cdn/static/cms/images/promos/cycle-hire-pushpin-icon.gif) to indicate the location of Santander Cycles docking stations
 - postcodes.
 - OA.
 - boundaries.
 - Census Data. **Source: Office for National Statistics**
 - Design. Adapted from [SuperZIP demo @ RStudio Shiny Example](http://github.com/rstudio/shiny-examples/blob/master/063-superzip-example/).



