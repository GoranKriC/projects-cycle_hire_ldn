# London Cycle Hire

The data has been processed to remove trips that:
 - are taken to/from any of *test* stations 
 - were below 60 seconds in length (potentially false starts or users trying to re-dock a bike to ensure it's secure).

Milage estimates are calculated using an assumed speed of 7.456 miles per hour, up to two hours. Trips over two hours max-out at 14.9 miles.

### Sequence

  1.
    1. 
    1. 
    1. 
  1.  
    1. 
    1. 
  1.
    1. 
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



