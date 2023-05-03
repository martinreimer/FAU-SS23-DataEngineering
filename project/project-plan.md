# Project Plan

## Summary

<!-- Describe your data science project in max. 5 sentences. -->
This data science project aims to explore the trends and patterns of bicycle traffic at specific stations in Münster (Germany) over a given time period. The project will investigate the factors that influence bicycle traffic such as time of day, day of the week, and external factors such as weather conditions and holidays. Additionally, the project will compare the bicycle traffic patterns of different locations in the city where bicycle traffic is monitored to identify any similarities or differences. Overall, the project seeks to provide insights into bicycle traffic patterns and factors that influence them.

## Rationale

<!-- Outline the impact of the analysis, e.g. which pains it solves. -->
Understanding the patterns of bicycle traffic can help city planners identify areas where bicycle infrastructure is needed and optimize existing infrastructure to better serve the needs of cyclists. This can make cycling safer and more accessible, reducing traffic congestion and improving air quality.

## Datasources

<!-- Describe each datasources you plan to use in a section. Use the prefic "DatasourceX" where X is the id of the datasource. -->

### Datasource 1: Verkehrszählung Fahrradverkehr: Tagesaktuelle Daten - Münster
* [mobilithek URL](https://mobilithek.info/offers/-6901989592576801458)
* [Data URL](https://github.com/od-ms/radverkehr-zaehlstellen)
* [Official Documentation](https://opendata.stadt-muenster.de/dataset/verkehrsz%C3%A4hlung-fahrradverkehr-tagesaktuelle-daten/resource/c072d000-ffb3-4e79-8811)
* Data Type: CSV
* License: freie Nutzung / Open Data

There are a number of bicycle counting points in the city of Münster. The Office for Mobility and Civil Engineering provides the number of cyclists counted daily at the bicycle counting stations in the GIT repository linked here on a daily basis.
Data is updated nightly and is available at 15 minute intervals. The most recent data is always in the subdirectory of the relevant counting location in the file named after the current month. Example is "04-2021.csv" for April 2021.

### Datasource 2: meteostat.net
* [Official Documentation](https://dev.meteostat.net/guide.html)
* Accessing Data by API
* Data Type: CSV
* License: CC BY-NC 4.0

Meteostat is a climate data source that provides easy access to weather data from national meteorological offices. It simplifies the process of developing weather and climate data-driven applications by offering a centralized, up-to-date database with import routines and quality assurance measures.


### Datasource 3: API-Feiertage
* [Documentation](https://www.api-feiertage.de/)
* [Data URL](https://get.api-feiertage.de/?years=2023&states=nw)
* License: no license / free of charge
* Data Type: JSON


Here you can easily select for which years and which federal states you want to call up public holidays. Then YOU can copy the displayed URL at the bottom of the generator and use it for your application.



## Work Packages
<!-- List of work packages ordered sequentially, each pointing to an issue with more details. -->
1. Automated Data Pipeline [i1](https://github.com/martinreimer/FAU-SS23-DataEngineering/issues/1)
2. Automated Tests [i2](https://github.com/martinreimer/FAU-SS23-DataEngineering/issues/2)
3. Continuous Integration [i3](https://github.com/martinreimer/FAU-SS23-DataEngineering/issues/3)
4. Deployment [i4](https://github.com/martinreimer/FAU-SS23-DataEngineering/issues/4)
5. Data Exploration [i5](https://github.com/martinreimer/FAU-SS23-DataEngineering/issues/5)
6. Data Preprocessing & Visualization [i6](https://github.com/martinreimer/FAU-SS23-DataEngineering/issues/6)

