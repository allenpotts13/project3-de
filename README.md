### Introduction

#### Marvan, a strategic think tank, has asked your company to build a catalog of **national** level open data related to the Covid-19 pandemic for the following countries:
- [United States](https://catalog.data.gov/dataset/?q=&sort=views_recent+desc&ext_location=United+States&ext_bbox=-124.733253%2C24.544245%2C-66.954811%2C49.388611&ext_prev_extent=) 
- [United Kindom](https://www.data.gov.uk/)
- [Canada](https://search.open.canada.ca/opendata/)

These datasets should have `Covid-19`, `SARS-CoV-2`, or `Coronavirus` in the dataset name and must be national/federal level data.
  

Marvan would like the data to be stored in this format:    

| Country | Dataset Name | Description | Last Updated | Data |    
| ------ | ------- | ------- | -------- | ---------- |
| Canada  | COVID-19 Rapid Test kits demand and usage | Percent of businesses that used COVID-19 Rapid Test kits to test on-site employees and percent of businesses that plan to test on-site employees in the next threee months by North American Industry Classification System (NAICS) for Canada and regions. Includes business' reason for not having plans to use COVID-19 Rapid Test Kits in the next three months and the percent of businesses that test employees less than once a week/once a week/twice a week/more than twice a week. Further includes percent of businesses that indicated COVID-19 Rapid Test kits as being essential personal protective equipment (PPE) or that expect to have shortages within the next 3 months. | 11/8/2023 | `<data>`
| Canada | SARS-CoV-2 antibody seroprevalence in Canadians, by age group and sex, November 2020 to April 2021 | Seroprevalence of Canadians aged 1 and older with antibodies against SARS-CoV-2 (the virus that causes COVID-19), overall and by antibody type, by age group and sex | 1/17/2023 | `<data>` |
| United States | Provisional COVID-19 death counts and rates, by jurisdiction of residence and demographic characteristics | This file contains COVID-19 death counts and rates by jurisdiction of residence (U.S., HHS Region) and demographic characteristics (sex, age, race and Hispanic origin, and age/race and Hispanic origin). United States death counts and rates include the 50 states, plus the District of Columbia. | 4/23/2025 | `<data>`

They would like to be able to retrieve the data based on a combination of filters for Country, keyword (Dataset Name and Description), and last update date.

### Process

#### Plan your approach for getting and storing the data and then execute that plan.
1. Look at each site to determine what data is available for area of interest and how that data can be accessed.
2. Decide where you will store the data.
4. Get the data and store it in the cloud or cloud simulator.

#### Create a pipeline that performs ELT
1. Read the data from the cloud or cloud simulator. 
2. Clean the data.
3. Store the cleaned data in a data warehouse.
4. Use SQL to transform the data as needed.
5. Create an API in Python that Marvan researchers can use to acquire data from the data warehouse
6. Write 3-5 unit tests where you think they are needed, run unit tests in github actions. Run static code analysis tools locally and paste the results in a txt doc and include it in the repo.  (ie. Black) 


