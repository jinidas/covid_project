/* This project explores COVID-19 cases, deaths, and vaccination trends using SQL. The project involves data cleaning, 
transformations, and complex SQL queries to extract meaningful patterns. By leveraging joins, window functions, 
CTEs, and views, we analyze the pandemic's impact on different locations and continents.*/

create database covidproject;
use covidproject;
create table CovidDeaths (
iso_code varchar (50),
continent varchar (100),
location varchar (100),
date date,
population int,
total_cases int,
new_cases int,
new_cases_smoothed decimal(6,4),
total_deaths int,
new_deaths int,
new_deaths_smoothed decimal(6,4),
total_cases_per_million decimal(6,4),
new_cases_per_million decimal(6,4),
new_cases_smoothed_per_million decimal(6,4),
total_deaths_per_million decimal(6,4),
new_deaths_per_million decimal(6,4),
new_deaths_smoothed_per_million decimal(6,4),
reproduction_rate decimal(6,4),
icu_patients int,
icu_patients_per_million decimal(6,4),
hosp_patients int,
hosp_patients_per_million decimal(6,4),
weekly_icu_admissions decimal(6,4),
weekly_icu_admissions_per_million decimal(6,4),
weekly_hosp_admissions decimal(6,4),
weekly_hosp_admissions_per_million decimal(6,4)
);

drop table covidvaccin;


create table covidvaccin
( iso_code varchar(50),
continent varchar(50),
location varchar(100),	
date varchar(50),
new_tests int,
total_tests int,
total_tests_per_thousand decimal(6,4),
new_tests_per_thousand decimal(6,4),
new_tests_smoothed int,
new_tests_smoothed_per_thousand decimal(6,4),
positive_rate decimal(6,4),
tests_per_case decimal(6,4),
tests_units varchar(100),
total_vaccinations int,
people_vaccinated int,
people_fully_vaccinated int,
new_vaccinations int,
new_vaccinations_smoothed int,
total_vaccinations_per_hundred decimal(6,4),
people_vaccinated_per_hundred decimal(6,4),
people_fully_vaccinated_per_hundred decimal(6,4),
new_vaccinations_smoothed_per_million int,
stringency_index decimal(6,4),
population_density decimal(6,4),
median_age decimal(6,4),
aged_65_older decimal(6,4),
aged_70_older decimal(6,4),
gdp_per_capita decimal(6,4),
extreme_poverty decimal(6,4),
cardiovasc_death_rate decimal(6,4),
diabetes_prevalence decimal(6,4),
female_smokers decimal(6,4),
male_smokers decimal(6,4),
handwashing_facilities decimal(6,4),
hospital_beds_per_thousand decimal(6,4),
life_expectancy decimal(6,4),
human_development_index decimal(6,4)
);

LOAD DATA LOCAL INFILE 
"C:\\Users\\JINIA\\Downloads\\CovidDeaths.csv"
INTO TABLE  CovidDeaths
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n' IGNORE 1 ROWS;



LOAD DATA LOCAL INFILE "C:\\Users\\JINIA\\Downloads\\Covidvaccin.csv" INTO TABLE covidvaccin 
 FIELDS TERMINATED BY ',' 
 ENCLOSED BY '"' 
 LINES TERMINATED BY '\n' IGNORE 1 ROWS;
 select * from covidvaccin limit 5;
 
 select *
 from coviddeaths
 where continent is not null
 order by 1,2;
 
 -- Looking at the total_cases vs total_deaths
 -- Showing likelihood of dying in any particular country
 select location,date,total_cases,total_deaths,round((total_deaths/total_cases),4) * 100 as DeathPercentage
 from coviddeaths
 where continent <> ""
 order by 1,2;
 
  select location,date,total_cases,total_deaths,round((total_deaths/total_cases),4) * 100 as DeathPercentage
 from coviddeaths
 where location = "India" and continent <> ""
 order by 1,2;
 
 
 -- Looking at the total cases vs population
 -- Showing what percentage of population got covid in any particular country, here India
 select location,date,total_cases,population,round((total_cases/population),4) * 100 as CasePercentage
 from coviddeaths
 where location = "India"
 order by 1,2;
 
 -- Looking at countries with Highest infection rate compared to population
 select location,max(total_cases) as HighestInfCount,population,max((total_cases/population)) * 100 as PercentofPopultionInf
 from coviddeaths
 -- where location = "India"
 group by 1,3
 order by PercentofPopultionInf desc;
 
 -- Showing countries with highest death count per population and by continents
 select continent,location, max(total_deaths) as highestdeathcount
 from coviddeaths
 where continent <> ""
 group by continent,location
 order by highestdeathcount  desc;

 select continent,location, max(total_deaths) as highestdeathcount
 from coviddeaths
 where continent = ""
 group by continent,location
 order by highestdeathcount  desc;
 
 -- global numbers
 select date,sum(new_cases) as totalnewcases, sum(new_deaths) as totalnewdeaths ,round(sum(new_deaths)/sum(new_cases),4) * 100 as DeathPercentage
 from coviddeaths
 where continent <> ""
 group by date;
 
  select sum(new_cases) as totalnewcases, sum(new_deaths) as totalnewdeaths ,round(sum(new_deaths)/sum(new_cases),4) * 100 as DeathPercentage
 from coviddeaths
 where continent <> "";
 
 -- GUYS WE HAVE TABLE NO 2, COVIDVACCIN. LET'S USE IT.
 select * from covidvaccin;
 
alter table covidvaccin
add column newdate date;

SET SQL_SAFE_UPDATEs =0 ; 
Update covidvaccin
SET newdate = Str_To_Date(date, "%d-%m-%Y");
alter table covidvaccin
drop column date;

-- LETS JOIN TWO TABLES AND GET SOME OUTPUT.
SELECT cd.location, cd.date , cv.location , cv.date
from coviddeaths cd join covidvaccin cv on cd.location = cv.location and cd.date=cv.date; 

-- Looking at the total population vs vaccination
SELECT cd.location, cd.date , cd.continent , cd.population , cv.new_vaccinations
from coviddeaths cd join covidvaccin cv on cd.location = cv.location and cd.date=cv.date
where cd.continent <> ""
order by 2,3; 

-- if we want total number of vaccination location wise grouped output. 
SELECT cd.location, cd.date , cd.continent , cd.population , cv.new_vaccinations, 
sum(cv.new_vaccinations) over (partition by cd.location order by cd.location , cd.date) as locationwisevaccinrollup
from coviddeaths cd join covidvaccin cv on cd.location = cv.location and cd.date=cv.date
where cd.continent <> "" and cd.location = "India" -- for example
order by 2,3; 

-- now to know the how many people are vaccinated in a specific location, we need to use CTE, LETS DO IT...
with popVSvac
 as
(	SELECT cd.location, cd.date , cd.continent , cd.population , cv.new_vaccinations, 
	sum(cv.new_vaccinations) over (partition by cd.location order by cd.location , cd.date) as locationwisevaccinrollup
	from coviddeaths cd join covidvaccin cv on cd.location = cv.location and cd.date=cv.date
	where cd.continent <> "" and cd.location = "India" -- for example
	order by 2,3
)
select * , (locationwisevaccinrollup/population) * 100 as perctagepeoplevacclocationwise
 from popVSvac;
 
 -- WITH TEMP TABLE... 
 create temporary table percentpoplvaccnted
	SELECT cd.location, cd.date , cd.continent , cd.population , cv.new_vaccinations, 
	sum(cv.new_vaccinations) over (partition by cd.location order by cd.location , cd.date) as locationwisevaccinrollup
	from coviddeaths cd join covidvaccin cv on cd.location = cv.location and cd.date=cv.date
	where cd.continent <> "" and cd.location = "India" -- for example
	order by 2,3;
select * , (locationwisevaccinrollup/population) * 100 as perctagepeoplevacclocationwise
 from percentpoplvaccnted;  
/* DIFF BETWEEN CTE AND TEMP TABLE IS CTE WITH BE EFFECTIVE TILL A SELECT STATEMENT IS EXECUTED,
  WHEREAS TEMP TABLE WILL BE ACTIVE TILL A PARTICULAR SESSION. */    
  
-- Creating VIEW to store data for later use
create view percentpoplvaccnted as
	SELECT cd.location, cd.date , cd.continent , cd.population , cv.new_vaccinations, 
	sum(cv.new_vaccinations) over (partition by cd.location order by cd.location , cd.date) as locationwisevaccinrollup
	from coviddeaths cd join covidvaccin cv on cd.location = cv.location and cd.date=cv.date
	where cd.continent <> "" and cd.location = "India" -- for example
	order by 2,3;
    
select * , (locationwisevaccinrollup/population) * 100 as perctagepeoplevacclocationwise
 from percentpoplvaccnted; 
 
 
 /* This SQL-based COVID-19 data analysis successfully extracts key insights, such as infection trends, mortality rates,
 and vaccination progress. The use of aggregations, partitions, and CTEs demonstrates proficiency in handling large datasets efficiently.
To further enhance the project, incorporating data visualizations using Power BI or Python could provide a more intuitive 
representation of the findings. Additionally, performance improvements such as indexing and query optimization 
can make the analysis more scalable.
 */
 
 


 
 

 
 
 
 
 
 
 
 
 
 
 
 
 
 