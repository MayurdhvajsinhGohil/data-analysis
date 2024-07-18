-- Data cleaning


select *
FROM layoffs;

-- 1. remove duplicates
-- 2. standardize the data
-- 3. Null values or blank values
-- 4. Remove any columns


create table layoff_staging
like layoffs;

select *
FROM layoff_staging;

INSERT layoff_staging
select *
from layoffs;



select *,
ROW_number() over(
partition by company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
FROM layoff_staging;

with duplicate_cte as
(
select *,
ROW_number() over(
partition by company , location , industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
FROM layoff_staging
)

select *
from duplicate_cte
where row_num > 1;


select * 
from layoff_staging
where company = 'casper';




with duplicate_cte as
(
select *,
ROW_number() over(
partition by company , location , industry, total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) as row_num
FROM layoff_staging
)

DELETE
from duplicate_cte
where row_num > 1;





CREATE TABLE `layoff_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


select * 
from layoff_staging2;


INSERT INTO layoff_staging2
select *,
ROW_number() over(
partition by company , location , industry, total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) as row_num
FROM layoff_staging;


DELETE 
from layoff_staging2
where row_num > 1;


select *
from layoff_staging2
where row_num > 1;

select *
from layoff_staging2;



-- standardizing data

select company, trim(company)
from layoff_staging2;


UPDATE layoff_staging2
set company = trim(company);

SELECT  distinct industry
FROM layoff_staging2;


SELECT  *
FROM layoff_staging2
where industry like 'crypto%';

update layoff_staging2
set industry = 'crypto'
where industry like 'crypto%';


select distinct country, trim(trailing '.' from country)
from layoff_staging2
order by 1 ;

update layoff_staging2
set country = trim(trailing '.' from country)
where country like 'united states';


select `date`
from layoff_staging2;

UPDATE layoff_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y');


ALTER table layoff_staging2
modify column `date` DATE;	

SELECT *
FROM layoff_staging2
where total_laid_off is null
and percentage_laid_off is null;

update layoff_staging2
set industry = null
where industry = '';


SELECT *
FROM layoff_staging2
where industry is null 
OR industry  = '';

SELECT *
FROM layoff_staging2
where company like 'bally%';

SELECT t1.industry , t2.industry
FROM layoff_staging2 t1
JOIN layoff_staging2 t2
	on t1.company = t2.company
    and t1.location = t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoff_staging2 t1
JOIN layoff_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry 
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;


SELECT *
FROM layoff_staging2;


SELECT *
FROM layoff_staging2
where total_laid_off is null
and percentage_laid_off is null;



DELETE 
FROM layoff_staging2
where total_laid_off is null
and percentage_laid_off is null;


ALTER table layoff_staging2
drop column row_num;

SELECT *
FROM layoff_staging2;


Select max(total_laid_off), max(percentage_laid_off)
from layoff_staging2;

select *
from layoff_staging2
where percentage_laid_off = 1
order by total_laid_off desc;

select company, sum(total_laid_off)
from layoff_staging2
group by company
order by 2 desc;

select min(`date`) , max(`date`)
from layoff_staging2;

select year(`date`) , sum(total_laid_off)
from layoff_staging2
group by year(`date`)
order by 1 desc;

select stage , sum(total_laid_off)
from layoff_staging2
group by stage
order by 2 desc;

select company , sum(percentage_laid_off)
from layoff_staging2
group by company
order by 2 desc;

select *
from layoff_staging2;

select company , year(`date`) as year,  sum(total_laid_off)
from layoff_staging2
group by company, year( `date`)
order by 3 desc;

WITH company_year (company, years, total_laid_off) as
(
select company , year(`date`) as year,  sum(total_laid_off)
from layoff_staging2
group by company, year( `date`)
), company_year_rank as
(select *,
 dense_rank() OVER (partition by years order by total_laid_off DESC) as ranking
from company_year
where years is not null
)
select *
from company_year_rank
where ranking <=  5
;

