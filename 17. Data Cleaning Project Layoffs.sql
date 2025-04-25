-- Data Cleaning Project

SELECT *
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data (spelling issues, etc)
-- 3. Look at NULL or Blank values to possibly fill (same as 4)
-- 4. Remove unnecessary rows and columns (sometimes good, sometimes shouldn't do)

-- Here we create a raw version of the data table, run, refresh...
-- First making the columns...
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;
-- Now insert the data... run (should be no output, again)
-- Now when selecting & running the l_s table, it should be an exact copy
INSERT layoffs_staging
SELECT *
FROM layoffs;
-- Now, Removing Duplicates. Date has back ticks because date is a mySQL keyword
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;
-- Let's put this in a CTE
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;
-- Now you can verify if the duplicates are indeed duplicates
SELECT *
FROM layoffs_staging
WHERE company = 'Oda';
-- HA! We caught one where Oda is actually NOT a duplicate. So now, let's add ALL columns to the CTE...
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;
-- Now, we can delete only 1, not both, of each duplicate. On left window l_s, r click, copy to clipboard, create statement & paste here
-- Added row_num (column name) and int (data type), highlight it all, run (no output)
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;
-- Once everything is done, no rows should appear when running l_s2
SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- Step 2, Standardizing Data

SELECT company, TRIM(company)
FROM layoffs_staging2;
-- Trimming, update, no output, run above again
UPDATE layoffs_staging2
SET company = TRIM(company);
-- Here, we see 3 Crypto industries, should just be one row since it's all the same
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';
-- Location has 3 with unique characters, could be a different language thing, but other than that, looks ok
SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;
-- 2 US's so let's fix that using trim to take away a period at the end of US
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';
-- Now, date is text data style, that needs to be changed for visualization purposes
-- Let's format it to how we want it, yyyy-mm-dd
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;
-- Now we can change it from text to date column
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Step 3, Nulls and Blanks

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL;
-- Hmm... so there are some with % laid off is also null... we'll come back to this later...
-- 1st let's change the blanks to nulls
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL;
-- So, we have 4. Let's see if we can populate these by searching for matching company
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';
-- So we have another, and the industry is Travel according to it
SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL)
AND t2.industry IS NOT NULL;
-- So, here we have located all entries that have null or blank industry with matching company & location to another that doesn't have null industry
-- Simply scroll right to see t2 and the industry column to see what it should be
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL)
AND t2.industry IS NOT NULL;
-- After updating, no rows should populate when running the join query 

-- Step 4, Removal of unnecessary rows & columns
-- Here, lots of rows, irl not sure if we'd actually delete it, but just for an example we are
-- Difficult to trust data on layoffs that has no actual layoff numbers...
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Now let's drop the row_num column from the table
SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;































