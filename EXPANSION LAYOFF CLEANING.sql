select SUBSTRING(`DATE`, 1,7) AS MONTH, SUM(total_laid_off)
from layoff_staging2
WHERE SUBSTRING(`DATE`, 1,7) IS NOT NULL
group by `MONTH`
ORDER BY 1 ASC;

with rolling_total as
(
select SUBSTRING(`DATE`, 1,7) AS MONTH, SUM(total_laid_off) as total_off
from layoff_staging2
WHERE SUBSTRING(`DATE`, 1,7) IS NOT NULL
group by `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, total_off,
SUM(total_off) over(order by `MONTH`) AS rolling_total
from rolling_total;