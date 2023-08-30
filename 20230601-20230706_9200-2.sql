-- 日期/股票代號/買張/賣張/淨買張/庫存/昨日庫存/買均價/賣均價/收盤價/昨日收盤價/昨日庫存損益/今日進場損益/今日出場損益/今日損益

DECLARE @sDate date = '20230601', @eDate date = '20230706', @broker char(4) = '9200';

WITH position as(
SELECT *, (T.買張-T.賣張) as 淨買張,
sum(T.買張-T.賣張) over (partition by T.股票代號 order by T.日期) as 庫存,
ROW_NUMBER() over (partition by T.股票代號 order by T.日期) as incremental
from
(
	SELECT table3.日期, table3.股票代號, ISNULL(買張, 0) as 買張, ISNULL(賣張, 0) as 賣張, ISNULL(買均價, 收盤價) as 買均價, ISNULL(賣均價, 收盤價) as 賣均價,  收盤價
	from
	(
		SELECT d.日期, m.股票代號
		from
			(
			SELECT distinct 日期
			from [BranchData].[dbo].[個股自營商進出表]
			where [日期] between @sDate and @eDate
			) d
			cross join
			(
			SELECT distinct 股票代號
			from [BranchData].[dbo].[個股自營商進出表]
			where [日期] between @sDate and @eDate and 券商代號 = @broker
			) m
	) table1
	left OUTER join
	(
	SELECT 日期, 股票代號, 買張, 賣張
	from [BranchData].[dbo].[個股自營商進出表]
	where [日期] between @sDate and @eDate and 券商代號 = @broker
	) table2
	on table1.日期 = table2.日期 and table1.股票代號 = table2.股票代號
	left outer join
	(
	SELECT  日期, 股票代號, 買均價, 賣均價
	FROM [TwCMData].[dbo].[日個股進出表]
	WHERE 日期 between @sDate  and @eDate and 券商代號  = @broker
	) table4
	on table4.日期 = table2.日期 and table4.股票代號 = table2.股票代號
	right outer join
	(
	SELECT 日期, 股票代號, 收盤價
	FROM [TwCMData].[dbo].[日收盤還原表排行]
	WHERE 日期 between @sDate  and @eDate
	) table3
	on table2.日期 = table3.日期 and table2.股票代號 = table3.股票代號

) as T
),
price as(
select *, isnull(lag(庫存,1) over (partition by 股票代號 order by incremental), 0) as 昨日庫存,
isnull(lag(收盤價,1) over (partition by 股票代號 order by incremental), 0) as 昨日收盤價
from position),
pl as(
select *, (收盤價-昨日收盤價)*昨日庫存*1000 as 昨日庫存損益,
(收盤價-買均價)*買張*1000 as 今日進場損益,
(賣均價-收盤價)*賣張*1000 as 今日出場損益
from price)
select 日期, 股票代號, 買張, 賣張, 淨買張, 庫存, 昨日庫存, 買均價, 賣均價, 收盤價, 昨日收盤價, 昨日庫存損益, 今日進場損益, 今日出場損益,
昨日庫存損益+今日進場損益+今日出場損益 as 今日損益
from pl
order by 股票代號, 日期
;