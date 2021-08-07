SELECT max(datetime),p.name,l.name,l.shortname,min(scalarvalue),max(scalarvalue) FROM timeseries.timeseriesvaluesandflags tsv 
join timeseries.timeseries ts on ts.timeserieskey = tsv.timeserieskey
join timeseries.parameter p on p.parameterkey = ts.parameterkey
join timeseries.location l on l.locationkey = ts.locationkey
where l.name in ('Meettrailer01_RD','Meettrailer02_RD') 
group by l.name, p.name,l.shortname
order by l.name

select * FROM timeseries.timeseriesvaluesandflags
where datetime >= '2021-01-23 17:41:01'::timestamp

SELECT max(datetime) from timeseries.timeseriesvaluesandflags tsv 
join timeseries.timeseries ts on ts.timeserieskey = tsv.timeserieskey
join timeseries.location l on l.locationkey = ts.locationkey
join timeseries.filesource f on f.filesourcekey = ts.filesourcekey
where f.filesourcekey = 3
group by datetime
order by datetime desc

-- select * from timeseries.location
-- select * from timeseries.parameter
-- select count(*) from timeseries.timeseriesvaluesandflags