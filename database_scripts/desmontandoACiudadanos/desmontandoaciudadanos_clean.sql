select count(*) from tweet;

-- clasificaci—n por usuario/localizaci—n
select u.screen_name, u.location, count(*)
from tuser u, tweet t
where t.user_id = u.id
and location is not null
group by u.screen_name, u.location
order by count(*) desc

-- totales por localizacion
select u.location, count(*)
from tuser u, tweet t
where t.user_id = u.id
group by  u.location
order by 2 desc

-- comprobar que est‡n todos los tuser
select  decode(u.rowid,null ,'TUSER NO DISPONIBLE' ,nvl(u.location,'Localizacion no especificada en tuser')) , count(*)
from tuser u, tweet t
where t.user_id = u.id(+)
group by  decode(u.rowid,null ,'TUSER NO DISPONIBLE' ,nvl(u.location,'Localizacion no especificada en tuser')) 
order by 2 desc


-- total tweets por localizaci—n, contando el nœmero de usuarios distintos que los generan
select  decode(u.rowid,null ,'TUSER NO DISPONIBLE' ,nvl(u.location,'Localizacion no especificada en tuser')) , count(*) total_tweets ,count(distinct t.user_id) distinct_users
from tuser u, tweet t
where t.user_id = u.id(+)
group by  decode(u.rowid,null ,'TUSER NO DISPONIBLE' ,nvl(u.location,'Localizacion no especificada en tuser')) 
order by count(*) desc






-- retuits
select count(*) from tweet
where text like 'RT @%'

-- hay unos 30 menos, investigar
select count(*), retweet
from tweet
group by retweet

-- comprobar aparte de esos 30 la informaci—n es congruente
select count(*) from tweet
where retweet = '1'
and retweeted_id is null

select count(*) from tweet
where retweet = '1'
and retweeted_user_id is null

select count(*) from tweet
where retweet = '1'
and retweeted_user_id not in (select distinct(id) from tuser)

select count(*) from tweet
where retweet = '1'
and retweeted_id not in (select distinct(id) from tweet)





-- tuits que han sido retuiteados
select count(*) from tweet
where text not like 'RT @%'
and id in (select retweeted_id from tweet where text like 'RT @%')

select count(*) from tweet
where retweet = '0'
and id in (select retweeted_id from tweet where retweet = '1')

-- tweets no retuiteados
select count(*) from tweet
where retweet = '0'
and id not in (select distinct(retweeted_id) from tweet where retweet = '1')



-- tweets origen Venezuela retuiteados
select count(*), retweet_count
from tweet
where retweet = '0'
and user_id in (
select id from tuser
where location in ('Caracas',
'venezuela', 'Caracas, Venezuela', 'Venezuela',
'Caracas - Venezuela','Barinas','Caracas-Venezuela',
'LA GUAIRA - EDO.VARGAS', '??T: 10.498023,-66.851165')
) 
group by retweet_count
order by retweet_count desc



-- usuarios
select count(*) from tuser
select count(distinct user_id) from tweet

-- comprobaci—n
select count(distinct user_id) from tweet
where retweet = '0' 
or (
retweet = '1' and
retweeted_id in (select id from tweet where retweet = '0')
)

-- usuarios que han retuiteado
select count(distinct user_id) from tweet
where retweet = '1'
and retweeted_id in (select id from tweet where retweet = '0')

-- usuarios con tweet original
select count(distinct user_id) from tweet
where retweet = 0

-- usuarios con tweet original y que han retuiteado
select count(distinct user_id) from(
  select user_id from tweet
  where retweet = '1'
  and retweeted_id in (select id from tweet where retweet = '0')
  intersect
  select user_id from tweet
  where retweet = '0'
)

select 22328 + 5556 - 2566 from dual



-- altavoces
-- usuarios (altavoces) que han generado un mayor nœmero de tuits
select count(*), sum(t.retweet_count), u.screen_name
from tweet t, tuser u
where t.retweet = '0'
and t.user_id = u.id
group by u.screen_name
order by sum(t.retweet_count) desc




-- distribuci—n tweets que causan retweets
-- para 13 tweets originales en el primer grupo
-- define TROZOS = 650; 
-- para 400 tweets originales en el primer grupo
define TROZOS = 21; 
select 'Del '|| to_char(100/&TROZOS*(grupo))||' % al ' || to_char(100/&TROZOS*(grupo+1))||' % ' GRUPO, 
        sum(tot_rt)/max(SUPER_TOT_RT)*100  PCT,sum(tot_rt),
        count(*) TWS_originales
from (
      select tot_rt ,trunc( (count(*) over (order by tot_rt desc ,retweeted_id)-0.001)/count(*) over ()* &TROZOS) grupo,sum(tot_rt) over () SUPER_TOT_RT,
      retweeted_user_id
      from
      (select  retweeted_id, retweeted_user_id, count(*) TOT_RT
      from tweet
       where retweeted_id in(
                            select id
                            from tweet where retweet = '0')
      and retweet = '1'
      group by retweeted_id ,retweeted_user_id
      )
)      
group by grupo      
order by grupo;



--Distibucion 2
---Àcuantos RT tienen los USERS mas retuiteados?
-- para los 11 altavoces
define TROZOS = 250;
-- para 200 usuarios
define TROZOS = 14;
select 'Del '|| to_char(100/&TROZOS*(grupo))||' % al ' || to_char(100/&TROZOS*(grupo+1))||' % ' GRUPO, 
        sum(tot_rt)/max(SUPER_TOT_RT)*100  PCT,sum(tot_rt) TOTAL_RT,
        count(*) USERS_RETUITEADOS
from (
      select tot_rt ,trunc( (count(*) over (order by tot_rt desc ,user_id)-0.001)/count(*) over ()* &TROZOS) grupo,sum(tot_rt) over () SUPER_TOT_RT,
      user_id
      from
          (select ori.user_id,count( RT.ID) TOT_RT
          from tweet ori,tweet RT
          where ori.retweet = '0'
          and RT.retweet = '1'  
          and ori.id=RT.retweeted_id
          group by ori.user_id
          )
)      
group by grupo      
order by grupo;


-- Distribucion 3 - original Agust’n
with  franjas as (
select 0 orden,0  desde from dual union all
select 1 orden,11  desde from dual union all
select 2 orden,200 desde from dual) 
select sum(USERS_RETUITEADOS) over (order by grupo)/ sum(USERS_RETUITEADOS) over ()*100 PCT_USERS_RETUITEADOS, 
        sum(PCT) over (order by grupo) PCT,sum(TOTAL_RT) over (order by grupo) TOTAL_RT,
        sum(USERS_RETUITEADOS) over (order by grupo) USERS_RETUITEADOS
from(
select  sum(tot_rt)/max(SUPER_TOT_RT)*100  PCT,sum(tot_rt) TOTAL_RT,
count(*) USERS_RETUITEADOS,grupo
from (
  select tot_rt,super_tot_rt,user_id,
   max(franjas.orden) grupo
  from(
      select tot_rt , count(*) over (order by tot_rt desc ,user_id) grupo,sum(tot_rt) over () SUPER_TOT_RT,
      user_id
      from
          (select ori.user_id,count( RT.ID) TOT_RT
          from tweet ori,tweet RT
          where ori.retweet = '0'
          and RT.retweet = '1'
          and ori.id=RT.retweeted_id
          group by ori.user_id
          )
       ) Z,franjas
       where     Z.grupo >franjas.desde
       group by tot_rt,super_tot_rt,user_id
)      
group by grupo      
)
order by grupo;



-- distribuci—n 3 bas‡ndose en la cuenta de twitter y no en los tweets de bbdd
with  franjas as (
select 0 orden,0  desde from dual union all
select 1 orden,11  desde from dual union all
select 2 orden,200 desde from dual) 
select sum(USERS_RETUITEADOS) over (order by grupo)/ sum(USERS_RETUITEADOS) over ()*100 PCT_USERS_RETUITEADOS, 
        sum(PCT) over (order by grupo) PCT,sum(TOTAL_RT) over (order by grupo) TOTAL_RT,
        sum(USERS_RETUITEADOS) over (order by grupo) USERS_RETUITEADOS
from(
select  sum(tot_rt)/max(SUPER_TOT_RT)*100  PCT,sum(tot_rt) TOTAL_RT,
count(*) USERS_RETUITEADOS,grupo
from (
  select tot_rt,super_tot_rt,user_id,
   max(franjas.orden) grupo
  from(
      select tot_rt , count(*) over (order by tot_rt desc ,user_id) grupo,sum(tot_rt) over () SUPER_TOT_RT,
      user_id
      from
          (select user_id, sum(retweet_count) TOT_RT
          from tweet
          where retweet = '0'
          group by user_id
          )
       ) Z,franjas
       where     Z.grupo >franjas.desde
       group by tot_rt,super_tot_rt,user_id
)      
group by grupo      
)
order by grupo;


-- distribuci—n 3 para retweets originales ==> ERROR
with  franjas as (
select 0 orden,0  desde from dual union all
select 1 orden,11  desde from dual union all
select 2 orden,200 desde from dual) 
select sum(USERS_RETUITEADOS) over (order by grupo)/ sum(USERS_RETUITEADOS) over ()*100 PCT_USERS_RETUITEADOS, 
        sum(PCT) over (order by grupo) PCT,sum(TOTAL_RT) over (order by grupo) TOTAL_RT,
        sum(USERS_RETUITEADOS) over (order by grupo) USERS_RETUITEADOS
from(
select  sum(tot_rt)/max(SUPER_TOT_RT)*100  PCT,sum(tot_rt) TOTAL_RT,
count(*) USERS_RETUITEADOS,grupo
from (
  select tot_rt,super_tot_rt,user_id,
   max(franjas.orden) grupo
  from(
      select tot_rt , count(*) over (order by tot_rt desc ,user_id) grupo,sum(tot_rt) over () SUPER_TOT_RT,
      user_id
      from
          (select user_id, count(*) TOT_RT
          from tweet 
          where retweet = '0'
          and id in (select distinct(retweeted_id) from tweet)
          group by user_id
          )
       ) Z,franjas
       where     Z.grupo >franjas.desde
       group by tot_rt,super_tot_rt,user_id
)      
group by grupo      
)
order by grupo;


-- distribuci—n 3 para usuarios alcanzados ==> ERROR
with  franjas as (
select 0 orden,0  desde from dual union all
select 1 orden,11  desde from dual union all
select 2 orden,200 desde from dual) 
select sum(USERS_RETUITEADOS) over (order by grupo)/ sum(USERS_RETUITEADOS) over ()*100 PCT_USERS_RETUITEADOS, 
        sum(PCT) over (order by grupo) PCT,sum(TOTAL_RT) over (order by grupo) TOTAL_RT,
        sum(USERS_RETUITEADOS) over (order by grupo) USERS_RETUITEADOS
from(
select  sum(tot_rt)/max(SUPER_TOT_RT)*100  PCT,sum(tot_rt) TOTAL_RT,
count(*) USERS_RETUITEADOS,grupo
from (
  select tot_rt,super_tot_rt,user_id,
   max(franjas.orden) grupo
  from(
      select tot_rt , count(*) over (order by tot_rt desc ,user_id) grupo,sum(distinct(tot_rt)) over () SUPER_TOT_RT,
      user_id
      from
          (select ori.user_id, RT.user_id TOT_RT
          from tweet ori,tweet RT
          where ori.retweet = '0'
          and RT.retweet = '1'
          and ori.id=RT.retweeted_id
          group by ori.user_id, RT.user_id
          )
       ) Z,franjas
       where     Z.grupo >franjas.desde
       group by tot_rt,super_tot_rt,user_id
)      
group by grupo      
)
order by grupo;

-- retweets originales de un grupo
with  franjas as (
select 0 orden,0  desde from dual union all
select 1 orden,11  desde from dual union all
select 2 orden,200 desde from dual) 
select count(*) from tweet 
where retweet = '0'
and id in (select distinct(retweeted_id) from tweet)
and user_id in (
select user_id
from(
  select tot_rt,super_tot_rt,user_id,
   max(franjas.orden) grupo
  from(
      select tot_rt , count(*) over (order by tot_rt desc ,user_id) grupo,sum(tot_rt) over () SUPER_TOT_RT,
      user_id
      from
          (select ori.user_id,count( RT.ID) TOT_RT
          from tweet ori,tweet RT
          where ori.retweet = '0'
          and RT.retweet = '1'
          and ori.id=RT.retweeted_id
          group by ori.user_id
          )
       ) Z,franjas
       where     Z.grupo >franjas.desde
       group by tot_rt,super_tot_rt,user_id
  order by grupo
) where grupo = 0
or grupo = 1
)

select statuses_count from tuser
where id in '341657886'

select count(*) from tuser
select count(distinct(user_id)) from tweet


-- delete from tuser where id = '18909241'
select * from tuser
where id in ('18909241', '50373757')

select screen_name from tuser
where id in ('341657886',
'173665005',
'1319390838',
'3131660806',
'2910840034',
'20909329',
'1652459718',
'187564239',
'261764261',
'282339186',
'1281187602')


-- retweets de un grupo (comprobaci—n)
with  franjas as (
select 0 orden,0  desde from dual union all
select 1 orden,11  desde from dual union all
select 2 orden,200 desde from dual) 
select count(*) from tweet 
where retweet = '1'
and retweeted_user_id in (
select user_id
from(
  select tot_rt,super_tot_rt,user_id,
   max(franjas.orden) grupo
  from(
      select tot_rt , count(*) over (order by tot_rt desc ,user_id) grupo,sum(tot_rt) over () SUPER_TOT_RT,
      user_id
      from
          (select ori.user_id,count( RT.ID) TOT_RT
          from tweet ori,tweet RT
          where ori.retweet = '0'
          and RT.retweet = '1'
          and ori.id=RT.retweeted_id
          group by ori.user_id
          )
       ) Z,franjas
       where     Z.grupo >franjas.desde
       group by tot_rt,super_tot_rt,user_id
  order by grupo
) where grupo = 0
or grupo = 1
)


-- usuarios alcanzados por un grupo (comprobaci—n)
with  franjas as (
select 0 orden,0  desde from dual union all
select 1 orden,11  desde from dual union all
select 2 orden,200 desde from dual) 
select count(distinct(user_id)) from tweet 
where retweet = '1'
and retweeted_user_id in (
select user_id
from(
  select tot_rt,super_tot_rt,user_id,
   max(franjas.orden) grupo
  from(
      select tot_rt , count(*) over (order by tot_rt desc ,user_id) grupo,sum(tot_rt) over () SUPER_TOT_RT,
      user_id
      from
          (select ori.user_id,count( RT.ID) TOT_RT
          from tweet ori,tweet RT
          where ori.retweet = '0'
          and RT.retweet = '1'
          and ori.id=RT.retweeted_id
          group by ori.user_id
          )
       ) Z,franjas
       where     Z.grupo >franjas.desde
       group by tot_rt,super_tot_rt,user_id
  order by grupo
) where grupo = 0
--or grupo = 1
)

-- usuarios que retuitean a los 11 altavoces
select count(distinct(rt.user_id))
from tweet rt
where rt.retweet = '1'
and rt.retweeted_user_id in ('341657886', '173665005', '1319390838', '3131660806', '2910840034',
'20909329', '1652459718', '187564239', '261764261', '282339186', '1281187602')
and rt.user_id not in ('341657886', '173665005', '1319390838', '3131660806', '2910840034',
'20909329', '1652459718', '187564239', '261764261', '282339186', '1281187602')

-- usuarios 2do nivel que retuitean a los 11 altavoces
select count(distinct(rt.user_id))
from tweet rt
where rt.retweet = '1'
and rt.retweeted_user_id in 
(select distinct(rt.user_id)
from tweet rt
where rt.retweet = '1'
and rt.retweeted_user_id in ('341657886', '173665005', '1319390838', '3131660806', '2910840034',
'20909329', '1652459718', '187564239', '261764261', '282339186', '1281187602')
and rt.user_id not in ('341657886', '173665005', '1319390838', '3131660806', '2910840034',
'20909329', '1652459718', '187564239', '261764261', '282339186', '1281187602')
)
and rt.user_id not in
(select distinct(rt.user_id)
from tweet rt
where rt.retweet = '1'
and rt.retweeted_user_id in ('341657886', '173665005', '1319390838', '3131660806', '2910840034',
'20909329', '1652459718', '187564239', '261764261', '282339186', '1281187602')
and rt.user_id not in ('341657886', '173665005', '1319390838', '3131660806', '2910840034',
'20909329', '1652459718', '187564239', '261764261', '282339186', '1281187602')
)

-- usuarios 3er nivel
select count(distinct(rt.user_id))
from tweet rt
where rt.retweet = '1'
and rt.retweeted_user_id in 
(select distinct(rt.user_id)
from tweet rt
where rt.retweet = '1'
and rt.retweeted_user_id in 
(select distinct(rt.user_id)
from tweet rt
where rt.retweet = '1'
and rt.retweeted_user_id in ('341657886', '173665005', '1319390838', '3131660806', '2910840034',
'20909329', '1652459718', '187564239', '261764261', '282339186', '1281187602')
and rt.user_id not in ('341657886', '173665005', '1319390838', '3131660806', '2910840034',
'20909329', '1652459718', '187564239', '261764261', '282339186', '1281187602')
)
and rt.user_id not in
(select distinct(rt.user_id)
from tweet rt
where rt.retweet = '1'
and rt.retweeted_user_id in ('341657886', '173665005', '1319390838', '3131660806', '2910840034',
'20909329', '1652459718', '187564239', '261764261', '282339186', '1281187602')
and rt.user_id not in ('341657886', '173665005', '1319390838', '3131660806', '2910840034',
'20909329', '1652459718', '187564239', '261764261', '282339186', '1281187602')
)
)
and rt.user_id not in
(select distinct(rt.user_id)
from tweet rt
where rt.retweet = '1'
and rt.retweeted_user_id in 
(select distinct(rt.user_id)
from tweet rt
where rt.retweet = '1'
and rt.retweeted_user_id in ('341657886', '173665005', '1319390838', '3131660806', '2910840034',
'20909329', '1652459718', '187564239', '261764261', '282339186', '1281187602')
and rt.user_id not in ('341657886', '173665005', '1319390838', '3131660806', '2910840034',
'20909329', '1652459718', '187564239', '261764261', '282339186', '1281187602')
)
and rt.user_id not in
(select distinct(rt.user_id)
from tweet rt
where rt.retweet = '1'
and rt.retweeted_user_id in ('341657886', '173665005', '1319390838', '3131660806', '2910840034',
'20909329', '1652459718', '187564239', '261764261', '282339186', '1281187602')
and rt.user_id not in ('341657886', '173665005', '1319390838', '3131660806', '2910840034',
'20909329', '1652459718', '187564239', '261764261', '282339186', '1281187602')
)
union
select count(distinct(rt.user_id))
from tweet rt
where rt.retweet = '1'
and rt.retweeted_user_id in ('341657886', '173665005', '1319390838', '3131660806', '2910840034',
'20909329', '1652459718', '187564239', '261764261', '282339186', '1281187602')
and rt.user_id not in ('341657886', '173665005', '1319390838', '3131660806', '2910840034',
'20909329', '1652459718', '187564239', '261764261', '282339186', '1281187602')
)



select count(distinct(user_id))
select user_id, retweeted_user_id, level, text
from tweet
where retweet = '1'
--and level > 1
and user_id in ('341657886', '173665005', '1319390838', '3131660806', '2910840034',
'20909329', '1652459718', '187564239', '261764261', '282339186', '1281187602')
connect by prior user_id = retweeted_user_id  and retweet = '0'
--start with retweeted_user_id in ('341657886')
start with retweeted_user_id in ('341657886', '173665005', '1319390838', '3131660806', '2910840034',
'20909329', '1652459718', '187564239', '261764261', '282339186', '1281187602')
