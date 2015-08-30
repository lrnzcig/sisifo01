drop table prop_arranque_pd;
drop table prop_arranque_cs;


create table prop_arranque_pd
as select id from tuser where id in (
-- altavoces
'341657886', '173665005', '1319390838', '3131660806', '2910840034',
'20909329', '1652459718', '187564239', '261764261', '282339186', '1281187602'
-- despuŽs de clustering
,'142261530', '206778116', '2773928160', '411695020', '2921992684', '1202377658', '3135332230',
'3012430924', '2998667969', '992135034'
);

create table prop_arranque_cs
as select id from tuser where id in (
'38643994', '88146816'
-- despuŽs de clustering
,'3083854816', '2829646418', '19772265', '2175872048', '412255788', '134915924', '115540467'
);

drop table prop_tweet;
-- max_user: (true/false) ese retweeted_user_id es el m‡s representativo para retweeted_user_id
-- retweets: nœmero de retweets que ha hecho user_id de retweeted_user_id
create table prop_tweet as
select user_id, retweeted_user_id, '0' as is_max, count(*) as retweets 
from tweet
where retweet='1'
group by user_id, retweeted_user_id;


update prop_tweet
set is_max = '1'
where (user_id, retweeted_user_id) in
(select user_id, max(retweeted_user_id) keep (dense_rank last order by retweets) from prop_tweet group by user_id);

update prop_tweet pt1
set pt1.is_max = '1'
where pt1.is_max = '0'
and pt1.retweets in 
(select pt2.retweets from prop_tweet pt2 where pt2.is_max = '1' and pt2.user_id = pt1.user_id)

select count(*), is_max
from prop_tweet group by is_max;

select * from prop_tweet
order by user_id, retweets desc;

select count(*), user_id, retweets
from prop_tweet
group by user_id, retweets
order by retweets desc;


alter table prop_tweet add primary key (user_id, retweeted_user_id);
analyze table prop_tweet compute statistics;
--he creado la tabla prop_tweet_pd para simplificar la tarea a Oracle
--creando la PK , a veces, das mas opciones al optimizador.
--Le grantizas que no hay nulos, que determinadas  joins no "multiplican" registros etc
--Pero  no se si hacia falta ....


drop table prop_pd_cs;

create table prop_pd_cs as 
select cast(0 AS integer) nivel, id user_id, '0' as cs, '1' as pd
from prop_arranque_pd
union
select cast(0 AS integer) nivel, id user_id, '1' as cs, '0' as pd
from prop_arranque_cs;

select * from prop_pd_cs;

-- proceso para propagar PODEMOS
declare
	v_nivel number := 0;

begin

	loop
	
		v_nivel := v_nivel + 1;
	
		insert into prop_pd_cs
			select distinct v_nivel, prop_tweet.user_id, '0', '1'
				from prop_tweet
			 where prop_tweet.retweeted_user_id in (select USER_ID from prop_pd_cs where nivel=v_nivel-1 and pd = '1') 
				 and prop_tweet.user_id not in (select USER_ID from prop_pd_cs where pd = '1')
         and (prop_tweet.is_max = '1' or prop_tweet.retweets > 2);

		exit when sql%rowcount = 0 or v_nivel > 100;

		commit;

	end loop;

end;


select nivel, count(*) from prop_pd_cs where pd = '1' group by nivel order by 1;
select count(*) from prop_pd_cs where pd = '1';

-- proceso para propagar CIUDADANOS
declare
	v_nivel number := 0;

begin

	loop
	
		v_nivel := v_nivel + 1;
	
		insert into prop_pd_cs
			select distinct v_nivel, prop_tweet.user_id, '1', '0'
				from prop_tweet
			 where prop_tweet.retweeted_user_id in (select USER_ID from prop_pd_cs where nivel=v_nivel-1 and cs = '1') 
				 and prop_tweet.user_id not in (select USER_ID from prop_pd_cs where cs = '1')
         and (prop_tweet.is_max = '1' or prop_tweet.retweets > 2);

		exit when sql%rowcount = 0 or v_nivel > 100;

		commit;

	end loop;

end;


select nivel, count(*) from prop_pd_cs where cs = '1' group by nivel order by 1;
select count(*) from prop_pd_cs where cs = '1';


select * from prop_pd_cs
where user_id in ('3083854816');

select * from prop_pd_cs
where user_id in ('2984717442', '108971949')

-- usuarios marcados como CIUDADANOS que han salido en PODEMOS
select count(distinct(user_id))
from prop_pd_cs
where cs = '1'
and user_id in
(select distinct(user_id)
from prop_pd_cs
where pd = '1');


-- usuarios marcados como PODEMOS que han salido en CIUDADANOS
select count(distinct(user_id))
from prop_pd_cs
where pd = '1'
and user_id in
(select distinct(user_id)
from prop_pd_cs
where cs = '1');


select * from list_of_users_clustering
delete from list_of_users_clustering

insert into list_of_users_clustering
select distinct(user_id), 'belief_prop', 'cs'
from prop_pd_cs
where cs = '1'
and user_id not in
(select distinct(user_id)
from prop_pd_cs
where pd = '1');

insert into list_of_users_clustering
select distinct(user_id), 'belief_prop', 'pd'
from prop_pd_cs
where pd = '1'
and user_id not in
(select distinct(user_id)
from prop_pd_cs
where cs = '1');

commit;


select * from list_of_users_clustering
where id in ('3083854816')


-- no clasificados
select text, retweet_count
from tweet
where retweet = '0'
and retweet_count >= 1
and user_id not in (select distinct(id) from list_of_users_clustering where cluster_label = 'belief_prop')
and id not in (select distinct(id1) from clases_equi_70 where id1 != clase_equi and num_tuits > 1)

-- no clasificados, buscar usuarios err—neos
select count(*), t.user_id, u.screen_name
from tweet t, tuser u
where t.retweet = '0'
and t.retweet_count >= 1
and t.user_id not in (select distinct(id) from list_of_users_clustering where cluster_label = 'belief_prop')
and t.id not in (select distinct(id1) from clases_equi_70 where id1 != clase_equi and num_tuits > 1)
and t.id in (select distinct(id) from list_of_tweets_clustering where cluster_label = 'nc' and additional_label = '14')
and t.user_id = u.id
group by t.user_id, u.screen_name
order by count(*) desc


-- usuarios no clasificados en similitud jaccard

-- clases mezcladas
select clases.clase
from 
  (select distinct(clase_equi) as clase from clases_equi_70) clases
  ,
  (select count(*) as count, cl.clase_equi as clase, ltc.additional_label as label
  from clases_equi_70 cl, tweet t, tuser u, list_of_users_clustering ltc
  where cl.id1 = t.id
  and cl.num_tuits > 1
  and t.user_id = u.id
  and ltc.id = u.id
  and ltc.additional_label = 'pd'
  group by cl.clase_equi, cl.clase_equi, ltc.additional_label) pd
  ,
  (select count(*) as count, cl.clase_equi as clase, ltc.additional_label as label
  from clases_equi_70 cl, tweet t, tuser u, list_of_users_clustering ltc
  where cl.id1 = t.id
  and cl.num_tuits > 1
  and t.user_id = u.id
  and ltc.id = u.id
  and ltc.additional_label = 'cs'
  group by cl.clase_equi, cl.clase_equi, ltc.additional_label) cs
where clases.clase = pd.clase
and clases.clase = cs.clase

-- comprobaci—n
select count(*) as count, cl.clase_equi as clase, ltc.additional_label as label
from clases_equi_70 cl, tweet t, tuser u, list_of_users_clustering ltc
where cl.id1 = t.id
and cl.num_tuits > 1
and t.user_id = u.id
and ltc.id = u.id
and cl.clase_equi in ('595248054162317312', '595227649854861313')
group by cl.clase_equi, cl.clase_equi, ltc.additional_label

select u.screen_name, t.text
from tweet t, clases_equi_70 cl, tuser u
where cl.id1 = t.id
and cl.clase_equi in ('595248054162317312', '595227649854861313')
and t.user_id = u.id


-- usuarios no clasificados que podr’an estarlo a travŽs de la clase
select count(distinct(t.user_id))
from clases_equi_70 cl, tweet t, tuser u
where cl.id1 = t.id
and cl.num_tuits > 1
and t.user_id = u.id
and u.id not in (select distinct(id) from list_of_users_clustering)

-- usuarios s’ clasificados
select cl.clase_equi, ltc.additional_label, ltc.id
from clases_equi_70 cl, tweet t, tuser u, list_of_users_clustering ltc
where cl.id1 = t.id
and cl.num_tuits > 1
and t.user_id = u.id
and u.id = ltc.id
group by cl.clase_equi, ltc.additional_label, ltc.id

-- usuarios no clasificados con su nueva clasificaci—n
select no_clase.user_id, si_clase.label
from
  (select t.user_id as user_id, cl.clase_equi as clase
  from clases_equi_70 cl, tweet t, tuser u
  where cl.id1 = t.id
  and cl.num_tuits > 1
  and t.user_id = u.id
  and u.id not in (select distinct(id) from list_of_users_clustering)) no_clase
  ,
  (select cl.clase_equi as clase, ltc.additional_label as label
  from clases_equi_70 cl, tweet t, tuser u, list_of_users_clustering ltc
  where cl.id1 = t.id
  and cl.num_tuits > 1
  and t.user_id = u.id
  and u.id = ltc.id
  group by cl.clase_equi, ltc.additional_label) si_clase
where no_clase.clase = si_clase.clase
group by no_clase.user_id, si_clase.label
order by user_id


insert into prop_arranque_pd
select no_clase.user_id
from
  (select t.user_id as user_id, cl.clase_equi as clase
  from clases_equi_70 cl, tweet t, tuser u
  where cl.id1 = t.id
  and cl.num_tuits > 1
  and t.user_id = u.id
  and u.id not in (select distinct(id) from list_of_users_clustering)) no_clase
  ,
  (select cl.clase_equi as clase, ltc.additional_label as label
  from clases_equi_70 cl, tweet t, tuser u, list_of_users_clustering ltc
  where cl.id1 = t.id
  and cl.num_tuits > 1
  and t.user_id = u.id
  and u.id = ltc.id
  group by cl.clase_equi, ltc.additional_label) si_clase
where no_clase.clase = si_clase.clase
and si_clase.label = 'pd'
group by no_clase.user_id, si_clase.label
order by user_id

insert into prop_arranque_cs
select no_clase.user_id
from
  (select t.user_id as user_id, cl.clase_equi as clase
  from clases_equi_70 cl, tweet t, tuser u
  where cl.id1 = t.id
  and cl.num_tuits > 1
  and t.user_id = u.id
  and u.id not in (select distinct(id) from list_of_users_clustering)) no_clase
  ,
  (select cl.clase_equi as clase, ltc.additional_label as label
  from clases_equi_70 cl, tweet t, tuser u, list_of_users_clustering ltc
  where cl.id1 = t.id
  and cl.num_tuits > 1
  and t.user_id = u.id
  and u.id = ltc.id
  group by cl.clase_equi, ltc.additional_label) si_clase
where no_clase.clase = si_clase.clase
and si_clase.label = 'cs'
group by no_clase.user_id, si_clase.label
order by user_id

select * from prop_arranque_pd pd, prop_arranque_cs cs
where pd.id = cs.id

-----------------------------------
-- comprobaciones

-- usuarios que hacen retweets
select count(distinct user_id) from tweet where retweet='1' or (retweet = '0' and retweet_count > 0);
-- porcentaje de usuarios clasificados (2/3)
select count(*), additional_label
from list_of_users_clustering
where cluster_label = 'belief_prop'
group by additional_label

-- tweets (y retweets) de los usuarios clasificados
select count(*)
from tweet t, list_of_users_clustering u
where u.cluster_label = 'belief_prop'
and t.user_id = u.id

-- retweets de los usuarios clasificados por usuarios no clasificados
select count(*)
from tweet t, list_of_users_clustering u
where u.cluster_label = 'belief_prop'
and t.retweeted_user_id = u.id
and t.user_id not in 
(select user_id from list_of_users_clustering where cluster_label = 'belief_prop')

-- tweets (y retweets) de los usuarios clasificados de CS
select count(*)
from tweet t, list_of_users_clustering u
where u.cluster_label = 'belief_prop'
and t.user_id = u.id
and u.additional_label = 'cs'
--and t.retweet = '0'
--and t.retweet_count > 5

-- tweet originales cubiertos por los usuarios clasificados 
select count(*)
from tweet t, list_of_users_clustering u
where u.cluster_label = 'belief_prop'
and t.user_id = u.id
and t.retweet = '0'
and t.id in (select distinct(retweeted_id) from tweet)

-- tweet originales cubiertos por los usuarios clasificados quitando los jaccard
select count(*)
from tweet t, list_of_users_clustering u
where u.cluster_label = 'belief_prop'
and t.user_id = u.id
and t.retweet = '0'
and t.id in (select distinct(retweeted_id) from tweet)
and t.id not in (select id1 from clases_equi_70 where id1 != clase_equi)

