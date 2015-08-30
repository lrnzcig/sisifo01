-- tweets originales clasificados
select count(*), cluster_label
from list_of_tweets_clustering
group by cluster_label

-- tweets y retweets clasificados
select count(*), ltc.cluster_label
from tweet t, list_of_tweets_clustering ltc
where (t.id = ltc.id 
and t.retweet = '0')
or (t.retweeted_id = ltc.id
and t.retweet = '1')
group by ltc.cluster_label


-- CUENTAS PARA CLUSTERING FINAL sin jaccard
select count(*)*100/86248, ltc.cluster_label, ltc.additional_label
from tweet t, list_of_tweets_clustering ltc
where (t.id = ltc.id 
and t.retweet = '0')
or (t.retweeted_id = ltc.id
and t.retweet = '1')
group by ltc.cluster_label, ltc.additional_label
order by ltc.cluster_label, count(*) desc

-- con jaccard ==> no va!
select count(*)*100/86248, ltc.cluster_label, ltc.additional_label
from tweet t, list_of_tweets_clustering ltc, clases_equi_70 jac
where ((t.id = ltc.id 
      and t.retweet = '0')
    or (t.retweeted_id = ltc.id
      and t.retweet = '1'))
or ((t.id = jac.id1
      and jac.id1 != jac.clase_equi)
    or (t.retweeted_id = jac.id1
      and jac.id1 != jac.clase_equi))
group by ltc.cluster_label, ltc.additional_label
order by ltc.cluster_label, count(*) desc


-- usuarios de un cluster KKKKKK!!!! 
select cluster_clases.cluster_label,cluster_clases.additional_label,sum(tot_tw_clase.tot_TW_y_RT_por_clase) TOTAL, 
       tot_tw_clase.user_id, tot_tw_clase.screen_name
from 
     (select distinct ltc.cluster_label, ltc.additional_label,ce70.clase_equi
      from list_of_tweets_clustering ltc,clases_equi_70 ce70
      where ltc.id=ce70.id1) cluster_clases
       ,
     (select clases_equi_70.CLASE_EQUI,count(*)+clases_equi_70.num_tuits tot_TW_y_RT_por_clase, u.screen_name screen_name, t.user_id user_id
     from clases_equi_70, tweet t, tuser u, tweet orig
     where clases_equi_70.id1= t.retweeted_id 
     and t.retweet = '1'
     and orig.retweet = '0'
     and orig.id = t.retweeted_id
     and orig.user_id = u.id
     group by clases_equi_70.CLASE_EQUI,clases_equi_70.num_tuits, u.screen_name, t.user_id) tot_tw_clase
where  cluster_clases.clase_equi=tot_tw_clase.CLASE_EQUI
and cluster_label = 'nc'
and additional_label = '1'
group by cluster_clases.cluster_label,cluster_clases.additional_label, tot_tw_clase.user_id, tot_tw_clase.screen_name
order by total desc
;

-- a–adir como podemos
'2147633173'
'373366361'
'414678568'

-- a–adir como cs
'2984717442'
'108971949'


-- buscar retweets que cambian el mensaje ==> PENDIENTE
select rt.text, orig.text, '%' || substr(rt.text, 30, 100) || '%'
--select count(*)
from tweet orig, tweet rt
where rt.retweet = '1'
and orig.retweet = '0'
and orig.id = rt.retweeted_id
and orig.text not like '%' || substr(rt.text, 30, 100) || '%'

select count(*)
from tweet orig, tweet rt
where rt.retweet = '1'
and orig.retweet = '0'
and orig.id = rt.retweeted_id
and rt.text not like 'RT @%'



-- tweets eliminados con distancia distancia jaccard
select count(*) from clases_equi_70
where num_tuits > 1
and id1 != clase_equi

-- tweets originales jaccard
select count(distinct(clase_equi)) from clases_equi_70
where num_tuits > 1

-- select * from clases_equi_70 order by num_tuits, clase_equi


-- tweets originales retweeteados menos distancia jaccard
select count(*) from tweet
where retweet = '0'
and id in (select distinct(retweeted_id) from tweet)
and id not in 
(select id1 from clases_equi_70
where num_tuits > 1
and id1 != clase_equi)

-- retweets originados por los tweets eliminados por jaccard
select count(*) from tweet
where retweet = '1'
and retweeted_id in
(select id1 from clases_equi_70
where num_tuits > 1
and id1 != clase_equi)

-- tweets originales de podemos quitando distancia jaccard
select count(*)
from tweet t, list_of_users_clustering u
where u.cluster_label = 'belief_prop'
and t.user_id = u.id
and u.additional_label = 'pd'
and t.retweet = 0
and t.id not in (
select id1 from clases_equi_70
where num_tuits > 0
and id1 != clase_equi
)


-- pruebas sencillas jaccard
select text
from tweet
where retweet = '0'
and text like '%eco del%'

select count(*)
from tweet
where retweet = '0'
and text like '%eco del%'

select count(*)
from tweet t, list_of_users_clustering u
where t.retweet = '0'
and t.text like '%eco del%'
and t.user_id = u.id
and u.cluster_label = 'belief_prop'

select text
from tweet
where retweet = '0'
and text like '%convertido en tendencia%'

select count(*)
from tweet
where retweet = '0'
and text like '%convertido en tendencia%'

select count(*)
from tweet t, list_of_users_clustering u
where t.retweet = '0'
and text like '%convertido en tendencia%'
and t.user_id = u.id
and u.cluster_label = 'belief_prop'