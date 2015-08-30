
  
  ---Calculo los tuits+retuits que tiene cada clase 
  select clases_equi_70.CLASE_EQUI,count(*)+clases_equi_70.num_tuits tot_TW_y_RT_por_clase
  from clases_equi_70,tweet 
  where clases_equi_70.id1=tweet.retweeted_id 
  and tweet.retweet = '1'
group by clases_equi_70.CLASE_EQUI,clases_equi_70.num_tuits  ;    /* RT + TW de cada clase_equi */



---Calculo que clases hay en cada uno de los clusters/additional_labels ,sin repetirlas (DISTINCT)
select DISTINCT ltc.cluster_label, ltc.additional_label,ce70.clase_equi
from list_of_tweets_clustering ltc,clases_equi_70 ce70
where ltc.id=ce70.id1;



--Hago la join de las dos consultas  anteriores para obtener el TOTAL de TW y RT por cluster_label/additional
select cluster_clases.cluster_label,cluster_clases.additional_label,sum(tot_tw_clase.tot_TW_y_RT_por_clase) TOTAL
from 
     (select distinct ltc.cluster_label, ltc.additional_label,ce70.clase_equi
      from list_of_tweets_clustering ltc,clases_equi_70 ce70
      where ltc.id=ce70.id1) cluster_clases
       ,
     (select clases_equi_70.CLASE_EQUI,count(*)+clases_equi_70.num_tuits tot_TW_y_RT_por_clase
     from clases_equi_70,tweet 
     where clases_equi_70.id1=tweet.retweeted_id 
     and tweet.retweet = '1'
     group by clases_equi_70.CLASE_EQUI,clases_equi_70.num_tuits  ) tot_tw_clase
where  cluster_clases.clase_equi=tot_tw_clase.CLASE_EQUI
group by cluster_clases.cluster_label,cluster_clases.additional_label
;

--Voy a calcular el % 
--Ya no se de donde cojones has sacado el 86248   .Lo mas parecido que he conseguido ha sido: (si no esta bien , mete tu consulta)
  select sum(tot) from(
  select count(*) tot from tweet where (retweet = '0'  and id in (select  retweeted_id from tweet where retweet = '1'))
  union all
  select count(*) tot from tweet where (retweet = '1'  and retweeted_id in (select  id from tweet where retweet = '0'))
  )
  

--ahora lo  meto en la select de mas arriba


select cluster_clases.cluster_label,cluster_clases.additional_label,sum(tot_tw_clase.tot_TW_y_RT_por_clase) TOTAL,trunc(sum(tot_tw_clase.tot_TW_y_RT_por_clase)/supertotal.tot *100,2) PCT_TOTAL 
from 
     (select distinct ltc.cluster_label, ltc.additional_label,ce70.clase_equi
      from list_of_tweets_clustering ltc,clases_equi_70 ce70
      where ltc.id=ce70.id1 ) cluster_clases
       ,
     (select clases_equi_70.CLASE_EQUI,count(*)+clases_equi_70.num_tuits tot_TW_y_RT_por_clase
     from clases_equi_70,tweet 
     where clases_equi_70.id1=tweet.retweeted_id 
     and tweet.retweet = '1'
     group by clases_equi_70.CLASE_EQUI,clases_equi_70.num_tuits  ) tot_tw_clase
     ,
     (
     select sum(tot) tot from(
      select count(*) tot from tweet where (retweet = '0'  and id in (select  retweeted_id from tweet where retweet = '1'))
      union all
      select count(*) tot from tweet where (retweet = '1'  and retweeted_id in (select  id from tweet where retweet = '0'))
     )
     ) SUPERTOTAL     /*sin join mas abajo, porque solo hay un registro*/
where  cluster_clases.clase_equi=tot_tw_clase.CLASE_EQUI
group by cluster_clases.cluster_label,cluster_clases.additional_label,supertotal.tot
order by cluster_clases.cluster_label, PCT_TOTAL desc





--deberias verificar que no hay tweets que pertecen a la misma clase dentro de cluster,additional_label distintos.  (??)
--Si te sale alguno , hay que pensar tambien que mis clases no estan del todo bien .Hay ,por lo menos, 5 clases mal( eso si, creo que con menos de 5 tweets cada una)

select clase_equi,count(*)
from
 (select distinct ltc.cluster_label, ltc.additional_label,ce70.clase_equi
      from list_of_tweets_clustering ltc,clases_equi_70 ce70
      where ltc.id=ce70.id1
   )
group by clase_equi 
having count(*)>1;


---Si te sale algo en la consulta anterior,puedes intentar  averiguar la causa con la consulta siguiente

select  ce70.clase_equi, ltc.cluster_label, ltc.additional_label,ltc.id
      from list_of_tweets_clustering ltc,clases_equi_70 ce70
      where ltc.id=ce70.id1
      and ce70.clase_equi in (select clase_equi
                              from
                             (select distinct ltc.cluster_label, ltc.additional_label,ce70.clase_equi
                              from list_of_tweets_clustering ltc,clases_equi_70 ce70
                             where ltc.id=ce70.id1
                               )
                            group by clase_equi 
                            having count(*)>1)
order by 1,2,3,4;



