with gasto_persona as (SELECT cd_cluster_sociodemographic,
           avg(compras.vl_real) media,
           approx_percentile(compras.vl_real, 0.75) percentil_75
      FROM platform_curated_zone.authorization_will compras
 LEFT JOIN customer_curated_zone.ca_book_cliente_cluster cluster
        ON compras.id_customer = cluster.id_customer
     WHERE cast(dt_autorizacao as date) = to_date('2023-05-29', 'yyyy-mm-dd')  
       --AND dt_autorizacao < (TIMESTAMP '2023-04-01')
       and ds_retorno = 'Aprovado'
     group by 1)
SELECT * FROM (
SELECT DISTINCT 
           compras.nr_cpf,
           compras.dt_autorizacao,
           compras.vl_real,
           compras.ds_nome_estabelecimento,
           compras.ds_cidade_estabelecimento,
           trat.ds_classificacao_nivel_3 AS ds_mcc,
           compras.is_compra_parcelada,
           compras.ds_compra,
           compras.ds_transacao,
           CASE WHEN ds_cartao = 'NAO IDENTIFICADO' AND SUBSTRING(nr_pan, 1,6) = '535016' THEN 'STANDARD_PAG'
                WHEN ds_cartao = 'NAO IDENTIFICADO' AND (SUBSTRING(nr_pan, 1,6) = '510534' OR  SUBSTRING(nr_pan, 1,6) = '557026') THEN 'BLACK'
                WHEN ds_cartao = 'NAO IDENTIFICADO' AND SUBSTRING(nr_pan, 1,6) = '546997' THEN 'STANDARD'
                WHEN ds_cartao = 'NAO IDENTIFICADO' AND (SUBSTRING(nr_pan, 1,6) = '529861' OR SUBSTRING(nr_pan, 1,6) = '223111') THEN 'AVISTA'
                ELSE ds_cartao END AS ds_cartao,
           cd_moeda,
           cluster.cd_cluster_sociodemographic,
           ie.nr_score_ie,
           case when compras.vl_real > gasto_persona.media then 1 else 0 end as vl_real_maior_media_persona, 
           case when compras.vl_real > gasto_persona.percentil_75 then 1 else 0 end as vl_real_maior_percentil_75_persona
      FROM platform_curated_zone.authorization_will compras
 LEFT JOIN customer_curated_zone.ca_dict_merchant_classifier trat
        ON trat.ds_nome_estabelecimento = compras.ds_nome_estabelecimento
 LEFT JOIN customer_curated_zone.ca_book_cliente_cluster cluster
        ON compras.id_customer = cluster.id_customer
 LEFT JOIN customer_curated_zone.ca_model_ie_cliente ie
        ON ie.cd_cpf = compras.nr_cpf
       and cast(ie.cd_yearmonth as varchar) = concat(cast(Year(dt_autorizacao) as varchar) , SUBSTR(concat('0', cast(month(dt_autorizacao)-1 as varchar)),-2))
 left join gasto_persona
        on gasto_persona.cd_cluster_sociodemographic = cluster.cd_cluster_sociodemographic
     WHERE cast(dt_autorizacao as date) = to_date('2023-05-29', 'yyyy-mm-dd')  
       --AND dt_autorizacao < (TIMESTAMP '2023-04-07')
       --and compras.nr_cpf = '03388400440'
       and ds_retorno = 'Aprovado'
)
 order by rand()
 limit 1000000
