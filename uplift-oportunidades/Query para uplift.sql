select * from 
martech_curated_zone.retorno_email_sfmc
limit 5

select * from growth_curated_zone.clientes
limit 5

select * from 
martech_curated_zone.log_infobip_email_pag
limit 5

select distinct tp_publico_experimento from 
platform_analytics_zone.pmm_pool_ds_cartoes_experimentos_metricas
limit 5

select min(dt_comunicacao) from platform_analytics_zone.pmm_pool_ds_cartoes_experimentos_metricas
--2023-05-05

/*
tp_publico_experimento
Alvo
GC Campanha
GC
*/

select * from 
platform_analytics_zone.pmm_pool_ds_cartoes_experimentos_metricas
limit 5


-----------------------------
-------- ACERTAR ------------
-----------------------------
select * from 
customer_curated_zone.ca_model_rfe_credito
limit 5


select * from 
customer_curated_zone.ca_model_ie_cliente 
limit 5


select * from 
customer_analytics_zone.ca_analitico_basao_crm
limit 5



select distinct
--a.id_customer
a.cpf 
, case when tp_publico_experimento = 'Alvo' then 1 else 0 end as f_tratamento
, case when vl_total_spending_m0 > 0 then 1 else 0 end as f_conversao
, b.ds_persona
, b.ds_perfil_research
, b.ds_seg_consumo
, cr.ds_rfe_perfil_credito

from  platform_analytics_zone.pmm_pool_ds_cartoes_experimentos_metricas a
left join customer_analytics_zone.ca_analitico_basao_crm b on (a.cpf = b.cd_cpf)
left join customer_curated_zone.ca_model_rfe_credito cr on (a.id_customer  = cr.id_customer and date_trunc('month', date_add('day', -1, a.dt_comunicacao)) = cr.dt_referencia)
left join platform_curated_zone.transaction_dedicada_spending s on (a.cpf = s.cpf)
--id_cliente,  dt_data

limit 10


--IE do mes anterior






-----------------------------
-------- ACERTAR ------------
-----------------------------


with base_campanhas as (
select * from (
select 
activityname --jornada campanha
, journeyname --campanha
, subject
, id_customer
, sum(nr_click) as cliques
, sum(nr_open) as aberturas
, min(dt_envio) as dt_envio
from 
martech_curated_zone.retorno_email_sfmc
where eventtype = 'Sent'
and tipo <> 'Transacional'
group by 1,2,3,4
union all 
select 
communication_name as activityname --jornada campanha
, communication_name as journeyname --campanha
, subject
, cl.id_customer
, sum(clicks) as cliques
, sum(opens) as aberturas
, min(send_at) as dt_envio
from 
martech_curated_zone.log_infobip_email_pag p
inner join growth_curated_zone.clientes cl on (cl.email = p."to")
where status = 'Delivered'
group by 1,2,3,4
)
where lower(journeyname) not like '%%cobranca%%'
and lower(journeyname) not like '%%cobrança%%'
and lower(journeyname) not like '%%aquisicao%%'
and lower(journeyname) not like '%%aquisição%%'
and lower(subject) not like '%%willclipping%%' 
and lower(subject) not like '%%relatório%%'
and lower(subject) not like '%%boletim%%'
and lower(subject) not like '%%correios%%'
and lower(subject) not like '%%newsletter%%'
and lower(journeyname) not like '%%teste%%'
and lower(subject) not like '%%teste%%'
and lower(journeyname) not like '%%pesquisa%%'
and lower(journeyname) not like '%%comunicação interna%%'
and lower(journeyname) not like '%%café%%'
and lower(journeyname) not like '%%cxm%%'
and lower(journeyname) not like '%%acesso%%'
and lower(subject) not like '%%[will bank e pag%%'
and lower(subject) not like '%%[willbank]%%'
and lower(subject) not like '%%[meu pag]%%'
and lower(subject) not like '%%[meupag]%%'
and lower(subject) not like '%%[will bank]%%'
and lower(subject) not like '%%pag amarelo%%'
and lower(journeyname) not like '%%unresolved communication%%'
and lower(subject) not like '%%certificado%%'
and lower(journeyname) not like '%%ri_pessoas%%'
and lower(subject) not like '%%will em um minuto%%'
and lower(subject) not like '%%chegaram os vouchers%%'
and dt_envio >= to_date('2023-01-01','yyyy-mm-dd')
)
, de_para as (
select distinct
date_trunc('month', dt_envio) as mes_ref
from base_campanhas
)
, conversao_cartao as (
select distinct id_cliente,  dt_data
from platform_curated_zone.transaction_dedicada_spending s
inner join  de_para c on (date_trunc('month', cast(s.dt_dia as date)) = c.mes_ref )
)
, basao_cross_join as (
select * from 
customer_analytics_zone.ca_analitico_basao_crm b
cross join de_para c
)
--CONCEITO DE CONVERSÃO
, flags_principais as (
select
coalesce(coalesce(b.id_customer, ct.id_cliente), c.id_customer) as id_customer,
date_trunc('month', coalesce(c.dt_envio, ct.dt_data)) as mes_ref,
case when c.id_customer is null then 0 else 1 end as f_tratamento,
case when ct.id_cliente is null then 0 else 1 end as f_conversao, 
case when c.aberturas > 0 then 1 else 0 end as f_comunicacao
from basao_cross_join b 
full outer join base_campanhas c on (c.id_customer = b.id_customer and date_trunc('month', c.dt_envio) = b.mes_ref)
--full outer join conversao_cartao ct on (ct.id_cliente = c.id_customer and date_diff('day', c.dt_envio, ct.dt_data) <= 7 and date_diff('day', c.dt_envio, ct.dt_data) > 0 )
full outer join conversao_cartao ct on (ct.id_cliente = b.id_customer and date_trunc('month', ct.dt_data) = b.mes_ref )
)
, com_aleatorizacao as (
select 
*
--, row_number() over (partition by f_tratamento, f_conversao order by rand()) as ordem
, row_number() over (partition by f_tratamento order by rand()) as ordem
from flags_principais
)
select 
f_tratamento, f_conversao,
count(distinct id_customer) as clientes 
from (
select 
b.id_customer
, b.f_tratamento, b.f_conversao, b.f_comunicacao
, c.pc_iu_ult_mes
, c.pc_iu_mes_atual
, c.vl_spending_medio_3m
, c.ds_classif_application
, c.ds_classif_behavior
, c.ds_classif_compra_online
, c.nr_score_ie_credito
, c.nr_score_ie_conta
, c.ds_rfe_credito
, c.ds_persona
, c.ds_perfil_research
, c.ds_seg_consumo
from 
com_aleatorizacao b
left join customer_analytics_zone.ca_analitico_basao_crm c on (c.id_customer = b.id_customer)
where ordem <= 200000
)
group by 1,2




with base_campanhas as (
select * from (
select 
activityname --jornada campanha
, journeyname --campanha
, subject
, id_customer
, sum(nr_click) as cliques
, sum(nr_open) as aberturas
, min(dt_envio) as dt_envio
from 
martech_curated_zone.retorno_email_sfmc
where eventtype = 'Sent'
and tipo <> 'Transacional'
group by 1,2,3,4
union all 
select 
communication_name as activityname --jornada campanha
, communication_name as journeyname --campanha
, subject
, cl.id_customer
, sum(clicks) as cliques
, sum(opens) as aberturas
, min(send_at) as dt_envio
from 
martech_curated_zone.log_infobip_email_pag p
inner join growth_curated_zone.clientes cl on (cl.email = p."to")
where status = 'Delivered'
group by 1,2,3,4
)
where lower(journeyname) not like '%%cobranca%%'
and lower(journeyname) not like '%%cobrança%%'
and lower(journeyname) not like '%%aquisicao%%'
and lower(journeyname) not like '%%aquisição%%'
and lower(subject) not like '%%willclipping%%' 
and lower(subject) not like '%%relatório%%'
and lower(subject) not like '%%boletim%%'
and lower(subject) not like '%%correios%%'
and lower(subject) not like '%%newsletter%%'
and lower(journeyname) not like '%%teste%%'
and lower(subject) not like '%%teste%%'
and lower(journeyname) not like '%%pesquisa%%'
and lower(journeyname) not like '%%comunicação interna%%'
and lower(journeyname) not like '%%café%%'
and lower(journeyname) not like '%%cxm%%'
and lower(journeyname) not like '%%acesso%%'
and lower(subject) not like '%%[will bank e pag%%'
and lower(subject) not like '%%[willbank]%%'
and lower(subject) not like '%%[meu pag]%%'
and lower(subject) not like '%%[meupag]%%'
and lower(subject) not like '%%[will bank]%%'
and lower(subject) not like '%%pag amarelo%%'
and lower(journeyname) not like '%%unresolved communication%%'
and lower(subject) not like '%%certificado%%'
and lower(journeyname) not like '%%ri_pessoas%%'
and lower(subject) not like '%%will em um minuto%%'
and lower(subject) not like '%%chegaram os vouchers%%'
and dt_envio >= to_date('2023-01-01','yyyy-mm-dd')
)
, conversao_cartao as (
select distinct id_cliente,  dt_data
from platform_curated_zone.transaction_dedicada_spending 
where dt_data >= to_date('2023-01-01','yyyy-mm-dd')
)
, flags_principais as (
select
coalesce(c.id_customer, ct.id_cliente) as id_customer,
date_trunc('month', coalesce(c.dt_envio, ct.dt_data)) as mes_ref,
case when c.id_customer is null then 0 else 1 end as f_tratamento,
case when ct.id_cliente is null then 0 else 1 end as f_conversao,
case when c.aberturas > 0 then 1 else 0 end as f_comunicacao
from base_campanhas c
full outer join conversao_cartao ct on (ct.id_cliente = c.id_customer and date_diff('day', c.dt_envio, ct.dt_data) <= 7 and date_diff('day', c.dt_envio, ct.dt_data) > 0 )
where c.id_customer is not null or ct.id_cliente is not null
)
, com_aleatorizacao as (
select 
*, row_number() over (partition by f_tratamento, f_conversao, f_comunicacao order by rand()) as ordem
from flags_principais
)
, base_final as (
select 
b.id_customer
, b.f_tratamento, b.f_conversao, b.f_comunicacao
, c.pc_iu_ult_mes
, c.pc_iu_mes_atual
, c.vl_spending_medio_3m
, c.ds_classif_application
, c.ds_classif_behavior
, c.ds_classif_compra_online
, c.nr_score_ie_credito
, c.nr_score_ie_conta
, c.ds_rfe_credito
, c.ds_persona
, c.ds_perfil_research
, c.ds_seg_consumo
from 
com_aleatorizacao b
left join customer_analytics_zone.ca_analitico_basao_crm c on (c.id_customer = b.id_customer)
where ordem <= 250000
)
select
f_tratamento, f_conversao, f_comunicacao, count(distinct id_customer) as clientes
from base_final
group by 1,2,3




