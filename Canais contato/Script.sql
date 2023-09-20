
select 
c.cpf
, max(p.ds_cluster_sociodemographic) as persona
, max(case when interacao_email <> 'nunca' then 1 else 0 end) as flag_email
, max(case when interacao_push <> 'nunca' then 1 else 0 end) as flag_push
from martech_curated_zone.historico_acionamento_pmm_cliente c
left join customer_curated_zone.ca_book_cliente cli on (cli.cpf = c.cpf)
left join customer_curated_zone.ca_book_cliente_cluster p on (p.id_customer = cli.id_customer) 
where (ult_disparo_email is not null or ult_disparo_push is not null)
and (interacao_email <> 'nunca' or interacao_push <> 'nunca')
group by 1
order by rand()
limit 500000




SELECT * FROM 
martech_curated_zone.historico_acionamento_pmm_cliente
LIMIT 5

select 
date_trunc('month', ult_disparo_email) as mes_envio_email --ultimo mes
, date_trunc('month',  ult_disparo_push) as mes_envio_push
, date_trunc('month',  ult_disparo_sms) as mes_envio_sms
, count(distinct cpf) as clientes
from martech_curated_zone.historico_acionamento_pmm_cliente
group by 1,2,3


select 
count(distinct cpf) as clientes
from martech_curated_zone.historico_acionamento_pmm_cliente
where ult_disparo_email is not null
and ult_disparo_push is not null 
and ult_disparo_sms is not null 
--1917340

select 
count(distinct cpf) as clientes
from martech_curated_zone.historico_acionamento_pmm_cliente
--5404921