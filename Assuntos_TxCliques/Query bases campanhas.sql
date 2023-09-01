
select * from 
martech_curated_zone.retorno_email_sfmc
limit 5


select * from 
martech_curated_zone.log_infobip_email_pag 
limit 5





select * 
, aberturas / cast(clientes as double) as taxa_abertura 
, cliques / cast(aberturas as double) as taxa_clique
from (
select 
activityname --jornada campanha
, journeyname --campanha
, substring(activityname, 12,10) as tema
, subject
, 'Salesforce' as ferramenta
, count(distinct email) as clientes
, sum(nr_click) as cliques
, sum(nr_open) as aberturas
, max(dt_envio) as dt_envio
from 
martech_curated_zone.retorno_email_sfmc
where eventtype = 'Sent'
and tipo <> 'Transacional'
group by 1,2,3,4
union all 
select 
communication_name as activityname --jornada campanha
, communication_name as journeyname --campanha
, ''   as tema
, subject
, 'Infobip' as ferramenta
, SUM(messages_count) as clientesflag_cobranca
, sum(clicks) as cliques
, sum(opens) as aberturas
, max(send_at) as dt_envio
from 
martech_curated_zone.log_infobip_email_pag p
where status = 'Delivered'
group by 1,2,3,4
)
where clientes > 0
order by cliques desc



