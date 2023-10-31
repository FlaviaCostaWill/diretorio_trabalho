select 
id_issue
, cpf_customer
, mes_chat
, substring(cast(dt_envio_mensagem as varchar), 1, 10) as dt_envio_mensagem
, array_agg(ds_message) as ds_message
from (
	select
	id_issue
	, cpf_customer
	, cast(mes_chat as date) as mes_chat, 
	ds_message
	, max(dt_envio_mensagem) as dt_envio_mensagem
	from (
	select distinct issue.id_issue 
	, dt_envio_mensagem
	, issue.cpf_customer 
	, date_trunc('month', issue.dt_criacao_chat) as mes_chat
	,  ds_message
	from cx_curated_zone.helpshift_issues_messages msg
	--left join cx_curated_zone.helpshift_tag_niveis AS helpshift_tag 
	--    on msg.id_issue = helpshift_tag.id_issue
	--left join processed_zone_api_cxm_tags.tags_hierarquia_gerencial as novas_tags 
	 --   on trim(lower(n3)) = trim(lower(nm_tag_n3))
	left join cx_curated_zone.helpshift_issues issue 
	on (issue.id_issue  = msg.id_issue)
	left join customer_curated_zone.ca_book_cliente ci 
	on (ci.cpf = issue.cpf_customer)
	where cast(dt_criacao_chat as date) >= to_date('2023-08-01', 'yyyy-mm-dd')
	and   cast(dt_criacao_chat as date) < to_date('2023-10-01', 'yyyy-mm-dd')
	and ds_entidade = 'cliente'
	and lower(ds_message) <> 'screenshot sent'
	        and lower(ds_message) <> 'ok'
	        and lower(ds_message) <> 'outros assuntos'
	        and lower(ds_message) <> 'não, ainda tenho dúvidas'
	        and lower(ds_message) <> 'obrigado'
	        and lower(ds_message) <> 'obrigada'
	        and lower(ds_message) <> 'quero falar de outra coisa'
	        and lower(ds_message) <> 'quero falar com atendimento'
	        and lower(ds_message) <> 'falar atendimento'
	        and lower(ds_message) <> 'quero falar com atendente'
	        and lower(ds_message) <> 'nenhuma das opções'
	        and lower(ds_message) <> 'certo'
	        and lower(ds_message) <> 'responde'
	        and lower(ds_message) <> 'falar com atendente'
	        and lower(ds_message) <> 'ainda preciso de ajuda'
	        and lower(ds_message) <> 'sim, ainda preciso de ajuda'
	        and lower(ds_message) <> 'voltar pras opções de antes'
	        and lower(ds_message) <> 'me mostra as opções de antes'
	        and lower(ds_message) <> 'quero negociar pelo chat'
	        and lower(ds_message) <> 'quero negociar a fatura'
	        and lower(ds_message) <> 'é outra coisa'
	        and lower(ds_message) <> 'nenhuma dessas'
	        and lower(ds_message) <> 'sim'
	        and lower(ds_message) <> 'não'
	        and lower(ds_message) <> 'nao'
	        and lower(ds_message) <> 'oi'
	        and lower(ds_message) <> '?'
	        and lower(ds_message) <> '??'
	        and lower(ds_message) <> 'bom dia'
	        and lower(ds_message) <> 'boa tarde'
	        and lower(ds_message) <> 'boa noite'
	        and is_resolvido_bot = 0
)
	group by 1,2,3,4
)
group by 1,2,3,4