select * from growth_curated_zone.proposal_analysis  
limit 5


select * from customer_curated_zone.ca_book_cobranca 
limit 5

select distinct ds_approval_type from growth_curated_zone.proposal_analysis


select dt_created_proposal,  dt_cfi_account
, date_diff('day', cast(dt_created_proposal as date), cast(dt_cfi_account as date)) as tempo_aprovacao 
from growth_curated_zone.proposal_analysis
where dt_cfi_account is not null
limit 10


select date_diff('day', cast(dt_created_proposal as date), cast(dt_cfi_account as date)) as tempo_aprovacao 
, count(distinct cpf) as clientes
from growth_curated_zone.proposal_analysis
where dt_cfi_account is not null
group by 1
order by 2 desc


select date_diff('day', cast(dt_created_proposal as date), cast(dt_sent_biometry as date)) as tempo_aprovacao 
, count(distinct cpf) as clientes
from growth_curated_zone.proposal_analysis
where dt_cfi_account is not null
group by 1
order by 2 desc


select * from growth_curated_zone.proposal_general
limit 10


select distinct ds_occupation 
, count(*) as registros
from growth_curated_zone.proposal_general
where lower(ds_occupation) like '%autonom%' or lower(ds_occupation) like '%autônom%'
group by 1
order by 2 desc

--where lower(ds_occupation) like '%autonom%' or lower(ds_occupation) like '%autônom%'

select * from 
customer_analytics_zone.ca_base_calculo_ie_conta
where cpf = '09198900757'
limit 5


select distinct nm_evento from 
marketplace_curated_zone.mktp_consolidado_eventos_loja_will
limit 5


select * from
customer_curated_zone.ca_analitico_frontend
limit 5

select distinct ds_event_name from
customer_curated_zone.ca_analitico_frontend
where lower(ds_event_name) like '%mgm%'


select count(cpf) as clientes from
customer_curated_zone.ca_analitico_frontend
where lower(ds_event_name) like '%click_button_sendinvite_telamgm%'
or lower(ds_event_name) like '%click_card_mgm_timeline%'
or lower(ds_event_name) like '%click_button_sendinvite_screenmgmhome%'

select * from
customer_curated_zone.ca_analitico_frontend
where lower(ds_event_name) like '%lojawill%' --lojawill loja-will ou marketplace
limit 5
-- ds_event_name <> 
login_knownuser | insert_textfield_password_loginknownuser | home | click_box_tracking_card_home
insert_textfield_password_loginknownuser | home | click_box_tracking_card_home | login_knownuser



-----------------------------------------------------------
---------------------- QUERY FINAL ------------------------
-----------------------------------------------------------

drop table customer_sandbox_zone.dados_estudo_mgm_flavia;

create table customer_sandbox_zone.dados_estudo_mgm_flavia as 
with gestao_carteira as (
select
cpf
, max(vl_current_limit) as max_limite
, max(nr_institutions) as max_qtd_if
, avg(vl_util_will) as avg_utilizado_interno
, sum(vl_util_will) as sum_utilizado_interno
, avg(vl_util_market) as avg_utilizado_mercado
, sum(vl_util_market) as sum_utilizado_mercado
from customer_curated_zone.ca_book_gestao_carteira 
group by 1
)
, cobranca as (
select
id_customer
, max(nr_days_paste_due_current) as nr_days_paste_due_current
from (
	select
	id_customer , nr_days_paste_due_current 
	, row_number() over (partition by id_customer order by cd_yearmonth desc) as num_ordem
	from customer_curated_zone.ca_book_cobranca 
)
where num_ordem = 1
group by 1
)
, features_conta as (
	select 
	cpf 
	, max(nr_chaves_importantes) as nr_chaves_importantes
	, max(nr_antecipacao_fatura) as nr_antecipacao_fatura
	, max(nr_bolso_ativo) as nr_bolso_ativo
	, max(nr_transacoes_cashin + nr_transacoes_pix_cashout + nr_transacoes_ted_cashout) as nr_trans_mov_conta
	from customer_analytics_zone.ca_base_calculo_ie_conta
	group by 1
)
, transacional as (
	select 
	id_customer
	, avg(vl_total_spending) as avg_vl_total_spending
	, sum(nr_purchases_virtual_card_online + nr_purchases_virtual_card_presentially) as nr_purchases_virtual
	, sum(nr_purchases_online) as nr_purchases_online
	from  customer_curated_zone.ca_book_cartao
	group by 1
)
, eventos as (
	select 
	cpf
	, count(distinct dt_event) as nr_acessos
	, count(distinct case when lower(ds_event_name) like '%lojawill%' or lower(ds_event_name) like '%loja-will%' or lower(ds_event_name) like '%marketplace%' then dt_event end) as nr_acessos_lojawill --ds_event_name 
	, max(case when lower(ds_event_name) like '%click_button_sendinvite_telamgm%' or lower(ds_event_name) like '%click_card_mgm_timeline%' or lower(ds_event_name) like '%click_button_sendinvite_screenmgmhome%' then 1 else 0 end) as flag_mgm
	from customer_curated_zone.ca_analitico_frontend
	where lower(ds_event_name) <> 'login_knownuser'
	and lower(ds_event_name) <> 'insert_textfield_password_loginknownuser'
	and lower(ds_event_name) <> 'home'
	and lower(ds_event_name) <> 'click_box_tracking_card_home'
	group by 1
)
, base_principal as (
 select 
	cli2.cpf 
	, max(date_diff('day', cast(pr.dt_created_proposal as date), cast(pr.dt_cfi_account as date))) as tempo_aprovacao --até criação efetiva da conta
	, max(date_diff('day', cast(pr.dt_cfi_account as date), current_date)) as tempo_relacionamento
	, max(gc.max_limite) - min(pr.nr_approved_limit_engine) as ganho_limite
	, max(pr.ds_risk_type) as ds_risk_type
	, max(pr2.ds_gender) as gender
	, max(pr2.nr_declared_income) as nr_declared_income
	, max(pr2.ds_region) as ds_region
	, max(case when lower(pr2.ds_occupation) like '%autonom%' or lower(pr2.ds_occupation) like '%autônom%' then 1 else 0 end) as flag_autonomo
	, max(gc.max_limite) as max_limite
	, max(gc.max_qtd_if) as max_qtd_if
	, sum(gc.avg_utilizado_interno) as avg_utilizado_interno
	, sum(gc.sum_utilizado_interno) as sum_utilizado_interno
	, sum(gc.avg_utilizado_mercado) as avg_utilizado_mercado
	, sum(gc.sum_utilizado_mercado) as sum_utilizado_mercado
	, sum(ic.nr_chaves_importantes) as nr_chaves_importantes
	, sum(ic.nr_antecipacao_fatura) as nr_antecipacao_fatura
	, sum(ic.nr_bolso_ativo) as nr_bolso_ativo
	, sum(ic.nr_trans_mov_conta) as nr_trans_mov_conta
	, sum(t.nr_purchases_virtual) as nr_purchases_virtual
	, sum(t.avg_vl_total_spending) as avg_vl_total_spending
	, sum(t.nr_purchases_online) as nr_purchases_online
	, sum(e.nr_acessos) as nr_acessos
	, sum(e.nr_acessos_lojawill) / cast(sum(e.nr_acessos) as double) as pc_acessos_lojawill
	, max(e.flag_mgm) as flag_mgm
	, max(cob.nr_days_paste_due_current) as dias_atraso
from customer_curated_zone.ca_book_status_cliente cli1
	inner join (
		select distinct id_customer, cpf from 
		customer_curated_zone.ca_book_cliente) cli2 
		on (cli1.id_customer = cli2.id_customer)
	left join growth_curated_zone.proposal_analysis pr on (pr.cpf = cli2.cpf)
	left join growth_curated_zone.proposal_general pr2 on (pr2.cd_cpf = cli2.cpf)
	left join gestao_carteira gc on (gc.cpf = cli2.cpf)
	left join features_conta ic on (ic.cpf = cli2.cpf)
	left join transacional t on (t.id_customer = cli1.id_customer)
	left join cobranca cob on (cob.id_customer = cli1.id_customer)
	left join eventos e on (e.cpf = cli2.cpf)
where pr.ds_approval_type = 'credito' and cob.nr_days_paste_due_current <= 5
group by 1
)
, com_aleatorizacao as (
select 
*
, row_number() over (partition by flag_mgm order by rand()) as ordem
, case when (avg_utilizado_mercado + avg_utilizado_interno) > max_limite then 1 else 0 end as flag_limite_insuficiente
, coalesce(coalesce(sum_utilizado_interno, 0.0)/(coalesce(sum_utilizado_interno, 0.0) + coalesce(sum_utilizado_mercado, 0.0)),0) as share_of_wallet
from base_principal
)
select * from com_aleatorizacao
where ordem <= 500000


---------------------------------------------------
----------- CONTAGEM FINAL ------------------------
---------------------------------------------------
--2794376 de clientes (adimplentes ate 5 dias), sem conta simples
with gestao_carteira as (
select
cpf
, max(vl_current_limit) as max_limite
, sum(vl_util_will) as sum_utilizado_interno
, sum(vl_util_market) as sum_utilizado_mercado
from customer_curated_zone.ca_book_gestao_carteira 
group by 1
)
, cobranca as (
select
id_customer
, max(nr_days_paste_due_current) as nr_days_paste_due_current
from (
	select
	id_customer , nr_days_paste_due_current 
	, row_number() over (partition by id_customer order by cd_yearmonth desc) as num_ordem
	from customer_curated_zone.ca_book_cobranca 
)
where num_ordem = 1
group by 1
)
, features_conta as (
	select 
	cpf 
	, max(nr_transacoes_cashin + nr_transacoes_pix_cashout + nr_transacoes_ted_cashout) as nr_trans_mov_conta
	from customer_analytics_zone.ca_base_calculo_ie_conta
	group by 1
)
, eventos as (
	select 
	cpf
	, max(case when lower(ds_event_name) like '%click_button_sendinvite_telamgm%' or lower(ds_event_name) like '%click_card_mgm_timeline%' or lower(ds_event_name) like '%click_button_sendinvite_screenmgmhome%' then 1 else 0 end) as flag_mgm
	from customer_curated_zone.ca_analitico_frontend
	where lower(ds_event_name) <> 'login_knownuser'
	and lower(ds_event_name) <> 'insert_textfield_password_loginknownuser'
	and lower(ds_event_name) <> 'home'
	and lower(ds_event_name) <> 'click_box_tracking_card_home'
	group by 1
)
, base_principal as (
 select 
	cli2.cpf 
	, max(date_diff('day', cast(pr.dt_cfi_account as date), current_date)) as tempo_relacionamento
	, max(gc.max_limite) - min(pr.nr_approved_limit_engine) as ganho_limite
	, sum(gc.sum_utilizado_interno) as sum_utilizado_interno
	, sum(gc.sum_utilizado_mercado) as sum_utilizado_mercado
	, sum(ic.nr_trans_mov_conta) as nr_trans_mov_conta
	, max(e.flag_mgm) as flag_mgm
	, max(cob.nr_days_paste_due_current) as dias_atraso
from customer_curated_zone.ca_book_status_cliente cli1
	inner join (
		select distinct id_customer, cpf from 
		customer_curated_zone.ca_book_cliente) cli2 
		on (cli1.id_customer = cli2.id_customer)
	left join growth_curated_zone.proposal_analysis pr on (pr.cpf = cli2.cpf)
	left join gestao_carteira gc on (gc.cpf = cli2.cpf)
	left join features_conta ic on (ic.cpf = cli2.cpf)
	left join cobranca cob on (cob.id_customer = cli1.id_customer)
	left join eventos e on (e.cpf = cli2.cpf)
where pr.ds_approval_type = 'credito' and cob.nr_days_paste_due_current <= 5
group by 1
)
, base_final as (
select 
*
, coalesce(coalesce(sum_utilizado_interno, 0.0)/(coalesce(sum_utilizado_interno, 0.0) + coalesce(sum_utilizado_mercado, 0.0)),0) as share_of_wallet
from base_principal
)
select * from base_final
--select COUNT(*) from base_final


----------------base salva ---------------------

select *
from customer_sandbox_zone.segmentacao_mgm
limit 5


-------------------- CRUZAMENTOS ---------------------------



with base_com_persona as (
select
m.cpf
, max(flag_mgm) as flag_mgm
, max(c.ds_cluster_sociodemographic) as ds_cluster_sociodemographic
from customer_sandbox_zone.segmentacao_mgm m
left join customer_curated_zone.ca_book_cliente cli on cli.cpf = m.cpf 
left join customer_curated_zone.ca_book_cliente_cluster c on (c.id_customer  = cli.id_customer)
group by 1
)
select 
ds_cluster_sociodemographic
, avg(flag_mgm) as percentual_persona
from base_com_persona
group by 1
--maximo 1%


select 
tempo_relacionamento_fx, avg(flag_mgm) as perc_mgm
from customer_sandbox_zone.segmentacao_mgm
group by 1
--maximo 3%


select 
ganho_limite_fx, avg(flag_mgm) as perc_mgm
from customer_sandbox_zone.segmentacao_mgm
group by 1
--maximo 2,9%

select 
nr_trans_mov_conta_fx, avg(flag_mgm) as perc_mgm
from customer_sandbox_zone.segmentacao_mgm
group by 1
--maximo 2,6%


select 
share_of_wallet_fx, avg(flag_mgm) as perc_mgm
from customer_sandbox_zone.segmentacao_mgm
group by 1
--maximo 2%


select * from customer_sandbox_zone.mgm_pesquisa 
limit 5


select 
*
from customer_sandbox_zone.mgm_pesquisa p
left join growth_curated_zone.clientes c on (c.email = p.e_mail)
left join customer_sandbox_zone.segmentacao_mgm m on (m.cpf = c.cpf)


select * from customer_analytics_zone.ca_base_calculo_ie
limit 5


select 
base
, segmento_final
, score
, count(distinct c.cpf) as clientes
from customer_sandbox_zone.mgm_pesquisa p
left join growth_curated_zone.clientes c on (c.email = p.e_mail)
left join customer_sandbox_zone.segmentacao_mgm m on (m.cpf = c.cpf)
group by 1,2,3






