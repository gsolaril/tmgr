--██████████████████████████████████████████████████████████████████████████████████
-- DROP TABLE public.tc_accounts;
CREATE TABLE public.tc_accounts (
	alias varchar(255) NOT NULL,
	id int8 NOT NULL,
	"password" varchar(255) NOT NULL,
	"server" varchar(255) NULL,
	ip varchar(32) NULL,
	mtver int4 NOT NULL,
	category int4 NOT NULL,
	active bool NOT NULL,
	"token" varchar(36) NULL,
	max_lots_sig float8 NULL,
	max_lots_net float8 NULL,
	max_marg_usd float8 NULL,
	max_marg_prc float8 NULL,
	max_loss_usd float8 NULL,
	max_loss_prc float8 NULL,
	sym_suffix varchar(32) NULL,
	"owner" varchar(255) NULL,
	max_exposure float8 NULL,
	max_pricerange int8 NULL,
	max_npos int8 NULL,
	max_late_tol int8 NULL,
	invert_trades bool NULL,
	enforce_sl int8 NULL,
	max_timerange int8 NULL,
	CONSTRAINT tc_account_pk PRIMARY KEY (alias)
);
--██████████████████████████████████████████████████████████████████████████████████
-- DROP TABLE public.tc_brokers;
CREATE TABLE public.tc_brokers (
	"server" varchar(255) NOT NULL,
	ip varchar(64) NOT NULL,
	alias varchar(4) NULL,
	broker varchar(255) NULL,
	utc int8 NULL,
	dst bool NULL,
	mtver int8 NULL,
	CONSTRAINT tc_brokers_pk PRIMARY KEY (server)
);
--██████████████████████████████████████████████████████████████████████████████████
-- DROP TABLE public.tc_config;
CREATE TABLE public.tc_config (
	"instance" varchar NOT NULL,
	freq_query_accounts int8 DEFAULT 60 NOT NULL,
	freq_query_config int8 DEFAULT 10 NOT NULL,
	is_enabled_ws_recv bool DEFAULT true NOT NULL,
	is_enabled_ws_exec bool DEFAULT true NOT NULL,
	is_enabled_zmq_recv bool DEFAULT true NOT NULL,
	is_enabled_zmq_exec bool DEFAULT true NOT NULL,
	freq_ws_ping int8 DEFAULT 30 NOT NULL,
	freq_update_responses int8 DEFAULT 30 NOT NULL,
	freq_update_state_acc int8 DEFAULT 60 NOT NULL,
	freq_query_specs int8 DEFAULT 86400 NOT NULL,
	freq_update_summary int8 DEFAULT 30 NOT NULL,
	freq_update_order_map int8 DEFAULT 10 NOT NULL,
	freq_order_intervention int8 DEFAULT 10 NOT NULL,
	freq_rep_send_update int4 DEFAULT 1800 NOT NULL,
	freq_rep_send_reports int4 DEFAULT 1800 NOT NULL,
	freq_rep_read_requests int4 DEFAULT 60 NOT NULL,
	ignore_symbols text NOT NULL,
	CONSTRAINT tc_config_pk PRIMARY KEY (instance)
);
--██████████████████████████████████████████████████████████████████████████████████
-- DROP TABLE public.tc_deposits;
CREATE TABLE public.tc_deposits (
	"index" int8 NOT NULL,
	alias varchar(255) NOT NULL,
	ticket int8 NOT NULL,
	value float8 NOT NULL,
	"comment" varchar(255) NULL
);
--██████████████████████████████████████████████████████████████████████████████████
-- DROP TABLE public.tc_gui_auth;
CREATE TABLE public.tc_gui_auth (
	username varchar(255) NOT NULL,
	"password" varchar(255) NOT NULL,
	last_login int8 NULL,
	last_change int8 NULL,
	access_level int4 NULL,
	CONSTRAINT tc_gui_auth_unique UNIQUE (username)
);
--██████████████████████████████████████████████████████████████████████████████████
-- DROP TABLE public.tc_gui_changes;
CREATE TABLE public.tc_gui_changes (
	"timestamp" int8 NOT NULL,
	username varchar(255) NOT NULL,
	"parameter" varchar(255) NULL,
	value_before varchar(255) NULL,
	value_after varchar(255) NULL,
	CONSTRAINT tc_gui_changes_unique UNIQUE ("timestamp")
);
--██████████████████████████████████████████████████████████████████████████████████
-- DROP TABLE public.tc_multipliers;
CREATE TABLE public.tc_multipliers (
	"source" text NULL,
	target text NULL,
	factor float8 NULL,
	interv varchar(255) NULL,
	CONSTRAINT tc_multipliers_unique UNIQUE (source, target)
);
CREATE INDEX ix_tc_multipliers_source ON public.tc_multipliers USING btree (source);
CREATE INDEX ix_tc_multipliers_target ON public.tc_multipliers USING btree (target);
--██████████████████████████████████████████████████████████████████████████████████
-- DROP TABLE public.tc_responses;
CREATE TABLE public.tc_responses (
	t_resp int8 NOT NULL,
	t_exec int8 NOT NULL,
	t_recv int8 NOT NULL,
	t_send int8 NOT NULL,
	t_sign int8 NOT NULL,
	success bool NOT NULL,
	"action" varchar(32) NOT NULL,
	alias_source varchar(255) NOT NULL,
	"server" varchar(32) NOT NULL,
	account varchar(32) NOT NULL,
	ticket_source int8 NOT NULL,
	nzmq int4 DEFAULT 1 NULL,
	ticket int8 NULL,
	symbol varchar(32) NULL,
	placed_as varchar(32) NULL,
	order_type varchar(32) NULL,
	"comment" varchar(255) NULL,
	lots float8 NULL,
	price_sign float8 NULL,
	price_open float8 NULL,
	price_close float8 NULL,
	stop_loss float8 NULL,
	take_profit float8 NULL,
	point_value float8 NULL,
	commission float8 NULL,
	swap float8 NULL,
	fee float8 NULL,
	alias varchar(255) NULL
);
--██████████████████████████████████████████████████████████████████████████████████
-- DROP TABLE public.tc_symbols;
CREATE TABLE public.tc_symbols (
	symbol varchar(32) NOT NULL,
	"server" varchar(64) NOT NULL,
	"class" varchar(32) NULL,
	standard varchar(32) NOT NULL,
	"quote" varchar(16) NULL,
	base varchar(16) NOT NULL,
	point float8 NULL,
	digits int8 NULL,
	contract float8 NULL,
	min_stop int8 NULL,
	lot_min float8 NULL,
	lot_max float8 NULL,
	lot_step float8 NULL,
	dev_rate int8 NULL,
	description varchar(255) NULL,
	CONSTRAINT tc_symbols_pk PRIMARY KEY (server, symbol)
);
CREATE INDEX ix_tc_symbols_symbol ON public.tc_symbols USING btree (symbol);
--██████████████████████████████████████████████████████████████████████████████████
-- DROP TABLE public.tc_timeseries;
CREATE TABLE public.tc_timeseries (
	"timestamp" int8 NOT NULL,
	balance float8 NULL,
	equity float8 NULL,
	margin float8 NULL,
	lots_net float8 NULL,
	account int8 NOT NULL,
	"server" varchar(255) NOT NULL,
	alias varchar(255) NOT NULL,
	n_ord_buy int8 NULL,
	n_ord_sell int8 NULL,
	leverage int8 NULL,
	n_cop_buy int8 NULL,
	n_cop_sell int8 NULL,
	lots_buy float8 NULL,
	lots_sell float8 NULL
);
--██████████████████████████████████████████████████████████████████████████████████
-- DROP TABLE public.tc_trades_active;
CREATE TABLE public.tc_trades_active (
	alias text NULL,
	ticket int8 NULL,
	alias_s text NULL,
	ticket_s int8 NULL,
	"quote" text NULL,
	base text NULL,
	symbol text NULL,
	lots_s float8 NULL,
	order_type text NULL,
	t_entry_s int8 NULL,
	t_exit_s float8 NULL,
	p_entry_s float8 NULL,
	p_exit_s float8 NULL,
	exit_s text NULL,
	"comment" text NULL,
	lots float8 NULL,
	t_entry int8 NULL,
	t_exit float8 NULL,
	p_entry float8 NULL,
	p_exit float8 NULL,
	"exit" text NULL
);
CREATE INDEX ix_tc_trades_active_alias ON public.tc_trades_active USING btree (alias);
CREATE INDEX ix_tc_trades_active_ticket ON public.tc_trades_active USING btree (ticket);
--██████████████████████████████████████████████████████████████████████████████████
-- DROP TABLE public.tc_trades_closed;
CREATE TABLE public.tc_trades_closed (
	alias varchar(255) NOT NULL,
	ticket varchar(32) NOT NULL,
	alias_s varchar(255) NULL,
	ticket_s varchar(32) NULL,
	symbol varchar(32) NULL,
	order_type varchar(32) NULL,
	lots float8 NULL,
	lots_s float8 NULL,
	"exit" varchar(32) NULL,
	exit_s varchar(32) NULL,
	t_entry int8 NULL,
	t_entry_s int8 NULL,
	t_exit int8 NULL,
	t_exit_s int8 NULL,
	p_entry float8 NULL,
	p_entry_s float8 NULL,
	p_exit float8 NULL,
	p_exit_s float8 NULL,
	"comment" varchar(255) NULL,
	"quote" varchar(16) NULL,
	base varchar(16) NULL,
	CONSTRAINT tc_copytrades_pk PRIMARY KEY (alias, ticket)
);
--██████████████████████████████████████████████████████████████████████████████████
-- DROP TABLE public.tc_trades_missed;
CREATE TABLE public.tc_trades_missed (
	alias text NULL,
	ticket int8 NULL,
	"quote" text NULL,
	base text NULL,
	symbol text NULL,
	lots float8 NULL,
	order_type text NULL,
	p_entry float8 NULL,
	p_last float8 NULL,
	p_sl float8 NULL,
	p_tp float8 NULL,
	"comment" text NULL,
	seconds_ago int8 NULL
);
CREATE INDEX ix_tc_trades_missed_alias ON public.tc_trades_missed USING btree (alias);
CREATE INDEX ix_tc_trades_missed_ticket ON public.tc_trades_missed USING btree (ticket);
--██████████████████████████████████████████████████████████████████████████████████
