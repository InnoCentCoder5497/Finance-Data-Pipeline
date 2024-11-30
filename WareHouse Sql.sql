
-- Table: public.mcc

CREATE TABLE public.mcc
(
    mcc_code integer NOT NULL,
    mcc_desc character varying(500),
    PRIMARY KEY (mcc_code)
);

-- Table: public.transactions

CREATE TABLE IF NOT EXISTS public.transactions
(
    trans_id bigint NOT NULL,
    trans_date timestamp without time zone NOT NULL,
    client_id bigint NOT NULL,
    card_id bigint NOT NULL,
    trans_amount double precision NOT NULL,
    payment_method character varying(500) COLLATE pg_catalog."default" NOT NULL,
    trans_type character varying(100) COLLATE pg_catalog."default" NOT NULL,
    merchant_id integer NOT NULL,
    merchant_city character varying(50) COLLATE pg_catalog."default",
    merchant_state character varying(50) COLLATE pg_catalog."default",
    zip integer,
    mcc integer NOT NULL,
    trans_errors character varying(2000) COLLATE pg_catalog."default",
    CONSTRAINT transactions_pkey PRIMARY KEY (trans_id)
);

-- Table: public.users

CREATE TABLE IF NOT EXISTS public.users
(
    client_id integer NOT NULL,
    gender character varying(1) COLLATE pg_catalog."default" NOT NULL,
    birth_year integer NOT NULL,
    birth_month integer,
    retirement_age integer NOT NULL,
    address character varying(500) COLLATE pg_catalog."default",
    latitude double precision,
    longitude double precision,
    per_capita_income double precision NOT NULL,
    yearly_income double precision NOT NULL,
    total_debt double precision NOT NULL,
    credit_score integer NOT NULL,
    num_credit_cards integer NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY (client_id)
);

-- Table: public.users

CREATE TABLE public.cards
(
    card_id integer NOT NULL,
    client_id integer NOT NULL,
    brand character varying(100),
    type character varying(100),
    card_number bigint NOT NULL,
    expires date NOT NULL,
    acct_open_date date NOT NULL,
    cvv integer NOT NULL,
    has_chip character varying(1) NOT NULL,
    num_cards_issued integer NOT NULL,
    credit_limit double precision NOT NULL,
    year_pin_last_changed integer,
    card_on_dark_web character varying(1),
    PRIMARY KEY (card_id)
);

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.transactions OWNER to postgres;
ALTER TABLE IF EXISTS public.mcc OWNER to postgres;
ALTER TABLE IF EXISTS public.users OWNER to postgres;
ALTER TABLE IF EXISTS public.cards OWNER to postgres;