CREATE TABLE public.mcc
(
    mcc_code integer NOT NULL,
    mcc_desc character varying(500),
    PRIMARY KEY (mcc_code)
);

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
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.transactions OWNER to postgres;
ALTER TABLE IF EXISTS public.mcc OWNER to postgres;