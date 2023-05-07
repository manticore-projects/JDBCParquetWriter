DROP TABLE IF EXISTS test CASCADE;

CREATE TABLE test (
    id_instrument                CHARACTER VARYING (40)  PRIMARY KEY NOT NULL
    , product                    CHARACTER VARYING (40)  NOT NULL
    , segment                    CHARACTER VARYING (40)  NOT NULL
    , type                       CHARACTER VARYING (40)  NOT NULL
    , currency                   CHARACTER VARYING (3)   NOT NULL
    , remaining_principal        DECIMAL (23,5)          NULL
    , amortised_cost             DECIMAL (23,5)          NULL
    , eir                        DECIMAL (15,12)         NULL
    , interest_income_pl         DECIMAL (23,5)          NULL
    , unamortized_fee            DECIMAL (23,5)          NULL
    , amortized_fee_pl           DECIMAL (23,5)          NULL
    , impairment_stage           DECIMAL (1)             NOT NULL
    , impairment_ias32           DECIMAL (23,5)          NULL
    , imp_ias32_expense_pl       DECIMAL (23,5)          NULL
    , ecl_ifrs9                  DECIMAL (23,5)          NULL
    , ecl_ifrs9_expense_pl       DECIMAL (23,5)          NULL
    , income_adj_stage3          DECIMAL (23,5)          NULL
)
;
