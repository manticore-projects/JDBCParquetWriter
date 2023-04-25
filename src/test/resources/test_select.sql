create table cfe.test as
SELECT  a1.id_instrument
        , product.attribute_value "PRODUCT"
        , segment.attribute_value "SEGMENT"
        , type.attribute_value "TYPE"
        , a.id_currency "CURRENCY"
        , b.nominal_balance_bc "REMAINING_PRINCIPAL"
        , b.amortised_cost_dirty_bc "AMORTISED_COST"
        , b.yield "EIR"
        , h.interest_d_bc - b.accrued_interest_d_bc - h.interest_smoothing_d_bc "INTEREST_INCOME_PL"
        , g.unamortized_fee_bc "UNAMORTIZED_FEE"
        , g.fee_d_bc - g.unamortized_fee_d_bc "AMORTIZED_FEE_PL"
        , f.impairment_stage "IMPAIRMENT_STAGE"
        , imp.impairment_bc "IMPAIRMENT_IAS32"
        , -imp.impairment_d_bc "IMP_IAS32_EXPENSE_PL"
        , ecl.impairment_bc "ECL_IFRS9"
        , -ecl.impairment_d_bc "ECL_IFRS9_EXPENSE_PL"
        , ecl.unwinding_d_bc "INCOME_ADJ_STAGE3"
FROM cfe.execution_v x
    INNER JOIN cfe.instrument_hst a
        ON x.value_date = a.value_date
            AND x.posting_date = a.posting_date
            AND x.flag = 'L'
    INNER JOIN cfe.instrument_ref a1
        ON a.id_instrument_ref = a1.id_instrument_ref
    /* -- BALANCE SHEET ---------------------------------------------------- */
    INNER JOIN cfe.instrument_measure_balance b
        ON x.value_date = b.value_date
            AND x.posting_date = b.posting_date
            AND a.id_instrument_ref = b.id_instrument_ref
            AND b.asset_liability_flag='A'
    /* -- PROVISION ---------------------------------------------------- */
    LEFT JOIN cfe.instrument_measure_impairment ecl
        ON x.value_date = ecl.value_date
            AND x.posting_date = ecl.posting_date
            AND a.id_instrument_ref = ecl.id_instrument_ref
            AND ecl.asset_liability_flag = '9'
    LEFT JOIN cfe.instrument_measure_impairment imp
        ON x.value_date = imp.value_date
            AND x.posting_date = imp.posting_date
            AND a.id_instrument_ref = imp.id_instrument_ref
            AND imp.asset_liability_flag = 'A'
   /* -- PRODUCCT ---------------------------------------------------- */
    LEFT JOIN ( cfe.instrument_attribute_hst2 c
                        INNER JOIN cfe.attribute_ref c1
                            ON c.id_attribute_ref = c1.id_attribute_ref
                                AND c1.id_attribute = 'product' )
        ON x.value_date = c.value_date
            AND x.posting_date = c.posting_date
            AND a.id_instrument_ref = c.id_instrument_ref
    LEFT JOIN cfe.attribute_value_ref product
        ON c.id_attribute_value_ref = product.id_attribute_value_ref
   /* -- SEGMENT ---------------------------------------------------- */
    LEFT JOIN ( cfe.instrument_attribute_hst2 d
                        INNER JOIN cfe.attribute_ref d1
                            ON d.id_attribute_ref = d1.id_attribute_ref
                                AND d1.id_attribute = 'segment' )
        ON x.value_date = d.value_date
            AND x.posting_date = d.posting_date
            AND a.id_instrument_ref = d.id_instrument_ref
    LEFT JOIN cfe.attribute_value_ref segment
        ON d.id_attribute_value_ref = segment.id_attribute_value_ref
   /* -- TYPE ---------------------------------------------------- */
    LEFT JOIN ( cfe.instrument_attribute_hst2 e
                        INNER JOIN cfe.attribute_ref e1
                            ON e.id_attribute_ref = e1.id_attribute_ref
                                AND e1.id_attribute = 'type' )
        ON x.value_date = e.value_date
            AND x.posting_date = e.posting_date
            AND a.id_instrument_ref = e.id_instrument_ref
    LEFT JOIN cfe.attribute_value_ref type
        ON e.id_attribute_value_ref = type.id_attribute_value_ref
   /* -- IMPAIRMENT STAGE------------------------------------------- */
    LEFT JOIN cfe.impairment_hst f
        ON x.value_date = f.valid_date
            AND a.id_instrument_ref = f.id_instrument_ref
   /* -- FEES and CHARGGES------------------------------------------- */
    LEFT JOIN ( SELECT  id_instrument_ref
                        , value_date
                        , posting_date
                        , Sum( unamortized_fee_bc ) unamortized_fee_bc
                        , Sum( fee_d_bc ) fee_d_bc
                        , Sum( unamortized_fee_d_bc ) unamortized_fee_d_bc
                FROM cfe.instrument_measure_fee
                GROUP BY    id_instrument_ref
                            , value_date
                            , posting_date ) g
        ON x.value_date = g.value_date
            AND x.posting_date = g.posting_date
            AND a.id_instrument_ref = g.id_instrument_ref
   /* -- INTEREST INCOME------------------------------------------- */
    INNER JOIN cfe.instrument_measure_income h
            ON x.value_date = h.value_date
                AND x.posting_date = h.posting_date
                AND a.id_instrument_ref = h.id_instrument_ref
                AND b.asset_liability_flag = h.asset_liability_flag
;
