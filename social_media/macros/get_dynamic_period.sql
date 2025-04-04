{% macro get_dynamic_date(payment_month)%}
    DATEADD({{var('period_type')}},-{{var('period_range')}},{{'payment_month'}})
{%endmacro%}