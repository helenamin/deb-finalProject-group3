/*
Generic Test for date columns
The following function is to test the format of the date columns to ensure it has the right format
*/

{% test date_format(model, column_name) %}

    select *
    from {{ model }}
    where {{ column_name }} like '%/%'

{% endtest %}