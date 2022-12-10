/*
Generic Test for PSO column 
The following function is to test length of PSO column to ensure the length is correct
*/

{% test length(model, column_name, val) %}

    select *
    from {{ model }}
    where LENGTH({{ column_name }}) = {{ val }}

{% endtest %}