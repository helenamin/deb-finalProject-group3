/*
Generic Test for String Type Detection
New PartsViz API updated on the 16/06/2022 includes a new column named relatedCustomer and that returns a void datatype
The following function is to test data types to ensure it is a string format
*/

{% test string_test(model, column_name) %}
WITH column_types AS (
    SELECT typeof({{ column_name }}) as types
    FROM {{ model }}

)

SELECT COUNT(*) FROM column_types
WHERE types = 'string'
HAVING NOT(COUNT(*) > 0)

{% endtest %}